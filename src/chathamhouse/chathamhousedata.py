#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Chatham House Data
------------------

Collects input data for Chatham House.

"""
import urllib.request
from io import BytesIO
from zipfile import ZipFile

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dictandlist import integer_value_convert
from hdx.utilities.location import Location
from os.path import join
from slugify import slugify
from tabulator import Stream


def get_worldbank_iso2_to_iso3(json_url, downloader):
    response = downloader.download(json_url)
    json = response.json()
    iso2iso3 = dict()
    for country in json[1]:
        if country['region']['value'] != 'Aggregates':
            iso2iso3[country['iso2Code']] = country['id'].lower()
    return iso2iso3


def get_camp_non_camp_populations(noncamp_types, camp_types, datasets):
    noncamp_types = noncamp_types.split(',')
    camp_types = camp_types.split(',')
    dataset_unhcr = None
    latest_date = None
    for dataset in datasets:
        if 'displacement' in dataset['title'].lower():
            date = dataset.get_dataset_date_as_datetime()
            if latest_date is None or date > latest_date:
                dataset_unhcr = dataset
                latest_date = date
    if dataset_unhcr is None:
        raise ValueError('No UNHCR dataset found!')
    url = dataset_unhcr.get_resources()[0]['url']
    country_ind = 0  # assume first column contains country
    iso3 = None
    country = None
    row = None
    prev_row = None
    unhcr_non_camp = dict()
    unhcr_camp = dict()
    stream = Stream(url, sheet=16)
    stream.open()
    for row in stream.iter():
        country = row[country_ind]
        iso3 = Location.get_iso3_country_code(country)
        if iso3 is not None:
            break
        prev_row = row
    accommodation_ind = None
    location_ind = None
    population_ind = None
    population = None
    for i, text in enumerate(prev_row):
        header = text.lower()
        value = row[i]
        if 'accommodation' in header:
            accommodation_ind = i
        elif 'location' in header and len(value) > 1:
            location_ind = i
        else:
            try:
                population = int(value)
                population_ind = i
                break
            except ValueError:
                pass
    accommodation_type = row[accommodation_ind].lower()
    for noncamp_type in noncamp_types:
        if noncamp_type in accommodation_type:
            unhcr_non_camp[iso3] = population
            break
    for camp_type in camp_types:
        if camp_type in accommodation_type:
            unhcr_camp[row[location_ind]] = population, iso3
            break
    for row in stream.iter():
        country = row[country_ind]
        iso3 = Location.get_iso3_country_code(country)
        if iso3 is None:
            continue
        accommodation_type = row[accommodation_ind].lower()
        for noncamp_type in noncamp_types:
            if noncamp_type in accommodation_type:
                population = unhcr_non_camp.get(iso3)
                if not population:
                    population = 0
                try:
                    population += int(row[population_ind])
                    unhcr_non_camp[iso3] = population
                except ValueError:
                    continue
                break
        for camp_type in camp_types:
            if camp_type in accommodation_type:
                unhcr_camp[row[location_ind]] = population, iso3
                break
    stream.close()
    return unhcr_non_camp, unhcr_camp


def get_camptypes(url, downloader):
    camptypes = downloader.download_csv_rows_as_dicts(url)
    for key in camptypes:
        camptypes[key] = integer_value_convert(camptypes[key])
    return camptypes


def get_worldbank_series(json_url, downloader, wbiso2iso3):
    response = downloader.download(json_url)
    json = response.json()
    data = dict()
    for countrydata in json[1]:
        iso3 = wbiso2iso3.get(countrydata['country']['id'])
        if iso3 is not None:
            value = countrydata.get('value')
            if value:
                data[iso3] = float(value) / 100.0
    return data


def get_slumratios(url):
    openedurl = urllib.request.urlopen(url)
    with ZipFile(BytesIO(openedurl.read())) as my_zip_file:
        source = my_zip_file.open(my_zip_file.namelist()[0])
        data = 'text://%s' % source.read().decode()
        stream = Stream(data, headers=1, format='csv')
        stream.open()
        years = set()
        for header in stream.headers:
            try:
                int(header)
                years.add(header)
            except ValueError:
                pass
        years = sorted(years, reverse=True)
        slumratios = dict()
        for row in stream.iter(keyed=True):
            if not row:
                break
            iso3 = Location.get_iso3_country_code(row['Country'])
            if iso3 is None:
                continue
            for year in years:
                value = row.get(year)
                if value and value != ' ':
                    slumratios[iso3] = float(value) / 100.0
        stream.close()
        return slumratios


def generate_dataset_and_showcase(pop_types, today):
    title = 'Energy consumption of refugees and displaced people'
    slugified_name = slugify(title.lower())

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('0c6bf79f-504c-4ba5-9fdf-c8cc893c8b2f')
    dataset.set_dataset_date_from_datetime(today)
    dataset.set_expected_update_frequency('Every month')
    dataset.add_other_location('world')

    tags = ['HXL', 'energy', 'refugees', 'idps']
    dataset.add_tags(tags)

    for pop_type in pop_types:
        resource_data = {
            'name': '%s_consumption.csv' % pop_type.lower().replace(' ', '_'),
            'description': '%s %s' % (pop_type, title.lower()),
            'format': 'csv'
        }
        resource = Resource(resource_data)
        dataset.add_update_resource(resource)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Energy services for refugees and displaced people',
        'notes': 'Click the image on the right to go to the energy services model',
        'url': 'http://www.sciencedirect.com/science/article/pii/S2211467X16300396',
        'image_url': 'https://www.chathamhouse.org/sites/files/chathamhouse/styles/large_square_/public/images/Combo_large_LCP%20%282%29.jpg?itok=0HgBOAyu'
    })
    showcase.add_tags(tags)

    return dataset, showcase
