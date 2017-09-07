#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging
from io import BytesIO
from zipfile import ZipFile
import urllib.request

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.utilities.location import Location
from slugify import slugify
from tabulator import Stream

logger = logging.getLogger(__name__)


def get_worldbank_iso2_to_iso3(json_url, downloader):
    response = downloader.download(json_url)
    json = response.json()
    iso2iso3 = dict()
    for country in json[1]:
        if country['region']['value'] != 'Aggregates':
            iso2iso3[country['iso2Code']] = country['id'].lower()
    return iso2iso3


def get_population():
    datasets = Dataset.search_in_hdx('displacement', fq='organization:unhcr')
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
    countries = dict()
    unhcr_non_camp = dict()
    stream = Stream(url, sheet=16)
    stream.open()
    for row in stream.iter():
        country = row[country_ind]
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 is not None and match is True:
            break
        prev_row = row
    countries[iso3] = country
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
    if 'individual' or 'undefined' in row[accommodation_ind].lower():
        unhcr_non_camp[iso3] = population
    for row in stream.iter():
        country = row[country_ind]
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 is None:
            continue
        countries[iso3] = country
        if 'individual' or 'undefined' in row[accommodation_ind].lower():
            population = unhcr_non_camp.get(iso3)
            if not population:
                population = 0
            try:
                population += int(row[population_ind])
                unhcr_non_camp[iso3] = population
            except ValueError:
                continue
    stream.close()
    return unhcr_non_camp


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
            iso3, match = Location.get_iso3_country_code(row['Country'])
            if iso3 is None:
                continue
            for year in years:
                value = row.get(year)
                if value and value != ' ':
                    slumratios[iso3] = float(value) / 100.0
        stream.close()
        return slumratios


def country_iso_key_convert(dictin):
    dictout = dict()
    for country in dictin:
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 and match:
            dictout[iso3] = dictin[country]
        else:
            logger.info('No match for %s' % country)
    return dictout


def integer_key_convert(dictin):
    dictout = dict()
    for key in dictin:
        dictout[int(key)] = dictin[key]
    return dictout


def float_value_convert(dictin):
    dictout = dict()
    for key in dictin:
        dictout[key] = float(dictin[key])
    return dictout


def avg_dicts(dictin1, dictin2):
    dictout = dict()
    for key in dictin1:
        dictout[key] = (dictin1[key] + dictin2[key]) / 2.0
    return dictout


class ChathamHouseModel:
    levels = ['Baseline', 'Target 1', 'Target 2', 'Target 3']

    def __init__(self, constants):
        self.constants = constants

    def calculate_population(self, iso3, unhcr_non_camp, urbanratios, slumratios):
        urbanratio = urbanratios.get(iso3)
        if not urbanratio:
            return None, None
        combined_urbanratio = (1 - urbanratio) * self.constants['Population Adjustment Factor'] + urbanratio
        displaced_population = unhcr_non_camp[iso3]
        urban_displaced_population = displaced_population * combined_urbanratio
        rural_displaced_population = displaced_population - urban_displaced_population
        slumratio = slumratios.get(iso3)
        if not slumratio:
            return None, None
        slum_displaced_population = urban_displaced_population * slumratio
        urban_minus_slum_displaced_population = urban_displaced_population - slum_displaced_population
        hh_size = self.constants['Household Size']
        number_hh = dict()
        number_hh['urban'] = urban_minus_slum_displaced_population / hh_size
        number_hh['slum'] = slum_displaced_population / hh_size
        number_hh['rural'] = rural_displaced_population / hh_size
        return number_hh

    @staticmethod
    def calculate_hh_grid_access(number_hh, ratio):
        hh_grid_access = number_hh * ratio
        hh_offgrid = number_hh - hh_grid_access
        return hh_grid_access, hh_offgrid

    def get_kWh_per_yr_per_hh(self, iso3, electiers, elecappliances):
        kWh_per_yr_per_hh = elecappliances[iso3]
        if kWh_per_yr_per_hh == 0.0:
            tier = self.constants['Lighting Grid Tier']
            kWh_per_yr_per_hh = electiers[tier]
        return kWh_per_yr_per_hh

    def calculate_ongrid_lighting(self, iso3, hh_grid_access, electiers, elecappliances, elecco2):
        kWh_per_yr_per_hh = self.get_kWh_per_yr_per_hh(iso3, electiers, elecappliances)
        expenditure_on_grid_dlrs_per_year_per_hh = self.constants['Electricity Cost'] * kWh_per_yr_per_hh / 100.0
        grid_expenditure = hh_grid_access * expenditure_on_grid_dlrs_per_year_per_hh / 1000000
        co2_emissions_per_hh = kWh_per_yr_per_hh * elecco2[iso3]
        grid_co2_emissions = hh_grid_access * co2_emissions_per_hh / 1000
        return grid_expenditure, grid_co2_emissions

    def calculate_offgrid_lighting(self, level, iso3, hh_offgrid, lighting, elecco2):
        offgrid_multiplier = hh_offgrid * self.constants['Lighting Offgrid Scaling Factor']
        offgrid_expenditure_multiplier = offgrid_multiplier / 1000000 * 12
        offgrid_capital_multiplier = offgrid_multiplier / 1000000
        offgrid_co2_multiplier = offgrid_multiplier / 1000 * elecco2[iso3]

        offgrid_expenditure = offgrid_expenditure_multiplier * lighting['%s Total fuel cost' % level]
        offgrid_capital_costs = offgrid_capital_multiplier * lighting['%s Total capital cost' % level]
        offgrid_co2_emissions = offgrid_co2_multiplier * (lighting['%s Direct CO2' % level] +
                                                          lighting['%s Grid Primary energy' % level])
        return offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions

def generate_dataset_and_showcase(downloader, countrydata):
    """Parse json of the form:
    {
      "Location": "Zimbabwe",
      "Dataset Title": "WorldPop Zimbabwe Population dataset",
      "Source": "WorldPop, University of Southampton, UK",
      "Description": "These datasets provide estimates of population counts for each 100 x 100m grid cell in the country for various years. Please refer to the metadata file and WorldPop website (www.worldpop.org) for full information.",
      "Dataset contains sub-national data": "true",
      "License": "Other",
      "Define License": "http://www.worldpop.org.uk/data/licence.txt",
      "Organisation": "WorldPop, University of Southampton, UK; www.worldpop.org",
      "Visibility": "Public",
      "id_no": "243",
      "URL_direct": "http://www.worldpop.org.uk/data/hdx/?dataset=ZWE-POP",
      "URL_summaryPage": "http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population",
      "URL_datasetDetailsPage": "http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP.txt",
      "URL_image": "http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP_500.JPG",
      "productionDate": "2013-01-01T00:00:00+00:00",
      "datasetDate": "2015",
      "lastModifiedDate": "2016-10-17T12:54:54+01:00",
      "fileFormat": "zipped geotiff",
      "location": "ZWE",
      "updateFrequency": "Annual",
      "maintainerName": "WorldPop",
      "maintainerEmail": "worldpop@geodata.soton.ac.uk",
      "authorName": "WorldPop",
      "authorEmail": "worldpop@geodata.soton.ac.uk",
      "tags": [
        "Population Statistics",
        "WorldPop",
        "University of Southampton"
      ]
    },
    """
    title = countrydata['Dataset Title'].replace('dataset', '').strip()
    logger.info('Creating dataset: %s' % title)
    licence_id = countrydata['License'].lower()
    licence = None
    if licence_id == 'other':
        licence_id = 'hdx-other'
        licence_url = countrydata['Define License']
        response = downloader.download(licence_url)
        licence = response.text
    slugified_name = slugify(title).lower()
    url_summary = countrydata['URL_summaryPage']
    description = 'Go to [WorldPop Dataset Summary Page](%s) for more information' % url_summary
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        'notes': countrydata['Description'],
        'methodology': 'Other',
        'methodology_other': description,
        'dataset_source': countrydata['Source'],
        'subnational': countrydata['Dataset contains sub-national data'] is True,
        'license_id': licence_id,
        'private': countrydata['Visibility'] != 'Public',
        'url': url_summary
    })
    dataset.set_maintainer('37023db4-a571-4f28-8d1f-15f0353586af')
    dataset.set_organization('3f077dff-1d05-484d-a7c2-4cb620f22689')
    dataset.set_dataset_date(countrydata['datasetDate'])
    dataset.set_expected_update_frequency(countrydata['updateFrequency'])
    dataset.add_country_location(countrydata['iso3'])
    tags = countrydata['tags']
    dataset.add_tags(tags)
    if licence:
        dataset.update({'license_other': licence})

    resource = {
        'name': title,
        'format': countrydata['fileFormat'],
        'url': countrydata['URL_direct'],
        'description': description
    }
    dataset.add_update_resource(resource)

    location = countrydata['Location']
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'WorldPop %s Summary Page' % location,
        'notes': 'Click the image on the right to go to the WorldPop summary page for the %s dataset' % location,
        'url': url_summary,
        'image_url': countrydata['URL_image']
    })
    showcase.add_tags(tags)
    return dataset, showcase
