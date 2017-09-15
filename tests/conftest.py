# -*- coding: UTF-8 -*-
"""Global fixtures"""
import pytest
from os.path import join

from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations


@pytest.fixture(scope='session')
def configuration():
    Configuration._create(hdx_read_only=True)
    Locations.set_validlocations([{'name': 'world', 'title': 'World'}])


@pytest.fixture(scope='session')
def lightingoffgridcost():
    return {
        'Fuel Baseline Type 1': 2.823413482,
        'Fuel Baseline Type 2': 4.44240625,
        'Fuel Baseline Type 3': 1.2633125,
        'Fuel Baseline Type 4': 1.6,
        'Fuel Baseline Type 5': 1.6,
        'Fuel Baseline Type 6': 1.6,
        'Fuel Baseline Type 7': '-',
        'Fuel Baseline Type 8': '-',
        'Fuel Target Type 1': 0.7170674096,
        'Fuel Target Type 2': 0.848125,
        'Fuel Target Type 3': 0.1369252525,
        'Fuel Target Type 4': 0.219080404,
        'Fuel Target Type 5': 4.032205144,
        'Fuel Target Type 6': 4.769166667,
        'Fuel Target Type 7': 0.7699564924,
        'Fuel Target Type 8': '-',
        'Capital Baseline Type 1': 1.490591562,
        'Capital Baseline Type 2': 22.6,
        'Capital Baseline Type 3': 315,
        'Capital Baseline Type 4': 33.15,
        'Capital Baseline Type 5': 307.125,
        'Capital Baseline Type 6': 33.15,
        'Capital Baseline Type 7': '-',
        'Capital Baseline Type 8': '-',
        'Capital Target Type 1': 44.00295781,
        'Capital Target Type 2': 350,
        'Capital Target Type 3': 177.37069,
        'Capital Target Type 4': 232.7931039,
        'Capital Target Type 5': 258.7943672,
        'Capital Target Type 6': 350,
        'Capital Target Type 7': 509.3103495,
        'Capital Target Type 8': '-',
        'CO2 Baseline Type 1': 9.866481818,
        'CO2 Baseline Type 2': 85.248,
        'CO2 Baseline Type 3': 7.104,
        'CO2 Baseline Type 4': 28.416,
        'CO2 Baseline Type 5': 28.416,
        'CO2 Baseline Type 6': 28.416,
        'CO2 Baseline Type 7': '-',
        'CO2 Baseline Type 8': '-',
        'CO2 Target Type 1': 20.91640909,
        'CO2 Target Type 2': 0,
        'CO2 Target Type 3': 6.274922727,
        'CO2 Target Type 4': 10.03987636,
        'CO2 Target Type 5': 117.6169091,
        'CO2 Target Type 6': 0,
        'CO2 Target Type 7': 35.28507273,
        'CO2 Target Type 8': '-'
    }


@pytest.fixture(scope='session')
def elecgriddirectenergy():
    return {
        'Baseline Type 1': 0,
        'Baseline Type 2': 1.725,
        'Baseline Type 3': 31.05,
        'Baseline Type 4': 0,
        'Baseline Type 5': 0,
        'Baseline Type 6': 0,
        'Target Type 1': 0,
        'Target Type 2': 34.5,
        'Target Type 3': 0,
        'Target Type 4': 0,
        'Target Type 5': 0,
        'Target Type 6': 194,
        'Target Type 7': 0
    }


@pytest.fixture(scope='session')
def cookingsolidcost():
    return {
        'Fuel Baseline Type 1': 8.058489806,
        'Fuel Baseline Type 2': 6.965355642,
        'Fuel Baseline Type 3': 19.16866051,
        'Fuel Baseline Type 4': 12.73516901,
        'Fuel Baseline Type 5': 13.84626055,
        'Fuel Baseline Type 6': 16.47473772,
        'Fuel Baseline Type 7': 15.30722003,
        'Fuel Baseline Type 8': 22.07752339,
        'Fuel Target Type 1': 3.253183005,
        'Fuel Target Type 2': 3.639391027,
        'Fuel Target Type 3': 14.33815018,
        'Fuel Target Type 4': 11.03458033,
        'Fuel Target Type 5': 6.055844752,
        'Fuel Target Type 6': 15.44765271,
        'Fuel Target Type 7': 6.738935308,
        'Fuel Target Type 8': 22.07649325,
        'Capital Baseline Type 1': 1.487070426,
        'Capital Baseline Type 2': 5.771732102,
        'Capital Baseline Type 3': 18.95809848,
        'Capital Baseline Type 4': 35.87836059,
        'Capital Baseline Type 5': 2.341114833,
        'Capital Baseline Type 6': 49.11622127,
        'Capital Baseline Type 7': 2.097807965,
        'Capital Baseline Type 8': 79.98973237,
        'Capital Target Type 1': 50.21875515,
        'Capital Target Type 2': 50.47249826,
        'Capital Target Type 3': 55.20139862,
        'Capital Target Type 4': 62.11943715,
        'Capital Target Type 5': 50,
        'Capital Target Type 6': 66.76853641,
        'Capital Target Type 7': 50,
        'Capital Target Type 8': 79.99603303,
        'CO2 Baseline Type 1': 221.1589807,
        'CO2 Baseline Type 2': 238.9478858,
        'CO2 Baseline Type 3': 124.9325673,
        'CO2 Baseline Type 4': 162.5503121,
        'CO2 Baseline Type 5': 235.9852608,
        'CO2 Baseline Type 6': 122.6936955,
        'CO2 Baseline Type 7': 233.2858074,
        'CO2 Baseline Type 8': 35.63830632,
        'CO2 Target Type 1': 86.07462795,
        'CO2 Target Type 2': 132.177205,
        'CO2 Target Type 3': 17.20304107,
        'CO2 Target Type 4': 15.30655537,
        'CO2 Target Type 5': 11.07817662,
        'CO2 Target Type 6': 17.03746798,
        'CO2 Target Type 7': 94.70995309,
        'CO2 Target Type 8': 19.58004714
    }


@pytest.fixture(scope='session')
def datasets(configuration):
    ds = list()
    dataset = Dataset({'title': 'UNHCR Refugee Population Statistics', 'dataset_date': '12/31/2013'})
    ds.append(dataset)
    dataset = Dataset({'title': 'UNHCR Global Trends: Forced Displacement in 2016 Data', 'dataset_date': '06/20/2017'})
    dataset.add_update_resource({'url': join('tests', 'fixtures', 'UNHCR-14-wrd-tab-v3-external.xls')})
    ds.append(dataset)
    dataset = Dataset({'title': 'Global Forced Displacement Trends in 2014', 'dataset_date': '06/19/2015'})
    ds.append(dataset)
    dataset = Dataset({'title': 'UNHCR Population of Concern from Colombia', 'dataset_date': '01/01/1975-12/01/2012'})
    ds.append(dataset)
    return ds 