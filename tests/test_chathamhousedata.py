#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for Chatham House data.

'''
from pprint import pprint

import pytest
from datetime import datetime

from os.path import join

from chathamhouse.chathamhousedata import get_camp_non_camp_populations, \
    get_worldbank_series, generate_dataset_and_showcase, check_name_dispersed, get_slumratios, get_camptypes, \
    get_camptypes_fallbacks, get_iso3
from chathamhouse.chathamhousemodel import ChathamHouseModel
from tests.expected_results import unhcr_non_camp_expected, unhcr_camp_expected, slum_ratios_expected, \
    country_totals_expected, all_camps_per_country_expected, camptypes_expected, smallcamptypes_expected, \
    camptypes_fallbacks_expected


class TestChathamHouseData:
    @pytest.fixture(scope='class')
    def camptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - CampTypes.csv'), downloader)

    @pytest.fixture(scope='class')
    def smallcamptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - SmallCampTypes.csv'), downloader)

    @pytest.fixture(scope='class')
    def camptypes_fallbacks(self, downloader):
        return get_camptypes_fallbacks(join('tests', 'fixtures', 'Chatham House Constants and Lookups - CampTypeFallbacks.csv'), downloader, keyfn=get_iso3)

    @pytest.fixture(scope='function')
    def wbdownloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/countries/all/indicators/SP.URB.TOTL.IN.ZS?MRV=1&format=json&per_page=10000':
                    def fn():
                        return [None, [{'value': '58.0939285510597', 'date': '2016', 'indicator': {'value': 'Urban population (% of total)', 'id': 'SP.URB.TOTL.IN.ZS'}, 'country': {'value': 'Arab World', 'id': '1A'}, 'decimal': '0'},
                                       {'value': '66.032', 'date': '2016', 'indicator': {'value': 'Urban population (% of total)', 'id': 'SP.URB.TOTL.IN.ZS'}, 'country': {'value': 'Austria', 'id': 'AT'}, 'decimal': '0'},
                                       {'value': '32.277', 'date': '2016', 'indicator': {'value': 'Urban population (% of total)', 'id': 'SP.URB.TOTL.IN.ZS'}, 'country': {'value': 'Zimbabwe', 'id': 'ZW'}, 'decimal': '0'}]]
                    response.json = fn
                elif url == 'http://haha/countries?format=json&per_page=10000':
                    def fn():
                        return [{'per_page': '10000', 'pages': 1, 'page': 1, 'total': 304},
                                [{'name': 'Aruba', 'region': {'value': 'Latin America & Caribbean ', 'id': 'LCN'},
                                  'capitalCity': 'Oranjestad', 'id': 'ABW',
                                  'lendingType': {'value': 'Not classified', 'id': 'LNX'},
                                  'adminregion': {'value': '', 'id': ''}, 'iso2Code': 'AW', 'longitude': '-70.0167',
                                  'incomeLevel': {'value': 'High income', 'id': 'HIC'}, 'latitude': '12.5167'},
                                 {'name': 'Afghanistan', 'region': {'value': 'South Asia', 'id': 'SAS'},
                                  'capitalCity': 'Kabul', 'id': 'AFG', 'lendingType': {'value': 'IDA', 'id': 'IDX'},
                                  'adminregion': {'value': 'South Asia', 'id': 'SAS'}, 'iso2Code': 'AF',
                                  'longitude': '69.1761', 'incomeLevel': {'value': 'Low income', 'id': 'LIC'},
                                  'latitude': '34.5228'},
                                 {'name': 'Africa', 'region': {'value': 'Aggregates', 'id': 'NA'},
                                  'capitalCity': '', 'id': 'AFR',
                                  'lendingType': {'value': 'Aggregates', 'id': ''},
                                  'adminregion': {'value': '', 'id': ''}, 'iso2Code': 'A9', 'longitude': '',
                                  'incomeLevel': {'value': 'Aggregates', 'id': 'NA'}, 'latitude': ''},
                                 {'name': 'Angola', 'region': {'value': 'Sub-Saharan Africa ', 'id': 'SSF'},
                                  'capitalCity': 'Luanda', 'id': 'AGO', 'lendingType': {'value': 'IBRD', 'id': 'IBD'},
                                  'adminregion': {'value': 'Sub-Saharan Africa (excluding high income)', 'id': 'SSA'},
                                  'iso2Code': 'AO', 'longitude': '13.242',
                                  'incomeLevel': {'value': 'Upper middle income', 'id': 'UMC'},
                                  'latitude': '-8.81155'}]]
                    response.json = fn
                return response
        return Download()

    def test_get_camp_non_camp_populations(self, datasets, downloader):
        all_camps_per_country, unhcr_non_camp, unhcr_camp, unhcr_camp_excluded = \
            get_camp_non_camp_populations('individual,undefined', 'self-settled,planned,collective,reception',
                                          {'Accommodation Type': {'Corum': 'Planned/managed camp',
                                           'MyCamp': 'Planned/managed camp'},
                                           'Country': {'MyCamp': 'ISL'},
                                           'Population': {'MyCamp': 1000}},
                                          datasets, downloader)
        country_totals = dict()
        for iso3 in all_camps_per_country:
            country_totals[iso3] = ChathamHouseModel.sum_population(all_camps_per_country, iso3)
        assert country_totals == country_totals_expected
        assert all_camps_per_country == all_camps_per_country_expected
        assert unhcr_non_camp == unhcr_non_camp_expected
        assert unhcr_camp == unhcr_camp_expected

    def test_get_worldbank_series(self, wbdownloader):
        result = get_worldbank_series('http://lala/countries/all/indicators/SP.URB.TOTL.IN.ZS?MRV=1&format=json&per_page=10000',
                                      wbdownloader)
        assert result == {'AUT': 0.66032, 'ZWE': 0.32277}

    def test_get_slumratios(self, slumratios):
        assert slumratios == slum_ratios_expected

    def test_get_camptypes(self, camptypes, smallcamptypes):
        assert camptypes == camptypes_expected
        assert smallcamptypes == smallcamptypes_expected

    def test_get_camptypes_fallbacks(self, camptypes_fallbacks):
        assert camptypes_fallbacks == camptypes_fallbacks_expected

    def test_check_name_dispersed(self):
        assert check_name_dispersed('Burundi : Dispersed in the country / territory') is True
        assert check_name_dispersed('Afghanistan') is False

    def test_generate_dataset_and_showcase(self, configuration):
        dataset, showcase = generate_dataset_and_showcase(['Urban', 'Small camps'], datetime(2017, 9, 15, 0, 0))
        assert dataset == {'title': 'Energy consumption of refugees and displaced people',
                           'data_update_frequency': '30', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'owner_org': '0c6bf79f-504c-4ba5-9fdf-c8cc893c8b2f', 'dataset_date': '09/15/2017',
                           'name': 'energy-consumption-of-refugees-and-displaced-people',
                           'groups': [{'name': 'world'}], 'tags': [{'name': 'HXL'}, {'name': 'energy'},
                                                                   {'name': 'refugees'}, {'name': 'idps'}]}
        assert dataset.get_resources() == [{'name': 'urban_consumption.csv', 'format': 'csv',
                                            'description': 'Urban energy consumption of refugees and displaced people'},
                                           {'name': 'small_camps_consumption.csv', 'format': 'csv',
                                            'description': 'Small camps energy consumption of refugees and displaced people'},
                                           {'name': 'population.csv', 'format': 'csv',
                                            'description': 'UNHCR displaced population totals'}]
        assert showcase == {'title': 'Energy services for refugees and displaced people',
                            'notes': 'Click the image on the right to go to the energy services model',
                            'image_url': 'https://www.chathamhouse.org/sites/files/chathamhouse/styles/large_square_/public/images/Combo_large_LCP%20%282%29.jpg?itok=0HgBOAyu',
                            'name': 'energy-consumption-of-refugees-and-displaced-people-showcase',
                            'tags': [{'name': 'HXL'}, {'name': 'energy'}, {'name': 'refugees'}, {'name': 'idps'}],
                            'url': 'http://www.sciencedirect.com/science/article/pii/S2211467X16300396'}