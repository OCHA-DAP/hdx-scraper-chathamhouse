#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from slugify import slugify

logger = logging.getLogger(__name__)


class ChathamHouseModel:
    levels = ['Baseline', 'Target 1', 'Target 2', 'Target 3']
    expenditure_divisor = 1000000.0
    capital_divisor = 1000000.0
    co2_divisor = 1000.0

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
        number_hh['Urban'] = urban_minus_slum_displaced_population / hh_size
        number_hh['Slum'] = slum_displaced_population / hh_size
        number_hh['Rural'] = rural_displaced_population / hh_size
        return number_hh

    @staticmethod
    def calculate_hh_access(number_hh, ratio):
        hh_access = number_hh * ratio
        hh_noaccess = number_hh - hh_access
        return hh_access, hh_noaccess

    @staticmethod
    def get_expenditure(scaled_number_hh, values, pop_type, level):
        return scaled_number_hh / ChathamHouseModel.expenditure_divisor * 12.0 * values['%s %s Total fuel cost' % (pop_type, level)]

    @staticmethod
    def get_capital(scaled_number_hh, values, pop_type, level):
        return scaled_number_hh / ChathamHouseModel.capital_divisor * values['%s %s Total capital cost' % (pop_type, level)]

    @staticmethod
    def get_co2(scaled_number_hh, multiplier, values, pop_type, level):
        return scaled_number_hh / ChathamHouseModel.co2_divisor * multiplier * values['%s %s Total CO2' % (pop_type, level)]

    def get_kWh_per_hh_per_yr(self, iso3, electiers, elecappliances):
        kWh_per_hh_per_yr = elecappliances[iso3]
        if kWh_per_hh_per_yr == 0.0:
            tier = self.constants['Lighting Grid Tier']
            kWh_per_hh_per_yr = electiers[tier]
        return kWh_per_hh_per_yr

    def calculate_ongrid_lighting(self, iso3, hh_grid_access, electiers, elecappliances, elecco2):
        kWh_per_hh_per_yr = self.get_kWh_per_hh_per_yr(iso3, electiers, elecappliances)

        expenditure_dlrs_per_hh_per_yr = self.constants['Electricity Cost'] * kWh_per_hh_per_yr / 100.0
        expenditure = hh_grid_access * expenditure_dlrs_per_hh_per_yr / ChathamHouseModel.expenditure_divisor

        co2_emissions_per_hh_per_yr = elecco2[iso3] * kWh_per_hh_per_yr
        co2_emissions = hh_grid_access * co2_emissions_per_hh_per_yr / ChathamHouseModel.co2_divisor

        return expenditure, co2_emissions

    def calculate_offgrid_lighting(self, pop_type, level, iso3, hh_offgrid, lighting, elecco2):
        scaled_number_hh = hh_offgrid * self.constants['Lighting Offgrid Scaling Factor']

        expenditure = self.get_expenditure(scaled_number_hh, lighting, pop_type, level)
        capital_costs = self.get_capital(scaled_number_hh, lighting, pop_type, level)
        co2_emissions = self.get_co2(scaled_number_hh, elecco2[iso3], lighting, pop_type, level)

        return expenditure, capital_costs, co2_emissions

    def get_kg_per_hh_per_yr(self, iso3, cookinglpg):
        kg_per_hh_per_mth = cookinglpg[iso3]
        if kg_per_hh_per_mth == 0.0:
            kg_per_hh_per_mth = self.constants['Cooking LPG Fallback']
        return kg_per_hh_per_mth * 12.0

    def calculate_non_solid_cooking(self, iso3, hh_nonsolid_access, cookinglpg):
        kg_per_hh_per_yr = self.get_kg_per_hh_per_yr(iso3, cookinglpg)

        expenditure_dlrs_per_hh_per_yr = self.constants['Cooking LPG NonCamp Price'] * kg_per_hh_per_yr
        expenditure = hh_nonsolid_access * expenditure_dlrs_per_hh_per_yr / ChathamHouseModel.expenditure_divisor

        co2_emissions_per_hh_per_yr = self.constants['Kerosene CO2 Emissions'] * kg_per_hh_per_yr
        co2_emissions = hh_nonsolid_access * co2_emissions_per_hh_per_yr / ChathamHouseModel.co2_divisor
        return expenditure, co2_emissions

    def calculate_solid_cooking(self, pop_type, level, iso3, hh_no_nonsolid_access, cooking):
        scaled_number_hh = hh_no_nonsolid_access * self.constants['Cooking Solid Scaling Factor']

        expenditure = self.get_expenditure(scaled_number_hh, cooking, pop_type, level)
        capital_costs = self.get_capital(scaled_number_hh, cooking, pop_type, level)
        co2_emissions = self.get_co2(scaled_number_hh, 1.0, cooking, pop_type, level)

        return expenditure, capital_costs, co2_emissions

