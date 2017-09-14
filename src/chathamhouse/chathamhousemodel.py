#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

logger = logging.getLogger(__name__)


class ChathamHouseModel:
    tiers = ['Baseline', 'Target 1', 'Target 2', 'Target 3']
    expenditure_divisor = 1000000.0
    capital_divisor = 1000000.0
    co2_divisor = 1000.0

    def __init__(self, constants):
        self.constants = constants

    def calculate_number_hh(self, pop):
        hh_size = self.constants['Household Size']
        return pop / hh_size

    def calculate_population(self, iso3, unhcr_non_camp, urbanratios, slumratios):
        urbanratio = urbanratios.get(iso3)
        if not urbanratio:
            logger.info('Missing urban ratio data for %s!' % iso3)
            return None
        combined_urbanratio = (1 - urbanratio) * self.constants['Population Adjustment Factor'] + urbanratio
        displaced_population = unhcr_non_camp[iso3]
        urban_displaced_population = displaced_population * combined_urbanratio
        rural_displaced_population = displaced_population - urban_displaced_population
        slumratio = slumratios.get(iso3)
        if not slumratio:
            logger.info('Missing slum ratio data for %s!' % iso3)
            return None
        slum_displaced_population = urban_displaced_population * slumratio
        urban_minus_slum_displaced_population = urban_displaced_population - slum_displaced_population
        number_hh = dict()
        number_hh['Urban'] = self.calculate_number_hh(urban_minus_slum_displaced_population)
        number_hh['Slum'] = self.calculate_number_hh(slum_displaced_population)
        number_hh['Rural'] = self.calculate_number_hh(rural_displaced_population)
        return number_hh

    @staticmethod
    def calculate_hh_access(number_hh, ratio):
        hh_access = number_hh * ratio
        hh_noaccess = number_hh - hh_access
        return hh_access, hh_noaccess

    @staticmethod
    def get_baseline_target(tier):
        if 'Target' in tier:
            return 'Target'
        else:
            return tier

    @staticmethod
    def get_expenditure(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.expenditure_divisor * 12.0 * \
               values['Fuel %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_capital(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.capital_divisor * \
               values['Capital %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_co2(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.co2_divisor * \
               values['CO2 %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_grid_co2(scaled_number_hh, elecgriddirectenergy, elecco2, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.co2_divisor * elecco2 * \
               elecgriddirectenergy['%s Type %s' % (baseline_target, comtype)]

    def get_kWh_per_hh_per_yr(self, elecgridtiers, elecappliances):
        kWh_per_hh_per_yr = elecappliances
        if kWh_per_hh_per_yr == 0.0:
            tier = self.constants['Lighting Grid Tier']
            kWh_per_hh_per_yr = elecgridtiers[tier]
        return kWh_per_hh_per_yr

    def calculate_ongrid_lighting(self, hh_grid_access, elecgridtiers, elecappliances, elecco2):
        kWh_per_hh_per_yr = self.get_kWh_per_hh_per_yr(elecgridtiers, elecappliances)

        expenditure_dlrs_per_hh_per_yr = self.constants['Electricity Cost'] * kWh_per_hh_per_yr / 100.0
        expenditure = hh_grid_access * expenditure_dlrs_per_hh_per_yr / ChathamHouseModel.expenditure_divisor

        co2_emissions_per_hh_per_yr = elecco2 * kWh_per_hh_per_yr
        co2_emissions = hh_grid_access * co2_emissions_per_hh_per_yr / ChathamHouseModel.co2_divisor

        return expenditure, co2_emissions

    def calculate_offgrid_lighting(self, tier, hh_offgrid, lightingoffgridtype,
                                   lightingoffgridcost, elecgriddirectenergy, elecco2):
        scaled_number_hh = hh_offgrid * self.constants['Lighting Offgrid Scaling Factor']

        baseline_target = self.get_baseline_target(tier)
        expenditure = self.get_expenditure(scaled_number_hh, lightingoffgridcost, baseline_target,
                                           lightingoffgridtype)
        capital_costs = self.get_capital(scaled_number_hh, lightingoffgridcost, baseline_target,
                                         lightingoffgridtype)
        co2_emissions = self.get_co2(scaled_number_hh, lightingoffgridcost, baseline_target,
                                     lightingoffgridtype)
        grid_co2_emissions = self.get_grid_co2(scaled_number_hh, elecgriddirectenergy, elecco2, baseline_target,
                                     lightingoffgridtype)
        return expenditure, capital_costs, co2_emissions + grid_co2_emissions

    def calculate_noncamp_offgrid_lighting(self, pop_type, tier, hh_offgrid, noncamplightingoffgridtypes,
                                           lightingoffgridcost, elecgriddirectenergy, elecco2):
        noncamplightingoffgridtype = noncamplightingoffgridtypes['%s %s Type' % (pop_type, tier)]
        return self.calculate_offgrid_lighting(tier, hh_offgrid, noncamplightingoffgridtype, lightingoffgridcost,
                                               elecgriddirectenergy, elecco2)

    def calculate_camp_offgrid_lighting(self, tier, hh_offgrid, camptypes, lightingoffgridcost, elecgriddirectenergy,
                                        elecco2):
        camplightingoffgridtype = camptypes['Lighting OffGrid %s' % tier]
        if not camplightingoffgridtype:
            return '', '', ''
        return self.calculate_offgrid_lighting(tier, hh_offgrid, camplightingoffgridtype, lightingoffgridcost,
                                               elecgriddirectenergy, elecco2)

    def get_kg_per_hh_per_yr(self, cookinglpg):
        kg_per_hh_per_mth = cookinglpg
        if kg_per_hh_per_mth == 0.0:
            kg_per_hh_per_mth = self.constants['Cooking LPG Fallback']
        return kg_per_hh_per_mth * 12.0

    def calculate_non_solid_cooking(self, hh_nonsolid_access, cookinglpg):
        kg_per_hh_per_yr = self.get_kg_per_hh_per_yr(cookinglpg)

        expenditure_dlrs_per_hh_per_yr = self.constants['Cooking LPG NonCamp Price'] * kg_per_hh_per_yr
        expenditure = hh_nonsolid_access * expenditure_dlrs_per_hh_per_yr / ChathamHouseModel.expenditure_divisor

        co2_emissions_per_hh_per_yr = self.constants['Kerosene CO2 Emissions'] * kg_per_hh_per_yr
        co2_emissions = hh_nonsolid_access * co2_emissions_per_hh_per_yr / ChathamHouseModel.co2_divisor
        return expenditure, co2_emissions

    def calculate_solid_cooking(self, tier, hh_no_nonsolid_access, cookingsolidtype, cookingsolidcost):
        scaled_number_hh = hh_no_nonsolid_access * self.constants['Cooking Solid Scaling Factor']

        baseline_target = self.get_baseline_target(tier)
        expenditure = self.get_expenditure(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        capital_costs = self.get_capital(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        # cooking co2 is monthly
        co2_emissions = self.get_co2(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype) * 12.0

        return expenditure, capital_costs, co2_emissions

    def calculate_noncamp_solid_cooking(self, pop_type, tier, hh_no_nonsolid_access, noncampcookingsolidtypes,
                                cookingsolidcost):
        noncampcookingsolidtype = noncampcookingsolidtypes['%s %s Type' % (pop_type, tier)]
        return self.calculate_solid_cooking(tier, hh_no_nonsolid_access, noncampcookingsolidtype, cookingsolidcost)

    def calculate_camp_solid_cooking(self, tier, hh_no_nonsolid_access, camptypes,
                                     cookingsolidcost):
        campcookingsolidtype = camptypes['Cooking Solid %s' % tier]
        if not campcookingsolidtype:
            return '', '', ''
        return self.calculate_solid_cooking(tier, hh_no_nonsolid_access, campcookingsolidtype, cookingsolidcost)
