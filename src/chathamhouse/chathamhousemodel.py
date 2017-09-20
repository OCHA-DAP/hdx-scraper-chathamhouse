#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Chatham House Model
-------------------

Run Chatham House model.

"""

import logging

import pandas

from chathamhouse import keyvaluelookup, keep_columns

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

    def calculate_population_pandas(self, unhcr_non_camp, popratios):
        number_hh = pandas.merge(unhcr_non_camp, popratios, on='ISO3 Country Code')
        number_hh['Combined Urban Ratio'] = (1 - number_hh['Urban Ratio']) * self.constants['Population Adjustment Factor'] + number_hh['Urban Ratio']
        number_hh['Urban/Slum Displaced Population'] = number_hh['Population'] * number_hh['Combined Urban Ratio']
        number_hh['Rural Displaced Population'] = number_hh['Population'] - number_hh['Urban/Slum Displaced Population']
        number_hh['Slum Displaced Population'] = number_hh['Urban/Slum Displaced Population'] * number_hh['Slum Ratio']
        number_hh['Urban Displaced Population'] = number_hh['Urban/Slum Displaced Population'] - number_hh['Slum Displaced Population']
        number_hh['Urban'] = self.calculate_number_hh(number_hh['Urban Displaced Population'])
        number_hh['Slum'] = self.calculate_number_hh(number_hh['Slum Displaced Population'])
        number_hh['Rural'] = self.calculate_number_hh(number_hh['Rural Displaced Population'])
        number_hh = keep_columns(number_hh, ['ISO3 Country Code', 'Urban', 'Slum', 'Rural'])
        return number_hh

    @staticmethod
    def calculate_hh_access(number_hh, ratio):
        hh_access = number_hh * ratio
        hh_noaccess = number_hh - hh_access
        return hh_access, hh_noaccess

    @staticmethod
    def calculate_hh_access_pandas(number_hh, ratios):
        hh_access = pandas.merge(number_hh, ratios, on='ISO3 Country Code')
        hh_access['Access'] = hh_access['no'] * hh_access['ratio']
        hh_access['No Access'] = hh_access['no'] - hh_access['Access']
        hh_access = keep_columns(hh_access, ['ISO3 Country Code', 'Access', 'No Access'])
        return hh_access

    @staticmethod
    def get_baseline_target(tier):
        if 'Target' in tier:
            return 'Target'
        else:
            return tier

    @staticmethod
    def get_description(descriptions, baseline_target, comtype):
        return descriptions['%s %s' % (baseline_target, comtype)]

    @staticmethod
    def get_description_pandas(descriptions, column, baseline_target, types):
        typedesccolnames = types.apply(lambda x: '%s %s' % (baseline_target, x))
        return descriptions[column][descriptions['Type'] == typedesccolnames]

    @staticmethod
    def get_expenditure(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.expenditure_divisor * 12.0 * \
               values['Fuel %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_expenditure_pandas(scaled_number_hh, values, column, baseline_target, types):
        typecolnames = types.apply(lambda x: 'Fuel %s Type %s' % (baseline_target, x))
        return scaled_number_hh / ChathamHouseModel.expenditure_divisor * 12.0 * \
               values[column][values['Type'] == typecolnames]

    @staticmethod
    def get_capital(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.capital_divisor * \
               values['Capital %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_capital_pandas(scaled_number_hh, values, column, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.capital_divisor * \
               keyvaluelookup(values, 'Capital %s Type %s' % (baseline_target, comtype), keycolumn='Type', valuecolumn=column)

    @staticmethod
    def get_co2(scaled_number_hh, values, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.co2_divisor * \
               values['CO2 %s Type %s' % (baseline_target, comtype)]

    @staticmethod
    def get_co2_pandas(scaled_number_hh, values, column, baseline_target, comtype):
        return scaled_number_hh / ChathamHouseModel.co2_divisor * \
               keyvaluelookup(values, 'CO2 %s Type %s' % (baseline_target, comtype), keycolumn='Type', valuecolumn=column)

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

    def calculate_ongrid_lighting_pandas(self, hh_grid_access, elecgridtiers, elecappliances, elecco2):
        costs = pandas.merge(hh_grid_access, elecappliances, on='ISO3 Country Code')
        costs = pandas.merge(costs, elecco2, on='ISO3 Country Code')
        costs['kWh per hh per yr'] = costs['Electrical Appliances']
        kWh_per_hh_per_yr = elecgridtiers[self.constants['Lighting Grid Tier']]

        costs.loc[costs['kWh per hh per yr'] == 0.0, 'kWh per hh per yr'] = kWh_per_hh_per_yr
        costs['expenditure dlrs per hh per yr'] = self.constants['Electricity Cost'] * costs['kWh per hh per yr'] / 100.0
        costs['Grid Expenditure ($m/yr)'] = costs['Access'] * costs['expenditure dlrs per hh per yr'] / ChathamHouseModel.expenditure_divisor

        costs['co2 emissions per hh per yr'] = costs['Grid Electricity kg CO2 per kWh'] * costs['kWh per hh per yr']
        costs['Grid CO2 Emissions (t/yr)'] = costs['Access'] * costs['co2 emissions per hh per yr'] / ChathamHouseModel.co2_divisor

        costs = keep_columns(costs, ['ISO3 Country Code', 'Grid Expenditure ($m/yr)', 'Grid CO2 Emissions (t/yr)'])
        return costs

    def calculate_offgrid_lighting(self, baseline_target, hh_offgrid, lightingoffgridtype,
                                   lightingoffgridcost, elecgriddirectenergy, elecco2):
        scaled_number_hh = hh_offgrid * self.constants['Lighting Offgrid Scaling Factor']

        expenditure = self.get_expenditure(scaled_number_hh, lightingoffgridcost, baseline_target,
                                           lightingoffgridtype)
        capital_costs = self.get_capital(scaled_number_hh, lightingoffgridcost, baseline_target,
                                         lightingoffgridtype)
        co2_emissions = self.get_co2(scaled_number_hh, lightingoffgridcost, baseline_target,
                                     lightingoffgridtype)
        grid_co2_emissions = self.get_grid_co2(scaled_number_hh, elecgriddirectenergy, elecco2, baseline_target,
                                               lightingoffgridtype)
        return expenditure, capital_costs, co2_emissions + grid_co2_emissions

    def calculate_offgrid_lighting_pandas(self, baseline_target, hh_offgrid, lightingoffgridtypes,
                                          lightingoffgridcost, elecgriddirectenergy, elecco2):
        costs = pandas.merge(hh_offgrid, elecco2, on='ISO3 Country Code')
        costs['scaled number hh'] = costs['No Access'] * self.constants['Lighting Offgrid Scaling Factor']

        costs['Offgrid Expenditure ($m/yr)'] = self.get_expenditure_pandas(costs['scaled number hh'], lightingoffgridcost, 'Lighting OffGrid', baseline_target,
                                           lightingoffgridtypes)
        costs['Offgrid Capital Costs ($m)'] = self.get_capital_pandas(costs['scaled number hh'], lightingoffgridcost, 'Lighting OffGrid', baseline_target,
                                         lightingoffgridtypes)
        costs['Offgrid CO2 Emissions'] = self.get_co2_pandas(costs['scaled number hh'], lightingoffgridcost, 'Lighting OffGrid', baseline_target,
                                     lightingoffgridtypes)
        costs['Grid CO2 Emissions'] = self.get_grid_co2(costs['scaled number hh'], elecgriddirectenergy, costs['Grid Electricity kg CO2 per kWh'], baseline_target,
                                               lightingoffgridtypes)
        costs['Offgrid CO2 Emissions (t/yr)'] = costs['Offgrid CO2 Emissions'] + costs['Grid CO2 Emissions']

        costs = keep_columns(costs, ['ISO3 Country Code', 'Offgrid Expenditure ($m/yr)', 'Offgrid Capital Costs ($m)', 'Offgrid CO2 Emissions (t/yr)'])
        return costs

    @staticmethod
    def get_noncamp_type(types, pop_type, tier):
        return types['%s %s Type' % (pop_type, tier)]

    @staticmethod
    def get_noncamp_type_pandas(types, column, pop_type, tier):
        return keyvaluelookup(types, '%s %s Type' % (pop_type, tier), valuecolumn=column)

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

    def calculate_non_solid_cooking_pandas(self, hh_nonsolid_access, cookinglpg):
        costs = pandas.merge(hh_nonsolid_access, cookinglpg, on='ISO3 Country Code')
        costs['kg per hh per yr'] = costs['Cooking LPG'] * 12
        costs.loc[costs['kg per hh per yr'] == 0.0, 'kg per hh per yr'] = self.constants['Cooking LPG Fallback'] * 12

        costs['expenditure dlrs per hh per yr'] = self.constants['Cooking LPG NonCamp Price'] * costs['kg per hh per yr']
        costs['Nonsolid Expenditure ($m/yr)'] = costs['Access'] * costs['expenditure dlrs per hh per yr'] / ChathamHouseModel.expenditure_divisor

        costs['co2 emissions per hh per yr'] = self.constants['Kerosene CO2 Emissions'] * costs['kg per hh per yr']
        costs['Nonsolid CO2 Emissions (t/yr)'] = costs['Access'] * costs['co2 emissions per hh per yr'] / ChathamHouseModel.co2_divisor

        costs = keep_columns(costs, ['ISO3 Country Code', 'Nonsolid Expenditure ($m/yr)', 'Nonsolid CO2 Emissions (t/yr)'])
        return costs

    def calculate_solid_cooking(self, baseline_target, hh_no_nonsolid_access, cookingsolidtype, cookingsolidcost):
        scaled_number_hh = hh_no_nonsolid_access * self.constants['Cooking Solid Scaling Factor']

        expenditure = self.get_expenditure(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        capital_costs = self.get_capital(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        # cooking co2 is monthly
        co2_emissions = self.get_co2(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype) * 12.0

        return expenditure, capital_costs, co2_emissions

    def calculate_solid_cooking_pandas(self, baseline_target, hh_no_nonsolid_access, cookingsolidtype, cookingsolidcost):
        costs = pandas.DataFrame(hh_no_nonsolid_access)
        costs['scaled number hh'] = costs['No Access'] * self.constants['Cooking Solid Scaling Factor']

        costs['Solid Expenditure ($m/yr)'] = self.get_expenditure_pandas(costs['scaled number hh'], cookingsolidcost, 'Cooking Solid', baseline_target,
                                                           cookingsolidtype)
        costs['Solid Capital Costs ($m)'] = self.get_capital_pandas(costs['scaled number hh'], cookingsolidcost, 'Cooking Solid', baseline_target,
                                                         cookingsolidtype)
        costs['Solid CO2_Emissions (t/yr)'] = self.get_co2_pandas(costs['scaled number hh'], cookingsolidcost, 'Cooking Solid', baseline_target,
                                                     cookingsolidtype)

        costs = keep_columns(costs, ['ISO3 Country Code', 'Solid Expenditure ($m/yr)', 'Solid Capital Costs ($m)', 'Solid CO2_Emissions (t/yr)'])
        return costs
