#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Chatham House Model
-------------------

Run Chatham House model.

"""

import logging

from hdx.location.country import Country
from hxl import Column

logger = logging.getLogger(__name__)


class ChathamHouseModel:
    tiers = ['Baseline', 'Target 1', 'Target 2', 'Target 3']
    region_levels = {1: 'main', 2: 'sub', 3: 'intermediate'}
    expenditure_divisor = 1000000.0
    capital_divisor = 1000000.0
    co2_divisor = 1000.0

    def __init__(self, constants):
        self.constants = constants
        self.total_biomass = 0
        self.total_nonbiomass = 0
        self.total_grid = 0
        self.total_offgrid = 0
        self.total_spending = 0
        self.camp_biomass = 0
        self.camp_nonbiomass = 0
        self.camp_grid = 0
        self.camp_offgrid = 0
        self.reset_pop_counters()

    def calculate_number_hh(self, pop):
        hh_size = self.constants['Household Size']
        return pop / hh_size

    @staticmethod
    def round(val):
        return int(val + 0.5)

    @staticmethod
    def calculate_average(datadict, keys=None):
        sum = 0.0
        if keys is None:
            keys = datadict.keys()
        no_keys = 0
        for key in keys:
            if key in datadict:
                sum += datadict[key]
                no_keys += 1
        if no_keys == 0:
            return None
        return float(sum) / no_keys

    @staticmethod
    def calculate_mostfrequent(datadict):
        type_buckets = dict()
        for valtype in datadict.values():
            no = type_buckets.get(valtype)
            if no is None:
                no = 0
            no += 1
            type_buckets[valtype] = no
        highestno = 0
        highest_type = None
        for valtype in type_buckets:
            no = type_buckets[valtype]
            if no > highestno:
                highestno = no
                highest_type = valtype
        return highest_type

    @staticmethod
    def sum_population(totals_dict, iso3, remove_dict=None):
        population = 0
        accom_types = totals_dict[iso3]
        for accom_type in sorted(accom_types):
            camps = accom_types[accom_type]
            for name in sorted(camps):
                population += camps[name]
                if remove_dict:
                    del remove_dict[iso3][accom_type][name]
        return population

    @classmethod
    def calculate_regional_average(cls, val_type, datadict, iso3):
        countryinfo = Country.get_country_info_from_iso3(iso3)
        level = 3
        while level != 0:
            region_level = cls.region_levels[level]
            region_prefix = region_level
            column = Column.parse('#region+code+%s' % region_prefix)
            regioncode = countryinfo[column.get_display_tag(sort_attributes=True)]
            if regioncode:
                regioncode = int(regioncode)
                column = Column.parse('#region+%s+name+preferred' % region_prefix)
                regionname = countryinfo[column.get_display_tag(sort_attributes=True)]
                countries_in_region = Country.get_countries_in_region(regioncode)
                avg = cls.calculate_average(datadict, countries_in_region)
                if avg:
                    logger.warning('%s: %s - Using %s (%s) average' % (iso3, val_type, regionname, region_level))
                    return avg, regioncode
            level -= 1
        logger.warning('%s: %s - Using global average' % (iso3, val_type))
        return cls.calculate_average(datadict), '001'

    def calculate_population_from_hh(self, hh):
        hh_size = self.constants['Household Size']
        return self.round(hh * hh_size)

    def calculate_population(self, iso3, displaced_population, urbanratios, slumratios, info):
        urbanratio = urbanratios.get(iso3)
        if not urbanratio:
            urbanratio, region = self.calculate_regional_average('Urban ratio', urbanratios, iso3)
            info.append('ur(%s)=%.3g' % (region, urbanratio))
        combined_urbanratio = (1 - urbanratio) * self.constants['Population Adjustment Factor'] + urbanratio
        urban_displaced_population = displaced_population * combined_urbanratio
        rural_displaced_population = displaced_population - urban_displaced_population
        slumratio = slumratios.get(iso3)
        if not slumratio:
            slumratio, region = self.calculate_regional_average('Slum ratio', slumratios, iso3)
            info.append('ur(%s)=%.3g' % (region, slumratio))
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
    def get_description(descriptions, baseline_target, comtype):
        return descriptions['%s %s' % (baseline_target, comtype)]

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

    @staticmethod
    def get_noncamp_type(types, pop_type, tier):
        return types['%s %s Type' % (pop_type, tier)]

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

    def calculate_solid_cooking(self, baseline_target, hh_no_nonsolid_access, cookingsolidtype, cookingsolidcost):
        scaled_number_hh = hh_no_nonsolid_access * self.constants['Cooking Solid Scaling Factor']

        expenditure = self.get_expenditure(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        capital_costs = self.get_capital(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype)
        # cooking co2 is monthly
        co2_emissions = self.get_co2(scaled_number_hh, cookingsolidcost, baseline_target, cookingsolidtype) * 12.0

        return expenditure, capital_costs, co2_emissions

    def calculate_offgrid_solid(self, tier, hh_offgrid, lighting_type_descriptions, lightingoffgridtype,
                                lightingoffgridcost, elecgriddirectenergy, country_elecgridco2,
                                hh_no_nonsolid_access, cooking_type_descriptions, cookingsolidtype, cookingsolidcost):
        baseline_target = self.get_baseline_target(tier)
        lightingtypedesc = ''
        oe, oc, oco2 = '', '', ''
        if lightingoffgridtype:
            lightingtypedesc = self.get_description(lighting_type_descriptions, baseline_target,
                                                    lightingoffgridtype)
            oe, oc, oco2 = self.calculate_offgrid_lighting(baseline_target, hh_offgrid, lightingoffgridtype,
                                                           lightingoffgridcost, elecgriddirectenergy,
                                                           country_elecgridco2)
        cookingtypedesc = ''
        se, sc, sco2 = '', '', ''
        if cookingsolidtype:
            cookingtypedesc = self.get_description(cooking_type_descriptions, baseline_target,
                                                   cookingsolidtype)
            se, sc, sco2 = self.calculate_solid_cooking(baseline_target, hh_no_nonsolid_access, cookingsolidtype,
                                                        cookingsolidcost)
        return lightingtypedesc, oe, oc, oco2, cookingtypedesc, se, sc, sco2

    def reset_pop_counters(self):
        self.pop_biomass = 0
        self.pop_nonbiomass = 0
        self.pop_grid = 0
        self.pop_offgrid = 0

    def add_keyfigures(self, iso3, country, camp, tier, se, oe, cookingtypedesc, cooking_pop, lightingtypedesc, lighting_pop, results, ne=0, ge=0):
        if tier == self.tiers[0]:
            cooking_expenditure = ne
            lighting_expenditure = ge
            if se:
                cooking_expenditure += se
            if oe:
                lighting_expenditure += oe
            self.total_spending += cooking_expenditure + lighting_expenditure
            if cookingtypedesc:
                if 'firewood' in cookingtypedesc.lower():
                    self.pop_biomass += cooking_pop
                else:
                    self.pop_nonbiomass += cooking_pop
            if lightingtypedesc:
                if 'grid' in lightingtypedesc.lower():
                    self.pop_grid += lighting_pop
                else:
                    self.pop_offgrid += lighting_pop
            if camp not in ['Urban', 'Slum', 'Rural']:
                self.camp_biomass += self.pop_biomass
                self.camp_nonbiomass += self.pop_nonbiomass
                self.camp_grid += self.pop_grid
                self.camp_offgrid += self.pop_offgrid
            self.total_biomass += self.pop_biomass
            self.total_nonbiomass += self.pop_nonbiomass
            self.total_grid += self.pop_grid
            self.total_offgrid += self.pop_offgrid
            row = [iso3, country, camp, tier,
                   cooking_expenditure, cookingtypedesc, self.pop_nonbiomass, self.pop_biomass,
                   lighting_expenditure, lightingtypedesc, self.pop_grid, self.pop_offgrid]
            results[len(results) - 2].append(row)

    def get_percentage_biomass(self):
        return self.total_biomass / (self.total_nonbiomass + self.total_biomass)

    def get_camp_percentage_biomass(self):
        return self.camp_biomass / (self.camp_nonbiomass + self.camp_biomass)

    def get_percentage_offgrid(self):
        return self.total_offgrid / (self.total_grid + self.total_offgrid)

    def get_camp_percentage_offgrid(self):
        return self.camp_offgrid / (self.camp_grid + self.camp_offgrid)

    def get_total_spending(self):
        return round(self.total_spending) * 1000000