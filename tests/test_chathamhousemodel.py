#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for Chatham House Model.

'''
from os.path import join

import pytest

from chathamhouse.chathamhousedata import get_camptypes
from chathamhouse.chathamhousemodel import ChathamHouseModel


class TestChathamHouseModel:
    @pytest.fixture(scope='class')
    def camptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - CampTypes.csv'),
                             downloader)

    @pytest.fixture(scope='class')
    def small_camptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - SmallCampTypes.csv'),
                              downloader)

    @pytest.fixture(scope='class')
    def lighting_type_descriptions(self):
        return {'Target 2': 'Grid', 'Target 1': 'Solar/diesel', 'Baseline 3': 'Electricity-dependent', 'Baseline 2': 'Kerosene-dependent', 'Baseline 6': '', 'Target 5': 'Solar/mini-grid', 'Target 6': '', 'Baseline 7': '-', 'Baseline 1': 'Torch-dependent', 'Baseline 5': '', 'Baseline 4': 'Solar-dependent', 'Target 7': '', 'Target 4': 'Grid', 'Target 3': 'Solar/mini-grid', 'Target 8': '-', 'Baseline 8': '-'}

    @pytest.fixture(scope='class')
    def cooking_type_descriptions(self):
        return {'Target 2': 'Firewood mix', 'Target 1': 'Firewood-dependent', 'Baseline 3': 'Kerosene dependent', 'Baseline 2': 'Firewood mix', 'Baseline 6': '', 'Target 5': 'Alternative biomass', 'Target 6': 'Biomass briquettes', 'Baseline 7': '', 'Baseline 1': 'Firewood-dependent', 'Baseline 5': 'Alternative biomass', 'Baseline 4': 'LPG fuelled', 'Target 7': 'LPG II', 'Target 4': 'LPG fuelled', 'Target 3': 'Kerosene dependent', 'Target 8': '', 'Baseline 8': ''}

    @staticmethod
    def calculate_noncamp_offgrid_lighting(model, pop_type, tier, hh_offgrid, noncamplightingoffgridtypes,
                                           lightingoffgridcost, elecgriddirectenergy, elecco2):
        noncamplightingoffgridtype = noncamplightingoffgridtypes['%s %s Type' % (pop_type, tier)]
        baseline_target = model.get_baseline_target(tier)
        return model.calculate_offgrid_lighting(baseline_target, hh_offgrid, noncamplightingoffgridtype,
                                                lightingoffgridcost, elecgriddirectenergy, elecco2)

    @staticmethod
    def calculate_camp_offgrid_lighting(model, tier, hh_offgrid, camptypes, lightingoffgridcost, elecgriddirectenergy,
                                        elecco2):
        camplightingoffgridtype = camptypes['Lighting OffGrid %s' % tier]
        if not camplightingoffgridtype:
            return '', '', ''
        baseline_target = model.get_baseline_target(tier)
        return model.calculate_offgrid_lighting(baseline_target, hh_offgrid, camplightingoffgridtype,
                                                lightingoffgridcost, elecgriddirectenergy, elecco2)
    @staticmethod
    def calculate_noncamp_solid_cooking(model, pop_type, tier, hh_no_nonsolid_access, noncampcookingsolidtypes,
                                cookingsolidcost):
        noncampcookingsolidtype = noncampcookingsolidtypes['%s %s Type' % (pop_type, tier)]
        baseline_target = model.get_baseline_target(tier)
        return model.calculate_solid_cooking(baseline_target, hh_no_nonsolid_access, noncampcookingsolidtype,
                                             cookingsolidcost)

    @staticmethod
    def calculate_camp_solid_cooking(model, tier, hh_no_nonsolid_access, camptypes,
                                     cookingsolidcost):
        campcookingsolidtype = camptypes['Cooking Solid %s' % tier]
        if not campcookingsolidtype:
            return '', '', ''
        baseline_target = model.get_baseline_target(tier)
        return model.calculate_solid_cooking(baseline_target, hh_no_nonsolid_access, campcookingsolidtype,
                                             cookingsolidcost)

    def testNonCampModel(self, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost, slumratios,
                         lighting_type_descriptions, cooking_type_descriptions):
        model = ChathamHouseModel({'Population Adjustment Factor': 0.7216833622,
                                   'Household Size': 5,
                                   'Electricity Cost': 25,
                                   'Cooking LPG NonCamp Price': 1.8,
                                   'Kerosene CO2 Emissions': 2.96,
                                   'Lighting Offgrid Scaling Factor': 1,
                                   'Cooking Solid Scaling Factor': 1})
        iso3 = 'ago'
        unhcr_non_camp = {iso3: 59970}
        urbanratios = {iso3: 0.58379}
        elecappliances = {iso3: 92.6033836492}
        noncampelecgridco2 = {iso3: 0.0375}
        cookinglpg = {iso3: 4.096473669}

        country_elecappliances = elecappliances.get(iso3)
        country_noncampelecgridco2 = noncampelecgridco2[iso3]
        country_cookinglpg = cookinglpg[iso3]

        displaced_population = unhcr_non_camp[iso3]
        number_hh_by_pop_type = model.calculate_population(iso3, displaced_population, urbanratios, slumratios, list())
        assert number_hh_by_pop_type == {'Rural': 1389.3629848179437, 'Slum': 7601.16815388216,
                                         'Urban': 3003.4688612998957}
        number_hh_by_pop_type = model.calculate_population(iso3, displaced_population, urbanratios, {iso3: 0.658}, list())
        assert number_hh_by_pop_type == {'Rural': 1389.3629848179437, 'Slum': 6977.851155989793,
                                         'Urban': 3626.785859192263}
        pop_type = 'Rural'
        number_hh = number_hh_by_pop_type[pop_type]
        country_elec_access = 0.055
        hh_grid_access, hh_offgrid = model.calculate_hh_access(number_hh, country_elec_access)
        assert hh_grid_access == 76.4149641649869
        assert hh_offgrid == 1312.9480206529568
        country_nonsolid_access = 0.11
        hh_nonsolid_access, hh_no_nonsolid_access = model.calculate_hh_access(number_hh, country_nonsolid_access)
        assert hh_nonsolid_access == 152.8299283299738
        assert hh_no_nonsolid_access == 1236.5330564879698
        elecgrid = {0: 3, 1: 35, 2: 194, 3: 820, 4: 1720}
        grid_expenditure, grid_co2_emissions = model.calculate_ongrid_lighting(hh_grid_access, elecgrid,
                                                                               country_elecappliances,
                                                                               country_noncampelecgridco2)
        assert grid_expenditure == 0.0017690710607775378
        assert grid_co2_emissions == 0.26536065911663065
        nonsolid_expenditure, nonsolid_co2_emissions = model.calculate_non_solid_cooking(hh_nonsolid_access,
                                                                                        country_cookinglpg)
        assert nonsolid_expenditure == 0.013522977588360128
        assert nonsolid_co2_emissions == 22.237785367525543
        noncamplightingoffgridtypes = {'Urban Baseline Type': 1, 'Urban Target 1 Type': 1, 'Urban Target 2 Type': 3,
                                       'Urban Target 3 Type': 7, 'Rural Baseline Type': 1, 'Rural Target 1 Type': 1,
                                       'Rural Target 2 Type': 3, 'Rural Target 3 Type': 7, 'Slum Baseline Type': 1,
                                       'Slum Target 1 Type': 1, 'Slum Target 2 Type': 3, 'Slum Target 3 Type': 7}
        noncampcookingsolidtypes = {'Urban Baseline Type': 2, 'Urban Target 1 Type': 2, 'Urban Target 2 Type': 7,
                                    'Urban Target 3 Type': 8, 'Rural Baseline Type': 1, 'Rural Target 1 Type': 1,
                                    'Rural Target 2 Type': 7, 'Rural Target 3 Type': 8, 'Slum Baseline Type': 1,
                                    'Slum Target 1 Type': 1, 'Slum Target 2 Type': 7, 'Slum Target 3 Type': 8}
        tier = 'Target 2'
        noncamplightingoffgridtype = noncamplightingoffgridtypes['%s %s Type' % (pop_type, tier)]
        noncampcookingsolidtype = noncampcookingsolidtypes['%s %s Type' % (pop_type, tier)]
        res = model.calculate_offgrid_solid(tier, hh_offgrid, lighting_type_descriptions,
                                            noncamplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, country_noncampelecgridco2,
                                            hh_no_nonsolid_access, cooking_type_descriptions, noncampcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 0.002157308870967376
        assert offgrid_capital_costs == 0.2328784963573492
        assert offgrid_co2_emissions == 8.238647374164904
        co2_emissions = grid_co2_emissions + offgrid_co2_emissions
        assert co2_emissions == 8.504008033281535
        assert solid_expenditure == 0.09999499528651126
        assert solid_capital_costs == 0.06182665282439849
        assert solid_co2_emissions == 1405.3438532905193
        co2_emissions = nonsolid_co2_emissions + solid_co2_emissions
        assert co2_emissions == 1427.5816386580448

        tier = 'Target 3'
        noncamplightingoffgridtype = noncamplightingoffgridtypes['%s %s Type' % (pop_type, tier)]
        noncampcookingsolidtype = noncampcookingsolidtypes['%s %s Type' % (pop_type, tier)]
        res = model.calculate_offgrid_solid(tier, hh_offgrid, lighting_type_descriptions,
                                            noncamplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, country_noncampelecgridco2,
                                            hh_no_nonsolid_access, cooking_type_descriptions, noncampcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 0.01213095423222568
        assert offgrid_capital_costs == 0.6686980152740906
        assert offgrid_co2_emissions == 46.32746639944912
        co2_emissions = grid_co2_emissions + offgrid_co2_emissions
        assert co2_emissions == 46.59282705856575
        assert solid_expenditure == 0.3275797640995024
        assert solid_capital_costs == 0.0989177392294985
        assert solid_co2_emissions == 290.5365064344328
        co2_emissions = nonsolid_co2_emissions + solid_co2_emissions
        assert co2_emissions == 312.7742918019583

    def testCampModel(self, camptypes, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost,
                      lighting_type_descriptions, cooking_type_descriptions):
        model = ChathamHouseModel({'Household Size': 5,
                                   'Electricity Cost': 25,
                                   'Cooking LPG NonCamp Price': 1.8,
                                   'Kerosene CO2 Emissions': 2.96,
                                   'Lighting Offgrid Scaling Factor': 1,
                                   'Cooking Solid Scaling Factor': 1})
        noncampelecgridco2 = {'sdn': 0.615}
        camp = 'Southern Darfur : Wilayat - State'
        unhcr_camp = {'Southern Darfur : Wilayat - State': (916885, 'sdn')}
        population, iso3 = unhcr_camp[camp]
        number_hh = model.calculate_number_hh(population)
        camp_camptypes = camptypes.get(camp)
        elecco2 = noncampelecgridco2[iso3]

        tier = 'Baseline'
        camplightingoffgridtype = camp_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = camp_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 9.775621570875002
        assert offgrid_capital_costs == 4.1443202
        assert offgrid_co2_emissions == 15827.062570875001
        assert solid_expenditure == 17.732900221858348
        assert solid_capital_costs == 0.272694513508602
        assert solid_co2_emissions == 486665.6448458868

        tier = 'Target 1'
        camplightingoffgridtype = camp_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = camp_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 1.5779240444426306
        assert offgrid_capital_costs == 8.06913039432437
        assert offgrid_co2_emissions == 3835.58834969693
        assert solid_expenditure == 7.15870727889462
        assert solid_capital_costs == 9.20896466314155
        assert solid_co2_emissions == 189409.2845950458

    def testSmallCampModel(self, small_camptypes, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost,
                           lighting_type_descriptions, cooking_type_descriptions):
        model = ChathamHouseModel({'Household Size': 5,
                                   'Electricity Cost': 25,
                                   'Cooking LPG NonCamp Price': 1.8,
                                   'Kerosene CO2 Emissions': 2.96,
                                   'Lighting Offgrid Scaling Factor': 1,
                                   'Cooking Solid Scaling Factor': 1})
        camp_group = 'Subsaharan Africa G'
        smallcamps = {camp_group: 83908.56}
        small_camps_elecgridco2 = {camp_group: 0.3994098361}
        number_hh = model.calculate_number_hh(smallcamps[camp_group])
        campgroup_camptypes = small_camptypes.get(camp_group)
        elecco2 = small_camps_elecgridco2[camp_group]

        tier = 'Baseline'
        camplightingoffgridtype = campgroup_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = campgroup_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 0.894614187294
        assert offgrid_capital_costs == 0.37926669120000006
        assert offgrid_co2_emissions == 1442.1696815239604
        assert solid_expenditure == ''
        assert solid_capital_costs == ''
        assert solid_co2_emissions == ''

        tier = 'Target 3'
        camplightingoffgridtype = campgroup_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = campgroup_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == 0.15505425729584385
        assert offgrid_capital_costs == 8.547099603928343
        assert offgrid_co2_emissions == 592.1439284539138
        assert solid_expenditure == ''
        assert solid_capital_costs == ''
        assert solid_co2_emissions == ''

        camp_group = 'Asia E'
        smallcamps = {camp_group: 19577.01373}
        small_camps_elecgridco2 = {camp_group: ''}
        number_hh = model.calculate_number_hh(smallcamps[camp_group])
        campgroup_camptypes = small_camptypes.get(camp_group)
        elecco2 = small_camps_elecgridco2[camp_group]

        tier = 'Target 2'
        camplightingoffgridtype = campgroup_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = campgroup_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == ''
        assert offgrid_capital_costs == ''
        assert offgrid_co2_emissions == ''
        assert solid_expenditure == 0.28453285405596584
        assert solid_capital_costs == 0.19577013729999998
        assert solid_co2_emissions == 520.506277903452

        tier = 'Baseline'
        camplightingoffgridtype = campgroup_camptypes['Lighting OffGrid %s' % tier]
        campcookingsolidtype = campgroup_camptypes['Cooking Solid %s' % tier]
        res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                            camplightingoffgridtype, lightingoffgridcost,
                                            elecgriddirectenergy, elecco2,
                                            number_hh, cooking_type_descriptions, campcookingsolidtype,
                                            cookingsolidcost)
        lighting_type_description, offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions, \
        cooking_type_description, solid_expenditure, solid_capital_costs, solid_co2_emissions = res
        assert offgrid_expenditure == ''
        assert offgrid_capital_costs == ''
        assert offgrid_co2_emissions == ''
        assert solid_expenditure == 0.6505642389516176
        assert solid_capital_costs == 0.00916640744582953
        assert solid_co2_emissions == 11087.728057822153

    def test_calculate_regional_average(self):
        avg = ChathamHouseModel.calculate_regional_average('things', {'COM': 0.5, 'ETH': 0.1, 'AGO': 0.9}, 'DJI')
        assert avg == (0.3, '014')
        avg = ChathamHouseModel.calculate_regional_average('things', {'AGO': 0.3, 'LSO': 0.7, 'DZA': 0.7}, 'DJI')
        assert avg == (0.5, '202')
        avg = ChathamHouseModel.calculate_regional_average('things', {'AGO': 0.3, 'COM': 0.5, 'AIA': 0.9}, 'LBY')
        assert avg == (0.4, '002')

    def test_sum_population(self):
        remdict = {'AFG': {'individual': {'a': 10, 'b': 20}}, 'BUR': {'self-settled': {'c': 12, 'd': 21}}}
        pop = ChathamHouseModel.sum_population({'AFG': {'individual': {'a': 10, 'b': 20}}}, 'AFG', remdict)
        assert pop == 30
        assert remdict == {'AFG': {'individual': {}}, 'BUR': {'self-settled': {'c': 12, 'd': 21}}}
