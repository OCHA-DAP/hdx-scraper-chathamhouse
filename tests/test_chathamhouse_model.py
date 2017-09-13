#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for Chatham House Model.

'''
import pytest
from os.path import join

from hdx.utilities.downloader import Download

from chathamhouse_data import get_camptypes
from chathamhouse_model import ChathamHouseModel


class TestChathamHouseModel:
    @pytest.fixture(scope='class')
    def downloader(self):
        return Download()

    @pytest.fixture(scope='class')
    def camptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - CampTypes.csv'),
                             downloader)

    @pytest.fixture(scope='class')
    def small_camptypes(self, downloader):
        return get_camptypes(join('tests', 'fixtures', 'Chatham House Constants and Lookups - SmallCampTypes.csv'),
                              downloader)

    def testNonCampModel(self, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost):
        model = ChathamHouseModel({'Population Adjustment Factor': 0.7216833622,
                                   'Household Size': 5,
                                   'Electricity Cost': 25,
                                   'Cooking LPG NonCamp Price': 1.8,
                                   'Kerosene CO2 Emissions': 2.96,
                                   'Lighting Offgrid Scaling Factor': 1,
                                   'Cooking Solid Scaling Factor': 1})
        iso3 = 'ang'
        unhcr_non_camp = {iso3: 59970}
        urbanratios = {iso3: 0.58379}
        slumratios = {iso3: 0.658}
        elecappliances = {iso3: 92.6033836492}
        noncampelecgridco2 = {iso3: 0.0375}
        cookinglpg = {iso3: 4.096473669}

        country_elecappliances = elecappliances.get(iso3)
        country_noncampelecgridco2 = noncampelecgridco2[iso3]
        country_cookinglpg = cookinglpg[iso3]
        number_hh_by_pop_type = model.calculate_population('ang', unhcr_non_camp, urbanratios, slumratios)
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
        nonsolid_expenditure, nonsolid_co2_emissions= model.calculate_non_solid_cooking(hh_nonsolid_access,
                                                                                        country_cookinglpg)
        assert nonsolid_expenditure == 0.013522977588360128
        assert nonsolid_co2_emissions == 22.237785367525543
        noncamplightingoffgridtypes = {'Urban Baseline Type': 1, 'Urban Target 1 Type': 1, 'Urban Target 2 Type': 3,
                                       'Urban Target 3 Type': 7, 'Rural Baseline Type': 1, 'Rural Target 1 Type': 1,
                                       'Rural Target 2 Type': 3, 'Rural Target 3 Type': 7, 'Slum Baseline Type': 1,
                                       'Slum Target 1 Type': 1, 'Slum Target 2 Type': 3, 'Slum Target 3 Type': 7}
        offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions = \
            model.calculate_noncamp_offgrid_lighting(pop_type, 'Target 2', hh_offgrid,
                                                     noncamplightingoffgridtypes, lightingoffgridcost,
                                                     elecgriddirectenergy, country_noncampelecgridco2)
        assert offgrid_expenditure == 0.002157308870967376
        assert offgrid_capital_costs == 0.2328784963573492
        assert offgrid_co2_emissions == 8.238647374164904
        co2_emissions = grid_co2_emissions + offgrid_co2_emissions
        assert co2_emissions == 8.504008033281535

        noncampcookingsolidtypes = {'Urban Baseline Type': 2, 'Urban Target 1 Type': 2, 'Urban Target 2 Type': 7,
                                    'Urban Target 3 Type': 8, 'Rural Baseline Type': 1, 'Rural Target 1 Type': 1,
                                    'Rural Target 2 Type': 7, 'Rural Target 3 Type': 8, 'Slum Baseline Type': 1,
                                    'Slum Target 1 Type': 1, 'Slum Target 2 Type': 7, 'Slum Target 3 Type': 8}
        solid_expenditure, solid_capital_costs, solid_co2_emissions = \
            model.calculate_noncamp_solid_cooking(pop_type, 'Target 3', hh_no_nonsolid_access,
                                                  noncampcookingsolidtypes, cookingsolidcost)
        assert solid_expenditure == 0.3275797640995024
        assert solid_capital_costs == 0.0989177392294985
        assert solid_co2_emissions == 290.5365064344328
        co2_emissions = nonsolid_co2_emissions + solid_co2_emissions
        assert co2_emissions == 312.7742918019583

    def testCampModel(self, camptypes, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost):
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
        offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions = \
            model.calculate_camp_offgrid_lighting('Baseline', number_hh, camp_camptypes, lightingoffgridcost,
                                                  elecgriddirectenergy, elecco2)
        assert offgrid_expenditure == 9.775621570875002
        assert offgrid_capital_costs == 4.1443202
        assert offgrid_co2_emissions == 15827.062570875001
        solid_expenditure, solid_capital_costs, solid_co2_emissions = \
            model.calculate_camp_solid_cooking('Target 1', number_hh, camp_camptypes, cookingsolidcost)
        assert solid_expenditure == 7.15870727889462
        assert solid_capital_costs == 9.20896466314155
        assert solid_co2_emissions == 189409.2845950458

    def testSmallCampModel(self, small_camptypes, lightingoffgridcost, elecgriddirectenergy, cookingsolidcost):
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

        offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions = \
            model.calculate_camp_offgrid_lighting('Baseline', number_hh, campgroup_camptypes, lightingoffgridcost,
                                                  elecgriddirectenergy, elecco2)
        assert offgrid_expenditure == 0.894614187294
        assert offgrid_capital_costs == 0.37926669120000006
        assert offgrid_co2_emissions == 1442.1696815239604
        solid_expenditure, solid_capital_costs, solid_co2_emissions = \
            model.calculate_camp_solid_cooking('Target 3', number_hh, campgroup_camptypes, cookingsolidcost)
        assert solid_expenditure is None
        assert solid_capital_costs is None
        assert solid_co2_emissions is None

        camp_group = 'Asia E'
        smallcamps = {camp_group: 19577.01373}
        small_camps_elecgridco2 = {camp_group: ''}
        number_hh = model.calculate_number_hh(smallcamps[camp_group])
        campgroup_camptypes = small_camptypes.get(camp_group)
        elecco2 = small_camps_elecgridco2[camp_group]

        offgrid_expenditure, offgrid_capital_costs, offgrid_co2_emissions = \
            model.calculate_camp_offgrid_lighting('Target 2', number_hh, campgroup_camptypes, lightingoffgridcost,
                                                  elecgriddirectenergy, elecco2)
        assert offgrid_expenditure is None
        assert offgrid_capital_costs is None
        assert offgrid_co2_emissions is None
        solid_expenditure, solid_capital_costs, solid_co2_emissions = \
            model.calculate_camp_solid_cooking('Baseline', number_hh, campgroup_camptypes, cookingsolidcost)
        assert solid_expenditure == 0.6505642389516176
        assert solid_capital_costs == 0.00916640744582953
        assert solid_co2_emissions == 11087.728057822153