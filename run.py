#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from functools import partial
from os.path import join

from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.dictandlist import avg_dicts, float_value_convert, key_value_convert, integer_value_convert
from hdx.utilities.downloader import Download
from hdx.utilities.location import Location

from chathamhouse_model import ChathamHouseModel
from chathamhouse_data import get_worldbank_iso2_to_iso3, get_camp_non_camp_populations, get_worldbank_series, \
    get_slumratios

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    downloader = Download()
    constants = float_value_convert(downloader.download_csv_key_value(configuration['constants_url']))
    constants['Lighting Grid Tier'] = int(constants['Lighting Grid Tier'])

    wbiso2iso3 = get_worldbank_iso2_to_iso3(configuration['wb_countries_url'], downloader)
    world_bank_url = configuration['world_bank_url']
    urbanratios = get_worldbank_series(world_bank_url % configuration['urban_ratio_wb'], downloader, wbiso2iso3)
    slumratios = get_slumratios(configuration['slum_ratio_url'])

    elec_access = dict()
    elec_access['Urban'] = get_worldbank_series(world_bank_url % configuration['urban_elec_wb'], downloader, wbiso2iso3)
    elec_access['Rural'] = get_worldbank_series(world_bank_url % configuration['rural_elec_wb'], downloader, wbiso2iso3)
    elec_access['Slum'] = avg_dicts(elec_access['Urban'], elec_access['Rural'])

    get_iso3 = partial(Location.get_iso3_country_code, exception=ValueError)

    ieadata = downloader.download_csv_cols_as_dicts(configuration['iea_data_url'])
    elecappliances = key_value_convert(ieadata['Electrical Appliances'], keyfn=get_iso3, valuefn=float,
                                       dropfailedkeys=True)
    cookinglpg = key_value_convert(ieadata['Cooking LPG'], keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
    elecgrid = key_value_convert(downloader.download_csv_key_value(configuration['elec_grid_url']), keyfn=int, valuefn=float)

    noncamptypes = downloader.download_csv_cols_as_dicts(configuration['noncamp_types_url'])
    noncamplightingoffgridtypes = integer_value_convert(noncamptypes['Lighting OffGrid'])
    noncampcookingsolidtypes = integer_value_convert(noncamptypes['Cooking Solid'])

    camptypes = downloader.download_csv_rows_as_dicts(configuration['camp_types_url'])
    for key in camptypes:
        camptypes[key] = integer_value_convert(camptypes[key])

    costs = downloader.download_csv_cols_as_dicts(configuration['costs_url'])
    lightingoffgridcost = float_value_convert(costs['Lighting OffGrid'])
    cookingsolidcost = float_value_convert(costs['Cooking Solid'])

    elecco2 = key_value_convert(downloader.download_csv_key_value(configuration['elec_co2_url']),
                                keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
    nonsolid_access = downloader.download_csv_cols_as_dicts(configuration['cooking_nonsolid_url'])
    nonsolid_access['Urban'] = key_value_convert(nonsolid_access['Urban'],
                                                 keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
    nonsolid_access['Rural'] = key_value_convert(nonsolid_access['Rural'],
                                                 keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
    nonsolid_access['Slum'] = nonsolid_access['Urban']

    model = ChathamHouseModel(constants)

    unhcr_non_camp, unhcr_camp = get_camp_non_camp_populations(constants['Non Camp Types'], constants['Camp Types'])

    non_camp_results = dict()
    for iso3 in sorted(unhcr_non_camp):
        number_hh_by_pop_type = model.calculate_population(iso3, unhcr_non_camp, urbanratios, slumratios)
        if number_hh_by_pop_type is None:
            continue

        non_camp_results[iso3] = dict()
        for pop_type in number_hh_by_pop_type:
            number_hh = number_hh_by_pop_type[pop_type]

            country_elec_access = elec_access[pop_type].get(iso3)
            if country_elec_access is None:
                logger.info('Missing electricity access data for %s!' % iso3)
                continue
            hh_grid_access, hh_offgrid = model.calculate_hh_access(number_hh, country_elec_access)

            hh_nonsolid_access, hh_no_nonsolid_access = \
                model.calculate_hh_access(number_hh, nonsolid_access[pop_type][iso3])

            country_elecappliances = elecappliances.get(iso3)
            if country_elecappliances is None:
                logger.info('Missing electricity appliances data for %s!' % iso3)
                continue
            res = dict()
            res['grid_expenditure'], res['grid_co2_emissions'] = \
                model.calculate_ongrid_lighting(hh_grid_access, elecgrid, country_elecappliances, elecco2[iso3])
            res['nonsolid_expenditure'], res['nonsolid_co2_emissions'] = \
                model.calculate_non_solid_cooking(hh_nonsolid_access, cookinglpg[iso3])

            offgrid_expenditure = list()
            offgrid_capital_costs = list()
            offgrid_co2_emissions = list()
            solid_expenditure = list()
            solid_capital_costs = list()
            solid_co2_emissions = list()
            for tier in model.tiers:
                oe, oc, oco2 = model.calculate_noncamp_offgrid_lighting(pop_type, tier, iso3, hh_offgrid,
                                                                        noncamplightingoffgridtypes,
                                                                        lightingoffgridcost,
                                                                        elecco2)
                offgrid_expenditure.append(oe)
                offgrid_capital_costs.append(oc)
                offgrid_co2_emissions.append(oco2)
                se, sc, sco2 = model.calculate_noncamp_solid_cooking(pop_type, tier, hh_no_nonsolid_access,
                                                                     noncampcookingsolidtypes, cookingsolidcost)
                solid_expenditure.append(se)
                solid_capital_costs.append(sc)
                solid_co2_emissions.append(sco2)
            res['offgrid_expenditure'] = offgrid_expenditure
            res['offgrid_capital_costs'] = offgrid_capital_costs
            res['offgrid_co2_emissions'] = offgrid_co2_emissions
            res['solid_expenditure'] = solid_expenditure
            res['solid_capital_costs'] = solid_capital_costs
            res['solid_co2_emissions'] = solid_co2_emissions

            non_camp_results[iso3][pop_type] = res

    camp_results = dict()
    for camp in sorted(unhcr_camp):
        number_hh = model.calculate_number_hh(unhcr_camp[camp])
        country_camptypes = camptypes.get(camp)
        if country_camptypes is None:
            logger.info('Missing camp %s in camp types!' % camp)
            continue

        offgrid_expenditure = list()
        offgrid_capital_costs = list()
        offgrid_co2_emissions = list()
        solid_expenditure = list()
        solid_capital_costs = list()
        solid_co2_emissions = list()
        for tier in model.tiers:
            oe, oc, oco2 = model.calculate_camp_offgrid_lighting(tier, number_hh, country_camptypes, lightingoffgridcost)
            offgrid_expenditure.append(oe)
            offgrid_capital_costs.append(oc)
            offgrid_co2_emissions.append(oco2)
            se, sc, sco2 = model.calculate_camp_solid_cooking(tier, number_hh, country_camptypes, cookingsolidcost)
            solid_expenditure.append(se)
            solid_capital_costs.append(sc)
            solid_co2_emissions.append(sco2)
        res = dict()
        res['offgrid_expenditure'] = offgrid_expenditure
        res['offgrid_capital_costs'] = offgrid_capital_costs
        res['offgrid_co2_emissions'] = offgrid_co2_emissions
        res['solid_expenditure'] = solid_expenditure
        res['solid_capital_costs'] = solid_capital_costs
        res['solid_co2_emissions'] = solid_co2_emissions

        camp_results[camp] = res

        # logger.info('Number of datasets to upload: %d' % len(countriesdata))
    # for countrydata in countriesdata:
    #     dataset, showcase = generate_dataset_and_showcase(downloader, countrydata)
    #     dataset.update_from_yaml()
    #     dataset.create_in_hdx()
    #     showcase.create_in_hdx()
    #     showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='prod', project_config_yaml=join('config', 'project_configuration.yml'))
