#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download
from hdx.utilities.location import Location
from tabulator import Stream

from chathamhouse_model import ChathamHouseModel
from utils import country_iso_key_convert, integer_key_convert, float_value_convert, avg_dicts
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
    elecappliances = float_value_convert(country_iso_key_convert(downloader.download_csv_key_value(configuration['elec_appliances_url'])))
    electiers = float_value_convert(integer_key_convert(downloader.download_csv_key_value(configuration['elec_tiers_url'])))
    lighting = float_value_convert(downloader.download_csv_key_value(configuration['lighting_url']))
    elecco2 = float_value_convert(country_iso_key_convert(downloader.download_csv_key_value(configuration['elec_co2_url'])))
    nonsolid_access = dict()
    nonsolid_access['Urban'] = float_value_convert(country_iso_key_convert(downloader.download_csv_key_value(configuration['urban_nonsolid_url'])))
    nonsolid_access['Rural'] = float_value_convert(country_iso_key_convert(downloader.download_csv_key_value(configuration['rural_nonsolid_url'])))
    nonsolid_access['Slum'] = nonsolid_access['Urban']
    cookinglpg = float_value_convert(country_iso_key_convert(downloader.download_csv_key_value(configuration['cooking_lpg_url'])))
    cooking = float_value_convert(downloader.download_csv_key_value(configuration['cooking_url']))

    model = ChathamHouseModel(constants)

    unhcr_non_camp, unhcr_camp = get_camp_non_camp_populations(constants['Non Camp Types'], constants['Camp Types'])
    non_camp_results = dict()
    for iso3 in sorted(unhcr_non_camp):
        number_hh = model.calculate_population(iso3, unhcr_non_camp, urbanratios, slumratios)
        non_camp_results[iso3] = dict()
        for pop_type in number_hh.keys():
            res = dict()

            hh_grid_access, hh_offgrid = model.calculate_hh_access(number_hh[pop_type],
                                                                   elec_access[pop_type][iso3])
            if hh_grid_access is None:
                continue
            hh_nonsolid_access, hh_no_nonsolid_access = \
                model.calculate_hh_access(number_hh[pop_type], nonsolid_access[pop_type][iso3])

            res['grid_expenditure'], res['grid_co2_emissions'] = \
                model.calculate_ongrid_lighting(iso3, hh_grid_access, electiers, elecappliances, elecco2)
            res['nonsolid_expenditure'], res['nonsolid_co2_emissions'] = \
                model.calculate_non_solid_cooking(iso3, hh_nonsolid_access, cookinglpg)

            offgrid_expenditure = list()
            offgrid_capital_costs = list()
            offgrid_co2_emissions = list()
            solid_expenditure = list()
            solid_capital_costs = list()
            solid_co2_emissions = list()
            for level in model.levels:
                oe, oc, oco2 = model.calculate_offgrid_lighting(pop_type, level, iso3, hh_offgrid, lighting, elecco2)
                offgrid_expenditure.append(oe)
                offgrid_capital_costs.append(oc)
                offgrid_co2_emissions.append(oco2)
                se, sc, sco2 = model.calculate_solid_cooking(pop_type, level, iso3, hh_no_nonsolid_access, cooking)
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


    # logger.info('Number of datasets to upload: %d' % len(countriesdata))
    # for countrydata in countriesdata:
    #     dataset, showcase = generate_dataset_and_showcase(downloader, countrydata)
    #     dataset.update_from_yaml()
    #     dataset.create_in_hdx()
    #     showcase.create_in_hdx()
    #     showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='prod', project_config_yaml=join('config', 'project_configuration.yml'))
