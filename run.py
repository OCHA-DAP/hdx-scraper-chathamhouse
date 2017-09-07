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

from chathamhouse import get_worldbank_iso2_to_iso3, generate_dataset_and_showcase, get_population, \
    get_worldbank_series, get_slumratios, country_iso_key_convert, integer_key_convert

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    downloader = Download()
    constants = downloader.download_csv_key_value(configuration['constants_url'])
    unhcr_non_camp = get_population()
    wbiso2iso3 = get_worldbank_iso2_to_iso3(configuration['wb_countries_url'], downloader)
    world_bank_url = configuration['world_bank_url']
    urbanratios = get_worldbank_series(world_bank_url % configuration['urban_ratio_wb'], downloader, wbiso2iso3)
    slumratios = get_slumratios(configuration['slum_ratio_url'])
    urbanelec = get_worldbank_series(world_bank_url % configuration['urban_elec_wb'], downloader, wbiso2iso3)
    elecappliances = country_iso_key_convert(downloader.download_csv_key_value(configuration['elec_appliances_url']))
    electiers = downloader.download_csv_key_value(configuration['elec_tiers_url'])
    lighting = downloader.download_csv_key_value(configuration['lighting_url'])
    elecco2 = country_iso_key_convert(downloader.download_csv_key_value(configuration['elec_co2_url']))

    for iso3 in sorted(unhcr_non_camp):
        urbanratio = urbanratios.get(iso3)
        if not urbanratio:
            continue
        combined_urbanratio = (1 - urbanratio) * float(constants['Population Adjustment Factor']) + urbanratio
        displaced_population = unhcr_non_camp[iso3]
        urban_displaced_population = displaced_population * combined_urbanratio
        rural_displaced_population = displaced_population - urban_displaced_population
        slumratio = slumratios.get(iso3)
        if not slumratio:
            continue
        slum_displaced_population = urban_displaced_population * slumratio
        urban_minus_slum_displaced_population = urban_displaced_population - slum_displaced_population

        displaced_urban_hh = urban_minus_slum_displaced_population / 5
        hh_grid_access = displaced_urban_hh * urbanelec[iso3]
        kWh_per_yr_per_hh = elecappliances[iso3]
        if kWh_per_yr_per_hh == 0:
            tier = constants['Baseline Lighting Grid Tier']
            kWh_per_yr_per_hh = electiers[tier]
        expenditure_on_grid_dlrs_per_year_per_hh = constants['Electricity Cost'] * kWh_per_yr_per_hh
        ongrid_expenditure = hh_grid_access * expenditure_on_grid_dlrs_per_year_per_hh / 1000000

        offgrid_expenditure = (displaced_urban_hh - hh_grid_access) * 12 * \
                              constants['Baseline Lighting Offgrid Scaling Factor'] / 1000000 * \
                              lighting['Baseline Total fuel cost']
        offgrid_capital_costs = (displaced_urban_hh - hh_grid_access) * \
                                constants['Baseline Lighting Offgrid Scaling Factor'] / 1000000 * \
                              lighting['Baseline Total capital cost']
        ongrid_co2_emissions_per_hh = kWh_per_yr_per_hh * elecco2[iso3]
        ongrid_co2_emissions = hh_grid_access * ongrid_co2_emissions_per_hh / 1000 + \
                               (displaced_urban_hh-hh_grid_access) * \
                               constants['Baseline Lighting Offgrid Scaling Factor'] / 1000 *\
                               (lighting['Baseline Direct CO2'] +
                                lighting['Baseline Grid Primary energy'] * elecco2[iso3])
        pass
    # logger.info('Number of datasets to upload: %d' % len(countriesdata))
    # for countrydata in countriesdata:
    #     dataset, showcase = generate_dataset_and_showcase(downloader, countrydata)
    #     dataset.update_from_yaml()
    #     dataset.create_in_hdx()
    #     showcase.create_in_hdx()
    #     showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))
