#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join

from datetime import datetime
from tempfile import gettempdir

from hdx.data.dataset import Dataset
from hdx.facades.hdx_scraperwiki import facade
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import avg_dicts, float_value_convert, key_value_convert, integer_value_convert, \
    write_list_to_csv
from hdx.utilities.downloader import Download
from hdx.location.country import Country

from chathamhouse.chathamhousedata import get_camp_non_camp_populations, get_worldbank_series, \
    get_slumratios, get_camptypes, generate_dataset_and_showcase
from chathamhouse.chathamhousemodel import ChathamHouseModel

logger = logging.getLogger(__name__)


def get_iso3(name):
    iso3, match = Country.get_iso3_country_code_fuzzy(name, exception=ValueError)
    if not match:
        logger.info('Country %s matched to ISO3: %s!' % (name, iso3))
    return iso3


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    with Download() as downloader:
        constants = float_value_convert(downloader.download_tabular_key_value(configuration['constants_url']))
        constants['Lighting Grid Tier'] = int(constants['Lighting Grid Tier'])

        camp_accommodation_types = downloader.download_tabular_key_value(configuration['camp_accommodation_tyes_url'])
        datasets = Dataset.search_in_hdx('displacement', fq='organization:unhcr')
        unhcr_non_camp, unhcr_camp, unhcr_camp_excluded = get_camp_non_camp_populations(constants['Non Camp Types'],
                                                                                        constants['Camp Types'],
                                                                                        camp_accommodation_types,
                                                                                        datasets)

        world_bank_url = configuration['world_bank_url']
        urbanratios = get_worldbank_series(world_bank_url % configuration['urban_ratio_wb'], downloader)
        slumratios = get_slumratios(configuration['slum_ratio_url'])

        noncamp_elec_access = dict()
        noncamp_elec_access['Urban'] = get_worldbank_series(world_bank_url % configuration['urban_elec_wb'], downloader)
        noncamp_elec_access['Rural'] = get_worldbank_series(world_bank_url % configuration['rural_elec_wb'], downloader)
        noncamp_elec_access['Slum'] = avg_dicts(noncamp_elec_access['Urban'], noncamp_elec_access['Rural'])

        ieadata = downloader.download_tabular_cols_as_dicts(configuration['iea_data_url'])
        elecappliances = key_value_convert(ieadata['Electrical Appliances'], keyfn=get_iso3, valuefn=float,
                                           dropfailedkeys=True)
        cookinglpg = key_value_convert(ieadata['Cooking LPG'], keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
        elecgridtiers = key_value_convert(downloader.download_tabular_key_value(configuration['elec_grid_tiers_url']), keyfn=int, valuefn=float)
        elecgriddirectenergy = float_value_convert(downloader.download_tabular_key_value(configuration['elec_grid_direct_energy_url']))
        noncampelecgridco2 = key_value_convert(downloader.download_tabular_key_value(configuration['noncamp_elec_grid_co2_url']),
                                    keyfn=get_iso3, valuefn=float, dropfailedkeys=True)

        noncamptypes = downloader.download_tabular_cols_as_dicts(configuration['noncamp_types_url'])
        noncamplightingoffgridtypes = integer_value_convert(noncamptypes['Lighting OffGrid'])
        noncampcookingsolidtypes = integer_value_convert(noncamptypes['Cooking Solid'])

        camptypes = get_camptypes(configuration['camp_types_url'], downloader)

        costs = downloader.download_tabular_cols_as_dicts(configuration['costs_url'])
        lightingoffgridcost = float_value_convert(costs['Lighting OffGrid'])
        cookingsolidcost = float_value_convert(costs['Cooking Solid'])

        noncamp_nonsolid_access = downloader.download_tabular_cols_as_dicts(configuration['noncamp_cooking_nonsolid_url'])
        noncamp_nonsolid_access['Urban'] = key_value_convert(noncamp_nonsolid_access['Urban'],
                                                     keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
        noncamp_nonsolid_access['Rural'] = key_value_convert(noncamp_nonsolid_access['Rural'],
                                                     keyfn=get_iso3, valuefn=float, dropfailedkeys=True)
        noncamp_nonsolid_access['Slum'] = noncamp_nonsolid_access['Urban']

        small_camptypes = get_camptypes(configuration['small_camptypes_url'], downloader)
        small_camp_data = downloader.download_tabular_cols_as_dicts(configuration['small_camps_data_url'])
        smallcamps = float_value_convert(small_camp_data['Population'])
        small_camps_elecgridco2 = float_value_convert(small_camp_data['Electricity Grid CO2'])

        type_descriptions = downloader.download_tabular_cols_as_dicts(configuration['type_descriptions_url'])
        lighting_type_descriptions = type_descriptions['Lighting Descriptions']
        cooking_type_descriptions = type_descriptions['Cooking Descriptions']

    model = ChathamHouseModel(constants)
    pop_types = ['Urban', 'Slum', 'Rural', 'Camp', 'Small Camp']
    headers = list()
    results = list()

    for pop_type in pop_types:
        results.append(list())
        if pop_type == 'Camp':
            headers.append(['ISO3 Country Code', 'Country Name', 'Camp Name'])
            hxlheaders = ['#country+code', '#country+name', '#loc+name']
        elif pop_type == 'Small Camp':
            headers.append(['Region'])
            hxlheaders = ['#region+name']
        else:
            headers.append(['ISO3 Country Code', 'Country Name'])
            hxlheaders = ['#country+code', '#country+name']
        headers[-1].extend(['Population', 'Tier'])
        hxlheaders.extend(['#population+num', '#indicator+tier'])
        if pop_type not in ['Camp', 'Small Camp']:
            headers[-1].extend(['Grid Expenditure ($m/yr)', 'Grid CO2 Emissions (t/yr)'])
            hxlheaders.extend(['#indicator+value+grid+expenditure', '#indicator+value+grid+co2_emissions'])
        headers[-1].extend(['Offgrid Type', 'Lighting Type Description', 'Offgrid Expenditure ($m/yr)',
                            'Offgrid Capital Costs ($m)', 'Offgrid CO2 Emissions (t/yr)'])
        hxlheaders.extend(['#indicator+type+offgrid', '#indicator+text+lighting', '#indicator+value+offgrid+expenditure',
                           '#indicator+value+offgrid+capital_costs', '#indicator+value+offgrid+co2_emissions'])
        if pop_type not in ['Camp', 'Small Camp']:
            headers[-1].extend(['Nonsolid Expenditure ($m/yr)', 'Nonsolid CO2 Emissions (t/yr)'])
            hxlheaders.extend(['#indicator+value+nonsolid+expenditure', '#indicator+value+nonsolid+co2_emissions'])
        headers[-1].extend(['Solid Type', 'Cooking Type Description', 'Solid Expenditure ($m/yr)',
                            'Solid Capital Costs ($m)', 'Solid CO2_Emissions (t/yr)'])
        hxlheaders.extend(['#indicator+type+solid', '#indicator+text+cooking', '#indicator+value+solid+expenditure',
                           '#indicator+value+solid+capital_costs', '#indicator+value+solid+co2_emissions'])
        results[pop_types.index(pop_type)].append(hxlheaders)

    today = datetime.now()
    dataset, showcase = generate_dataset_and_showcase(pop_types, today)
    resources = dataset.get_resources()

    for iso3 in sorted(unhcr_non_camp):
        number_hh_by_pop_type = model.calculate_population(iso3, unhcr_non_camp, urbanratios, slumratios)
        country_elecappliances = elecappliances.get(iso3)
        if country_elecappliances is None:
            country_elecappliances = model.calculate_regional_average('Electrical Appliances', elecappliances, iso3)
        country_noncampelecgridco2 = noncampelecgridco2.get(iso3)
        if country_noncampelecgridco2 is None:
            country_noncampelecgridco2 = model.calculate_regional_average('Grid CO2', noncampelecgridco2, iso3)
        country_cookinglpg = cookinglpg.get(iso3)
        if country_cookinglpg is None:
            country_cookinglpg = model.calculate_regional_average('LPG', cookinglpg, iso3)

        for pop_type in number_hh_by_pop_type:
            number_hh = number_hh_by_pop_type[pop_type]

            country_elec_access = noncamp_elec_access[pop_type].get(iso3)
            if country_elec_access is None:
                country_elec_access = model.calculate_regional_average('Grid access', noncamp_elec_access[pop_type],
                                                                       iso3)
            hh_grid_access, hh_offgrid = model.calculate_hh_access(number_hh, country_elec_access)

            country_noncamp_nonsolid_access = noncamp_nonsolid_access[pop_type].get(iso3)
            if country_noncamp_nonsolid_access is None:
                country_noncamp_nonsolid_access = model.calculate_regional_average('Nonsolid access',
                                                                                   noncamp_nonsolid_access[pop_type],
                                                                                   iso3)
            hh_nonsolid_access, hh_no_nonsolid_access = \
                model.calculate_hh_access(number_hh, country_noncamp_nonsolid_access)

            ge, gc = model.calculate_ongrid_lighting(hh_grid_access, elecgridtiers, country_elecappliances,
                                                     country_noncampelecgridco2)
            ne, nc = model.calculate_non_solid_cooking(hh_nonsolid_access, country_cookinglpg)

            for tier in model.tiers:
                baseline_target = model.get_baseline_target(tier)
                noncamplightingoffgridtype = model.get_noncamp_type(noncamplightingoffgridtypes, pop_type, tier)
                noncamplightingtypedesc = model.get_description(lighting_type_descriptions, baseline_target,
                                                                noncamplightingoffgridtype)
                oe, oc, oco2 = model.calculate_offgrid_lighting(baseline_target, hh_offgrid, noncamplightingoffgridtype,
                                                                lightingoffgridcost, elecgriddirectenergy,
                                                                country_noncampelecgridco2)
                noncampcookingsolidtype = model.get_noncamp_type(noncampcookingsolidtypes, pop_type, tier)
                noncampcookingtypedesc = model.get_description(cooking_type_descriptions, baseline_target,
                                                               noncamplightingoffgridtype)
                se, sc, sco2 = model.calculate_solid_cooking(baseline_target, hh_no_nonsolid_access,
                                                             noncampcookingsolidtype, cookingsolidcost)

                cn = Country.get_country_name_from_iso3(iso3)
                population = model.calculate_population_from_hh(number_hh)
                row = [iso3, cn, population, tier, ge, gc, noncamplightingoffgridtype, noncamplightingtypedesc,
                       oe, oc, oco2, ne, nc, noncampcookingsolidtype, noncampcookingtypedesc, se, sc, sco2]
                results[pop_types.index(pop_type.capitalize())].append(row)

    for camp in sorted(camptypes):
        result = unhcr_camp.get(camp)
        if result is None:
            firstpart = camp.split(':')[0].strip()
            for campname in sorted(unhcr_camp):
                if firstpart in campname:
                    result = unhcr_camp[campname]
                    logger.info('Matched first part of name of %s to UNHCR name: %s' % (camp, campname))
                    break
        if result is None:
            camptype = unhcr_camp_excluded.get(camp)
            if camptype is None:
                logger.info('Missing camp %s in UNHCR data!' % camp)
            else:
                logger.info('Camp %s is in UNHCR data but has camp type %s!' % (camp, camptype))
            continue
        population, iso3 = result
        number_hh = model.calculate_number_hh(population)
        camp_camptypes = camptypes[camp]

        country_noncampelecgridco2 = noncampelecgridco2.get(iso3)
        if country_noncampelecgridco2 is None:
            country_noncampelecgridco2 = model.calculate_regional_average('Grid CO2', noncampelecgridco2, iso3)

        for tier in model.tiers:
            baseline_target = model.get_baseline_target(tier)
            camplightingoffgridtype = camp_camptypes['Lighting OffGrid %s' % tier]
            camplightingtypedesc = model.get_description(lighting_type_descriptions, baseline_target,
                                                         camplightingoffgridtype)
            oe, oc, oco2 = model.calculate_offgrid_lighting(baseline_target, number_hh, camplightingoffgridtype,
                                                            lightingoffgridcost, elecgriddirectenergy,
                                                            country_noncampelecgridco2)
            campcookingsolidtype = camp_camptypes['Cooking Solid %s' % tier]
            campcookingtypedesc = model.get_description(cooking_type_descriptions, baseline_target,
                                                        campcookingsolidtype)
            se, sc, sco2 = model.calculate_solid_cooking(baseline_target, number_hh, campcookingsolidtype,
                                                         cookingsolidcost)
            cn = Country.get_country_name_from_iso3(iso3)
            row = [iso3, cn, camp, population, tier, camplightingoffgridtype, camplightingtypedesc, oe, oc, oco2,
                   campcookingsolidtype, campcookingtypedesc, se, sc, sco2]
            results[pop_types.index('Camp')].append(row)

    for region in sorted(smallcamps):
        population = smallcamps[region]
        if not population or population == '-':
            continue
        number_hh = model.calculate_number_hh(population)
        region_camptypes = small_camptypes.get(region)
        if region_camptypes is None:
            logger.info('Missing camp group %s in small camp types!' % region)
            continue

        elecco2 = small_camps_elecgridco2[region]
        if not elecco2 or elecco2 == '-':
            elecco2 = 0

        for tier in model.tiers:
            baseline_target = model.get_baseline_target(tier)
            camplightingoffgridtype = region_camptypes['Lighting OffGrid %s' % tier]
            oe, oc, oco2 = '', '', ''
            camplightingtypedesc = ''
            if camplightingoffgridtype:
                camplightingtypedesc = model.get_description(lighting_type_descriptions, baseline_target,
                                                             camplightingoffgridtype)
                oe, oc, oco2 = model.calculate_offgrid_lighting(baseline_target, number_hh, camplightingoffgridtype,
                                                                lightingoffgridcost, elecgriddirectenergy, elecco2)
            campcookingsolidtype = region_camptypes['Cooking Solid %s' % tier]
            se, sc, sco2 = '', '', ''
            campcookingtypedesc = ''
            if campcookingsolidtype:
                campcookingtypedesc = model.get_description(cooking_type_descriptions, baseline_target,
                                                            campcookingsolidtype)
                se, sc, sco2 = model.calculate_solid_cooking(baseline_target, number_hh, campcookingsolidtype,
                                                             cookingsolidcost)
            row = [region, model.round(population), tier, camplightingoffgridtype, camplightingtypedesc, oe, oc, oco2,
                   campcookingsolidtype, campcookingtypedesc, se, sc, sco2]
            results[pop_types.index('Small Camp')].append(row)

    folder = gettempdir()
    for i, pop_type in enumerate(pop_types):
        resource = resources[i]
        file_to_upload = write_list_to_csv(results[i], folder, resource['name'], headers=headers[i])
        resource.set_file_to_upload(file_to_upload)

    # dataset.update_from_yaml()
    # dataset.create_in_hdx()
    # showcase.create_in_hdx()
    # showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))
