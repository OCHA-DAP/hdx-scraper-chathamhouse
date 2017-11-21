#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import copy
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
    get_slumratios, get_camptypes, generate_dataset_and_showcase, check_name_dispersed, append_value, \
    get_camptypes_fallbacks, get_iso3
from chathamhouse.chathamhousemodel import ChathamHouseModel

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    with Download() as downloader:
        constants = float_value_convert(downloader.download_tabular_key_value(configuration['constants_url']))
        constants['Lighting Grid Tier'] = int(constants['Lighting Grid Tier'])

        camp_overrides = downloader.download_tabular_cols_as_dicts(configuration['camp_overrides_url'])
        camp_overrides['Population'] = integer_value_convert(camp_overrides['Population'], dropfailedvalues=True)
        camp_overrides['Country'] = key_value_convert(camp_overrides['Country'], valuefn=get_iso3)
        datasets = Dataset.search_in_hdx('displacement', fq='organization:unhcr')
        all_camps_per_country, unhcr_non_camp, unhcr_camp, unhcr_camp_excluded = \
            get_camp_non_camp_populations(constants['Non Camp Types'], constants['Camp Types'],
                                          camp_overrides, datasets, downloader)
        country_totals = copy.deepcopy(all_camps_per_country)

        world_bank_url = configuration['world_bank_url']
        urbanratios = get_worldbank_series(world_bank_url % configuration['urban_ratio_wb'], downloader)
        slumratios = get_slumratios(configuration['slum_ratio_url'], downloader)

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
        elecgridco2 = key_value_convert(downloader.download_tabular_key_value(configuration['elec_grid_co2_url']),
                                        keyfn=get_iso3, valuefn=float, dropfailedkeys=True)

        def get_elecgridco2(iso, inf):
            elgridco2 = elecgridco2.get(iso)
            if elgridco2 is None:
                elgridco2, reg = model.calculate_regional_average('Grid CO2', elecgridco2, iso)
                inf.append('elco2(%s)=%.3g' % (reg, elgridco2))
            return elgridco2

        noncamptypes = downloader.download_tabular_cols_as_dicts(configuration['noncamp_types_url'])
        noncamplightingoffgridtypes = integer_value_convert(noncamptypes['Lighting OffGrid'])
        noncampcookingsolidtypes = integer_value_convert(noncamptypes['Cooking Solid'])

        camptypes = get_camptypes(configuration['camp_types_url'], downloader)
        camptypes_fallbacks_offgrid, camptypes_fallbacks_solid = \
            get_camptypes_fallbacks(configuration['camp_types_fallbacks_url'], downloader, keyfn=get_iso3)

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

    for i, pop_type in enumerate(pop_types):
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
        if pop_type != 'Small Camp':
            headers[-1].append('Info')
            hxlheaders.append('#meta+info')

        results[i].append(hxlheaders)

    results.append(list())
    headers.append(['ISO3 Country Code', 'Country Name', 'Population'])
    hxlheaders = ['#country+code', '#country+name', '#population+num']
    results[len(results)-1].append(hxlheaders)

    today = datetime.now()
    dataset, showcase = generate_dataset_and_showcase(pop_types, today)
    resources = dataset.get_resources()

    for iso3 in sorted(unhcr_non_camp):
        info = list()
        population = model.sum_population(unhcr_non_camp, iso3, all_camps_per_country)
        number_hh_by_pop_type = model.calculate_population(iso3, population, urbanratios, slumratios, info)
        country_elecappliances = elecappliances.get(iso3)
        if country_elecappliances is None:
            country_elecappliances, region = \
                model.calculate_regional_average('Electrical Appliances', elecappliances, iso3)
            info.append('elap(%s)=%.3g' % (region, country_elecappliances))
        country_elecgridco2 = get_elecgridco2(iso3, info)
        country_cookinglpg = cookinglpg.get(iso3)
        if country_cookinglpg is None:
            country_cookinglpg, region = model.calculate_regional_average('LPG', cookinglpg, iso3)
            info.append('lpg(%s)=%.3g' % (region, country_elecappliances))

        cn = Country.get_country_name_from_iso3(iso3)
        for pop_type in number_hh_by_pop_type:
            info2 = copy.deepcopy(info)
            number_hh = number_hh_by_pop_type[pop_type]

            country_elec_access = noncamp_elec_access[pop_type].get(iso3)
            if country_elec_access is None:
                country_elec_access, region = \
                    model.calculate_regional_average('Grid access', noncamp_elec_access[pop_type], iso3)
                info2.append('elac(%s)=%.3g' % (region, country_elecappliances))
            hh_grid_access, hh_offgrid = model.calculate_hh_access(number_hh, country_elec_access)

            country_noncamp_nonsolid_access = noncamp_nonsolid_access[pop_type].get(iso3)
            if country_noncamp_nonsolid_access is None:
                country_noncamp_nonsolid_access, region = \
                    model.calculate_regional_average('Nonsolid access', noncamp_nonsolid_access[pop_type], iso3)
                info2.append('nsac(%s)=%.3g' % (region, country_elecappliances))
            hh_nonsolid_access, hh_no_nonsolid_access = \
                model.calculate_hh_access(number_hh, country_noncamp_nonsolid_access)

            ge, gc = model.calculate_ongrid_lighting(hh_grid_access, elecgridtiers, country_elecappliances,
                                                     country_elecgridco2)
            ne, nc = model.calculate_non_solid_cooking(hh_nonsolid_access, country_cookinglpg)

            for tier in model.tiers:
                info3 = copy.deepcopy(info2)
                noncamplightingoffgridtype = model.get_noncamp_type(noncamplightingoffgridtypes, pop_type, tier)
                noncampcookingsolidtype = model.get_noncamp_type(noncampcookingsolidtypes, pop_type, tier)

                res = model.calculate_offgrid_solid(tier, hh_offgrid, lighting_type_descriptions,
                                                    noncamplightingoffgridtype, lightingoffgridcost,
                                                    elecgriddirectenergy, country_elecgridco2,
                                                    hh_no_nonsolid_access, cooking_type_descriptions,
                                                    noncampcookingsolidtype, cookingsolidcost)
                noncamplightingtypedesc, oe, oc, oco2, noncampcookingtypedesc, se, sc, sco2 = res

                population = model.calculate_population_from_hh(number_hh)
                info3 = ','.join(info3)
                row = [iso3, cn, population, tier, ge, gc, noncamplightingoffgridtype, noncamplightingtypedesc,
                       oe, oc, oco2, ne, nc, noncampcookingsolidtype, noncampcookingtypedesc, se, sc, sco2, info3]
                results[pop_types.index(pop_type.capitalize())].append(row)

    camp_offgridtypes_in_countries = dict()
    camp_solidtypes_in_countries = dict()
    missing_from_unhcr = list()
    for name in sorted(camptypes):
        info = list()
        unhcrcampname = name
        result = unhcr_camp.get(unhcrcampname)
        if result is None:
            firstpart = name.split(':')[0].strip()
            for unhcrcampname in sorted(unhcr_camp):
                if firstpart in unhcrcampname:
                    result = unhcr_camp[unhcrcampname]
                    logger.info('Matched first part of name of %s to UNHCR name: %s' % (name, unhcrcampname))
                    info.append('Matched %s' % firstpart)
                    break
        if result is None:
            camptype = unhcr_camp_excluded.get(name)
            if camptype is None:
                if check_name_dispersed(name):
                    logger.info('Camp %s from the spreadsheet has been treated as non-camp!' % name)
                else:
                    missing_from_unhcr.append(name)
            else:
                logger.info('Camp %s is in UNHCR data but has camp type %s!' % (name, camptype))
            continue
        population, iso3, accommodation_type = result
        del all_camps_per_country[iso3][accommodation_type][unhcrcampname]

        camp_camptypes = camptypes[name]

        number_hh = model.calculate_number_hh(population)
        country_elecgridco2 = get_elecgridco2(iso3, info)

        for tier in model.tiers:
            info2 = copy.deepcopy(info)
            camplightingoffgridtype = camp_camptypes['Lighting OffGrid %s' % tier]
            campcookingsolidtype = camp_camptypes['Cooking Solid %s' % tier]

            res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                                camplightingoffgridtype, lightingoffgridcost,
                                                elecgriddirectenergy, country_elecgridco2,
                                                number_hh, cooking_type_descriptions, campcookingsolidtype,
                                                cookingsolidcost)
            camplightingtypedesc, oe, oc, oco2, campcookingtypedesc, se, sc, sco2 = res

            cn = Country.get_country_name_from_iso3(iso3)
            info2 = ','.join(info2)
            row = [iso3, cn, name, population, tier, camplightingoffgridtype, camplightingtypedesc, oe, oc, oco2,
                   campcookingsolidtype, campcookingtypedesc, se, sc, sco2, info2]
            results[pop_types.index('Camp')].append(row)
            append_value(camp_offgridtypes_in_countries, iso3, tier, name, camplightingoffgridtype)
            append_value(camp_solidtypes_in_countries, iso3, tier, name, campcookingsolidtype)

    logger.info('The following camps are in the spreadsheet but not in the UNHCR data : %s' %
                ', '.join(missing_from_unhcr))

    for iso3 in sorted(country_totals):
        info = list()
        population = model.sum_population(country_totals, iso3)
        cn = Country.get_country_name_from_iso3(iso3)
        row = [iso3, cn, population]
        results[len(results)-1].append(row)

        extra_camp_types = all_camps_per_country[iso3]

        country_elecgridco2 = get_elecgridco2(iso3, info)

        for accommodation_type in sorted(extra_camp_types):
            camps = extra_camp_types[accommodation_type]
            for name in sorted(camps):
                info2 = copy.deepcopy(info)
                population = camps[name]
                number_hh = model.calculate_number_hh(population)
                offgrid_tiers_in_country = camp_offgridtypes_in_countries.get(iso3)
                if offgrid_tiers_in_country is None:
                    offgrid_tiers_in_country = camptypes_fallbacks_offgrid.get(iso3)
                    if offgrid_tiers_in_country is None:
                        logger.warning('For country %s, UNHCR data has extra camp %s with population %s and accommodation type %s' %
                                       (cn, name, population, accommodation_type))
                        continue
                info2.append('UNHCR only')
                for tier in offgrid_tiers_in_country:
                    info3 = copy.deepcopy(info2)
                    camplightingoffgridtype = offgrid_tiers_in_country[tier]
                    if isinstance(camplightingoffgridtype, int):
                        campcookingsolidtype = camptypes_fallbacks_solid[iso3][tier]
                        info3.append('Fallback')
                    else:
                        camplightingoffgridtype = model.round(model.calculate_average(offgrid_tiers_in_country[tier]))
                        campcookingsolidtype = model.round(model.calculate_average(camp_solidtypes_in_countries[iso3][tier]))

                    res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                                        camplightingoffgridtype, lightingoffgridcost,
                                                        elecgriddirectenergy, country_elecgridco2,
                                                        number_hh, cooking_type_descriptions, campcookingsolidtype,
                                                        cookingsolidcost)
                    camplightingtypedesc, oe, oc, oco2, campcookingtypedesc, se, sc, sco2 = res
                    info3 = ','.join(info3)
                    row = [iso3, cn, name, population, tier, camplightingoffgridtype, camplightingtypedesc, oe, oc, oco2,
                           campcookingsolidtype, campcookingtypedesc, se, sc, sco2, info3]
                    results[pop_types.index('Camp')].append(row)

    for region in sorted(smallcamps):
        info = list()
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
            info.append('Blank elco2')
            elecco2 = 0

        for tier in model.tiers:
            info2 = copy.deepcopy(info)
            camplightingoffgridtype = region_camptypes['Lighting OffGrid %s' % tier]
            campcookingsolidtype = region_camptypes['Cooking Solid %s' % tier]

            res = model.calculate_offgrid_solid(tier, number_hh, lighting_type_descriptions,
                                                camplightingoffgridtype, lightingoffgridcost,
                                                elecgriddirectenergy, elecco2,
                                                number_hh, cooking_type_descriptions, campcookingsolidtype,
                                                cookingsolidcost)
            camplightingtypedesc, oe, oc, oco2, campcookingtypedesc, se, sc, sco2 = res
            info2 = ','.join(info2)
            row = [region, model.round(population), tier, camplightingoffgridtype, camplightingtypedesc, oe, oc, oco2,
                   campcookingsolidtype, campcookingtypedesc, se, sc, sco2, info2]
            results[pop_types.index('Small Camp')].append(row)

    folder = gettempdir()
    for i, _ in enumerate(results):
        resource = resources[i]
        file_to_upload = write_list_to_csv(results[i], folder, resource['name'], headers=headers[i])
        resource.set_file_to_upload(file_to_upload)

    # dataset.update_from_yaml()
    # dataset.create_in_hdx()
    # showcase.create_in_hdx()
    # showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))
