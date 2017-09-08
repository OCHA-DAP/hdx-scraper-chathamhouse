from hdx.utilities.location import Location

from chathamhouse_model import logger


def country_iso_key_convert(dictin):
    dictout = dict()
    for country in dictin:
        iso3, match = Location.get_iso3_country_code(country)
        if iso3 and match:
            dictout[iso3] = dictin[country]
        else:
            logger.info('No match for %s' % country)
    return dictout


def integer_key_convert(dictin):
    dictout = dict()
    for key in dictin:
        dictout[int(key)] = dictin[key]
    return dictout


def float_value_convert(dictin):
    dictout = dict()
    for key in dictin:
        dictout[key] = float(dictin[key])
    return dictout


def avg_dicts(dictin1, dictin2):
    dictout = dict()
    for key in dictin1:
        dictout[key] = (dictin1[key] + dictin2[key]) / 2.0
    return dictout