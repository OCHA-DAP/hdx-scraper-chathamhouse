from pandas import DataFrame


def keep_columns(df, columns_to_keep):
    # Drop unwanted columns.  Note that I confirmed that the two date columns are always equal.
    df = df.loc[:,columns_to_keep]
    return df


def vlookup(df, column, condition):
    return df[column][condition].iloc[0]


def keyvaluelookup(df, key, keycolumn='Key', valuecolumn='Value', fn=None):
    val = vlookup(df, valuecolumn, df[keycolumn] == key)
    if fn:
        return fn(val)
    return val


def hxlated_dataframe(headers, hxlmapping):
    hxlheaders = {header: hxlmapping[header] for header in headers}
    return DataFrame([hxlheaders], columns=headers)


def select_rename(df, cols, mapping):
    new_df = df[cols].rename(columns=mapping)
    return new_df

