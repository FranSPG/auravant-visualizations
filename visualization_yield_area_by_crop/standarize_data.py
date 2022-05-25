import pandas as pd

def stand_unidad_medida(df_query) -> pd.DataFrame:
    """
    This function standardizes the unidad_medida column.

    :param df_query: A Dataframe with the unidad_medida column in it.
    :return: A Dataframe with the unidad_medida column standardized.
    """
    df_query.loc[df_query['unidad_medida'].str.lower() == 'kg', 'unidad_medida'] = 'kg/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'lts', 'unidad_medida'] = 'lt/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'lts/ha', 'unidad_medida'] = 'lt/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'kgs', 'unidad_medida'] = 'kg/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'g/ha', 'dosis'] = df_query.loc[df_query[
                                                                                                 'unidad_medida'].str.lower() == 'g/ha', 'dosis'] / 1000
    df_query.loc[df_query['unidad_medida'].str.lower() == 'cm3/ha', 'dosis'] = df_query.loc[df_query[
                                                                                                   'unidad_medida'].str.lower() == 'cm3/ha', 'dosis'] / 1000
    df_query.loc[df_query['unidad_medida'].str.lower() == 'g/ha', 'unidad_medida'] = 'kg/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'cm3/ha', 'unidad_medida'] = 'lt/ha'
    df_query.loc[df_query['unidad_medida'].str.lower() == 'tn', 'dosis'] = df_query.loc[df_query[
                                                                                               'unidad_medida'].str.lower() == 'tn', 'dosis'] * 1000
    df_query.loc[df_query['unidad_medida'].str.lower() == 'tn', 'unidad_medida'] = 'kg/ha'

    return df_query