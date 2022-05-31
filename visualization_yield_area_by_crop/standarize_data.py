import pandas as pd
import ast
import os
from os.path import dirname, abspath
import pandas as pd

abs_dir = dirname(dirname(abspath(__file__)))

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


def stand_ins_name(df_query) -> pd.DataFrame:
    """
    This function standardizes the insumos column. Validating it with the nombres_closests_match_2.csv

    :param df_query: A Dataframe with the insumo_vecino column in it.
    :return: Returns a Dataframe with the insumo_vecino_stand column.
    """
    csv_stand_path = abs_dir + os.sep + 'files_validation' + os.sep + 'nombre_closest_match_2022-05-21 15:35:21.csv'
    df_stand = pd.read_csv(csv_stand_path, index_col=0)

    df_query.loc[:, 'nombre'] = synonymous_replacements(df_query['nombre'])

    for ins in df_query['nombre'].unique():
        try:
            stand_name = df_stand.loc[df_stand['nombre'] == ins, 'nombre_alpha_closest_match'].values[0]
            df_query.loc[df_query['nombre'] == ins, 'insumo_nombre_stand'] = stand_name
        except:
            pass
    df_query.loc[df_query['insumo_nombre_stand'].isna(),
                 'insumo_nombre_stand'] = df_query.loc[df_query['insumo_nombre_stand'].isna(),
                                                       'nombre']
    return df_query


def validate_dosis(df_query) -> pd.DataFrame:
    """
    This function validates that if the insumo is in the dosis_realistic_values_stand.csv file, it validates that
    the values of the dosis are a realistic one.

    :param df_query: A Dataframe with the insumos columns.
    :return: A Dataframe with the validations for dosis and units.
    """
    csv_dosis_path = abs_dir + os.sep + 'files_validation' + os.sep + 'dosis_realistic_values_stand.csv'
    df_dosis = pd.read_csv(csv_dosis_path, index_col=0)
    df_dosis['insumo_vecino'] = df_dosis['nombre_senasa']
    df_dosis_stand = stand_ins_name(df_dosis)

    for idx, row in df_query.iterrows():
        match = df_dosis_stand.loc[df_dosis_stand['insumo_vecino_stand'] == row['insumo_vecino_stand'],
                                   ['min', 'max', 'unit', 'nombre_senasa']]
        if match.empty:
            continue
        else:
            min_dosis, max_dosis, unit, nombre_senasa = match.values[0]

        df_query.loc[idx, 'nombre_senasa_matched'] = nombre_senasa
        if (row['dosis_promedio'] <= max_dosis) and (row['dosis_promedio'] >= min_dosis):
            df_query.loc[idx, 'valid_dosis'] = True
        else:
            df_query.loc[idx, 'valid_dosis'] = False
        if row['unidad_medida'] == unit:
            df_query.loc[idx, 'valid_unit'] = True
        else:
            df_query.loc[idx, 'valid_unit'] = False
    df_query = df_query.loc[df_query['dosis_promedio'] > 0]
    df_query = df_query.fillna('match_not_found')
    return df_query


def get_insumos_list() -> pd.Series:
    """
    This function returns a list with the names of the files dosis_realistic_values_stand.csv

    :return: return a pd Series with the names of insumos in dosis_realistic_values_stand.csv
    """
    csv_dosis_path = abs_dir + os.sep + 'files_validation' + os.sep + 'dosis_realistic_values_stand.csv'
    df_dosis = pd.read_csv(csv_dosis_path, index_col=0)
    return df_dosis['nombre_senasa']


def get_only_alpha(string) -> str:
    """
    This function takes a string and returns only the alphanumeric value of it.

    :param string: a string to process.
    :return: only the alphanumerical value of the string.
    """
    final_string = []
    for s in string.lower():
        if s.isalnum():
            final_string.append(s)
    return "".join(final_string)


def synonymous_replacements(column: pd.Series) -> pd.Series:
    synonymous = {
        "dap": "fosfato diamónico",
        "Fosfato Monoamónico (MAP)": "fosfato monoamónico"
    }

    for key, value in synonymous.items():
        column.loc[column == key] = value

    return column

