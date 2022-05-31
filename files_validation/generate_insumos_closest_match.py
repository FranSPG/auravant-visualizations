# import sqlalchemy as sqlalchemy
import pandas as pd
# import numpy as np
from difflib import get_close_matches
from tqdm import tqdm
import datetime
# from sklearn.cluster import dbscan
#
# import ast
import unidecode
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

from db import get_db
from utils.standarize_data import stand_unidad_medida

tqdm.pandas()
engine = get_db()


def get_only_alpha(input):
    """
    Converts an input to string, lower, alphanumerical, and unidecode.
    :param input:
    :return:
    """
    string = str(input)
    final_string = []
    for s in string.lower():
        if s.isalnum():
            final_string.append(s)

    final_string = "".join(final_string)
    final_string = unidecode.unidecode(final_string)
    return final_string


def get_close_matches_try(x, y, n):
    try:
        return get_close_matches(x, y, n, cutoff=0)[0]
    except Exception:
        return ''


if __name__ == '__main__':
    # Read oficial list from SENASA
    # https://www.argentina.gob.ar/sites/default/files/12_11_21_listado_oficial_de_insumos_po.xls
    df_listado_oficial_supplies = pd.read_excel('12_11_21_listado_oficial_de_insumos_po.xls',
                                                header=1)

    # Query of all insumos in the database.
    query_supplies = "select i.nombre, il.dosis, il.unidad_medida, il.fecha_ini " \
                     "from insumos i, insumos_labores il where i.id = il.id_insumo;"

    # Query for oficial names from SENASA in the database.
    query_supplies_senasa = "select distinct nombre from insumos where id_usuario is null;"

    # Getting all supplies from the database.
    df_supplies = pd.read_sql(query_supplies, engine)

    # Getting all SENASA supplies names in the database.
    df_supplies_senasa_name = pd.read_sql(query_supplies_senasa, engine)

    df_supplies_senasa_name['version'] = 'db_auravant'
    df_listado_oficial_supplies['version'] = 'senasa_12_11_21'

    # Renaming columns to match after. We have two sources of official supplies names.
    df_listado_oficial_supplies.rename(columns={"MARCA COMERCIAL": "nombre_senasa"}, inplace=True)
    df_supplies_senasa_name.rename(columns={"nombre": "nombre_senasa"}, inplace=True)

    # Concatenating the two sources of official supplies names.
    df_supplies_senasa = pd.concat(
        [df_supplies_senasa_name[['nombre_senasa', 'version']],
         df_listado_oficial_supplies[['nombre_senasa', 'version']]])

    # Applying get_only_alpha to the names.
    df_supplies.loc[:, 'nombre_alpha'] = df_supplies['nombre'].apply(lambda x: get_only_alpha(x))
    df_supplies_senasa.loc[:, 'nombre_senasa_alpha'] = df_supplies_senasa['nombre_senasa'].apply(
        lambda x: get_only_alpha(x))

    # Unique supplies from database.
    unique_names = df_supplies_senasa['nombre_senasa_alpha'].unique()

    # Unique supplies from SENASA and sorted
    unique_supplies_names_senasa = sorted(df_supplies_senasa['nombre_senasa_alpha'].unique())

    # Unique supplies names from database.
    unique_supplies_names_database = sorted(df_supplies['nombre_alpha'].unique())

    df_supplies.loc[:, 'nombre_alpha_closest_match'] = df_supplies['nombre_alpha'].parallel_apply(
        lambda x: get_close_matches_try(x, unique_supplies_names_senasa, 1))

    df_supplies_senasa.drop_duplicates('nombre_senasa_alpha', inplace=True)

    # Merging the datasets
    df_merge_supplies_db_with_senasa = df_supplies.merge(df_supplies_senasa,
                                                         left_on='nombre_alpha_closest_match',
                                                         right_on='nombre_senasa_alpha',
                                                         how='left')
    df_merge_supplies_db_with_senasa.rename(columns={'dosis': 'dosis_promedio'}, inplace=True)

    df_result = stand_unidad_medida(df_merge_supplies_db_with_senasa)

    # df_supplies['nombre_alpha_closest_match'].apply(
    #     lambda x: df_supplies_senasa.loc[df_supplies_senasa['nombre_senasa_alpha'] == x[0]])

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_supplies[
        ["nombre", "nombre_alpha", "nombre_alpha_closest_match"]].to_csv(f'nombre_closest_match_{timestamp}.csv')
