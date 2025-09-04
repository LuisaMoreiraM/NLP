import os
import pandas as pd
import numpy as np
# from utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null  # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null

def procesar_edlist(data_raw=None, data_processed=None):
    """Procesa las bases de datos EDlist y devuelve un DataFrame limpio."""
    
    # Rutas absolutas seguras para importar y exportar datos
    raw_dir, processed_dir = data_dirs(__file__, data_raw, data_processed)

    # Leemos los datasets de EDlist
    edlist_1 = pd.read_excel(os.path.join(raw_dir, "list1.xlsx"))
    edlist_2 = pd.read_excel(os.path.join(raw_dir, "list2.xlsx"))
    edlist_3 = pd.read_excel(os.path.join(raw_dir, "list3.xlsx"))

    # Renombramos categorías utilizando mapeo 
    mapeo_categorias = {
        'List I, List II': 'List I',
        'List I, List II, List III': 'List I',
        'List II, List III': 'List II',
        'List I, List III': 'List I'
    }

    edlist_1['Appears on lists'] = edlist_1['Appears on lists'].replace(mapeo_categorias)
    edlist_2['Appears on lists'] = edlist_2['Appears on lists'].replace(mapeo_categorias)
    edlist_3['Also appears on lists'] = edlist_3['Also appears on lists'].replace(mapeo_categorias)

    # renombramos columnas
    edlist_1 = edlist_1.rename(columns={'Status year': 'Year'})
    edlist_3 = edlist_3.rename(columns={'Also appears on lists': 'Appears on lists'})

    # añadimos columna de la fuente original
    edlist_1['fuente_original'] = 'edlist_1'
    edlist_2['fuente_original'] = 'edlist_2'
    edlist_3['fuente_original'] = 'edlist_3'

    # Ordenamos las 3 listas por ID (for name)
    for df in (edlist_1, edlist_2, edlist_3):
        df.sort_values('ID (for name)', inplace=True)

    # Concatenamos tablas
    edlist_completa = pd.concat([edlist_1, edlist_2, edlist_3], ignore_index=True)

    # Tratamiento de variable año
    año = { '2022, 2023': '2023', '2020, 2021': '2021'}
    edlist_completa['Year'] = edlist_completa['Year'].replace(año)
    edlist_completa['Year'] = pd.to_numeric(edlist_completa['Year'])

    # Eliminamos los duplicados por las variables más relantes y ordenando por las otras que pierdan información
    edlist_final = drop_duplicates_sort(
        edlist_completa,
        subset=['ID (for name)','Name and abbreviation','CAS no.','EC / List no.'],
        sort_by=['Year','Regulatory Field','Environmental effects','Health effects'],
        ascending=[True, False, False, False]
    ).sort_values('ID (for name)').copy()

    # Mapear 'Yes' a 1, y el resto a 0 (incluye NaN)
    edlist_final['Health effects'] = (edlist_final['Health effects'] == 'Yes').astype(int)
    edlist_final['Environmental effects'] = (edlist_final['Environmental effects'] == 'Yes').astype(int)

    # Dividimos variables "CAS no." y "EC / List no."
    edlist_final['cas_list'] = edlist_final['CAS no.'].str.split(r'[,;/]')
    edlist_final['ec_list'] = edlist_final['EC / List no.'].str.split(r'[,;/]')

    # Descomponemos fila por cada CAS y EC individual
    edlist_final = edlist_final.explode('cas_list')
    edlist_final = edlist_final.explode('ec_list')

    # Eliminamos 'CAS no.' y 'EC / List no.'
    edlist_final.drop(columns=['CAS no.','EC / List no.'], inplace=True, errors='ignore')

    # Renombramos variables con el nombre antiguo
    edlist_final = edlist_final.rename(columns={'cas_list': 'CAS Number', 'ec_list': 'EC Number'})

    # Reemplazamos los valores '-' y ' ' por NaN
    edlist_final = clean_cas_ec(edlist_final, 'CAS Number', 'EC Number')

    # Filtramos los registros que cumplen la condición que "CAS no." y "EC no." sean NO nulos.
    edlist_final = not_both_null(edlist_final, 'CAS Number', 'EC Number').reset_index(drop=True)


    # Reiniciamos índice
    edlist_final.reset_index(drop=True, inplace=True)
    edlist_clean = edlist_final

    # Exportamos base de datos limpia
    edlist_clean.to_parquet(os.path.join(processed_dir, "edlist_clean.parquet"), index=False)

    return edlist_clean

if __name__ == "__main__":
    df = procesar_edlist()
    print( df.info())