import os
import pandas as pd
import numpy as np
# from utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null  # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null

def procesar_echa(data_raw=None, data_processed=None):
    """Procesa la base de datos ECHA y devuelve un DataFrame limpio."""
    
    # Rutas absolutas seguras
    raw_dir, processed_dir = data_dirs(__file__, data_raw, data_processed)

    # Leemos los datos de la lista de ECHA (con skiprows=3)
    echa = pd.read_excel(os.path.join(raw_dir, "endocrine-disruptor-assessment-export.xlsx"), skiprows=3)

    # Eliminamos columna que no contiene información
    echa.drop(columns='DISLIST_ED_description', inplace=True)

    # Renombramos variables para que coincidan con Edlist
    echa.rename(columns={'CAS no': 'CAS Number', 'EC / List no': 'EC Number'}, inplace=True)

    # Eliminamos los duplicados por las variables más relantes y ordenando por las otras que pierdan información
    echa = drop_duplicates_sort(echa, subset=['Substance name','CAS Number','EC Number'], sort_by='Authority')

    # Sustituimos valores '-', ' ' y '' por NaN
    echa = clean_cas_ec(echa, 'CAS Number', 'EC Number')

    # Filtramos los registros que cumplen la condición de que "CAS Number" y "EC Number" NO sean ambos nulos
    echa = not_both_null(echa, 'CAS Number', 'EC Number')

    # # Quitamos los registros que continen el string 'not ED#inconclusive' y 'not ED']
    echa_clean= echa[~echa['Outcome'].isin(['not ED', 'not ED#inconclusive'])]

    # Creamos un diccionario de mapeo
    mapeo = {
        "ED ENV": "ECHA - ED_Env",
        "ED ENV#ED HH": "ECHA - ED_Humans&Env",
        "ED HH": "ECHA - ED_Humans",
        "ED ENV 1": "ECHA - ED_Env",
        "Under development (SEV)" : "ECHA - ED_pendiente",
        "Under development (BPR)" : "ECHA - ED_pendiente",
        "inconclusive" : "ECHA - ED_pendiente",
        "postponed" : "ECHA - ED_pendiente",
        "Under development (other)" : "ECHA - ED_pendiente",
        "ED ENV#inconclusive" : "ECHA - ED_pendiente"
    }

    # Creamos la nueva columna
    echa_clean['fuente_original'] = echa_clean['Outcome'].map(mapeo)

    # Reiniciamos índice
    echa_clean.reset_index(drop=True, inplace=True)

    # Exportamos base de datos limpia
    echa_clean.to_parquet(os.path.join(processed_dir, "echa_clean.parquet"), index=False)

    return echa_clean

if __name__ == "__main__":
    df = procesar_echa()
    print(df.info())
    