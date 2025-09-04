import os
import pandas as pd
import numpy as np
# from utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null    # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs, drop_duplicates_sort, clean_cas_ec, not_both_null

def procesar_cosing(data_raw=None, data_processed=None):
    """Procesa los anexos de la base de datos COSING y devuelve un DataFrame limpio."""

    # BASE_DIR apunta al directorio raíz del proyecto (dos niveles arriba desde este archivo)
    raw_dir, processed_dir = data_dirs(__file__, data_raw, data_processed)

    # Definimos los headers.
    header_2 = ['Reference Number', 'Chemical name','CAS Number', 'EC Number', 'Regulation', 'Other Directives/Regulations', 
                'SCCS opinions', 'Chemical/IUPAC Name', 'Identified INGREDIENTS or substances e.g.', 'CMR', 'Update date']


    header = ['Reference Number', 'Chemical name', 'Name of Common Ingredients Glossary',
              'CAS Number', 'EC Number', 'Product Type, body parts', 'Maximum concentration in ready for use preparation',
              'Other Restrictions', 'Wording of conditions of use and warnings', 'Regulation',
              'Other Directives/Regulations', 'SCCS opinions', 'Chemical/IUPAC Name',
              'Identified INGREDIENTS or substances e.g.', 'CMR', 'Update date']

    header_4 = ['Reference Number', 'Chemical name', 'Name of Common Ingredients Glossary',
                'CAS Number', 'EC Number', 'Color', 'Product Type, body parts', 'Maximum concentration in ready for use preparation',
                'Other Restrictions', 'Wording of conditions of use and warnings', 'Regulation',
                'Other Directives/Regulations', 'SCCS opinions', 'Chemical/IUPAC Name',
                'Identified INGREDIENTS or substances e.g.', 'CMR', 'Update date']

    # Función para cargar los anexos
    def cargar_anexo(ruta, columnas):
        df = pd.read_excel(ruta, skiprows=6, header=[0,1])
        df.columns = columnas  # renombra las columnas con las listas creadas
        return df

    # Cargamos los anexos
    anexo_2 = cargar_anexo(os.path.join(raw_dir, "COSING_Annex_II_v2.xlsx"), header_2)
    anexo_3 = cargar_anexo(os.path.join(raw_dir, "COSING_Annex_III_v2.xlsx"), header)
    anexo_4 = cargar_anexo(os.path.join(raw_dir, "COSING_Annex_IV_v2.xlsx"), header_4)
    anexo_5 = cargar_anexo(os.path.join(raw_dir, "COSING_Annex_V_v2.xlsx"), header)
    anexo_6 = cargar_anexo(os.path.join(raw_dir, "COSING_Annex_VI_v2.xlsx"), header)


    # Añadimos columna de anexo
    anexo_2['anexo_cosing'] = 'Anexo_2'
    anexo_3['anexo_cosing'] = 'Anexo_3'
    anexo_4['anexo_cosing'] = 'Anexo_4'
    anexo_5['anexo_cosing'] = 'Anexo_5'
    anexo_6['anexo_cosing'] = 'Anexo_6'

    # Concatenamos
    cosing = pd.concat([anexo_2, anexo_3, anexo_4, anexo_5, anexo_6], ignore_index=True)  

    # Eliminamos registros 'Moved or deleted'
    cosing = cosing[~cosing['Chemical name'].str.contains('Moved or deleted', na=False)].copy()

    # Eliminamos los duplicados por 'Chemical name' y manteniendo el primero
    cosing_final = drop_duplicates_sort(cosing, subset='Chemical name')

    # Tratamiento de CAS y EC Number múltiples
    cosing_final['CAS Number_2'] = cosing_final['CAS Number'].str.split(r'[,;/]')
    cosing_final['EC Number_2'] = cosing_final['EC Number'].str.split(r'[,;/]')
    cosing_final = cosing_final.explode('CAS Number_2')
    cosing_final = cosing_final.explode('EC Number_2')

    # Eliminamos las columnas antiguas y renombramos
    cosing_final.drop(columns='CAS Number', inplace=True)
    cosing_final.drop(columns='EC Number', inplace=True)
    cosing_final = cosing_final.rename(columns={'CAS Number_2': 'CAS Number', 'EC Number_2': 'EC Number'})
    
    # Limpiamos valores extraños
    cosing_final = clean_cas_ec(cosing_final, 'CAS Number', 'EC Number')

    # Nos quedamos con las columnas mínimas necesarias
    cosing_final= cosing_final[['Chemical name', 'Chemical/IUPAC Name', 'Identified INGREDIENTS or substances e.g.',
    'CAS Number', 'EC Number','Name of Common Ingredients Glossary', 'Product Type, body parts', 'anexo_cosing' ]]

    # Filtramos los registros que cumplen la condición que "CAS Number" y "EC Number" sean NO nulos. 
    cosing_clean= not_both_null(cosing_final, 'CAS Number', 'EC Number')

    # Eliminamos los duplicados por las variables  de la consutla
    cosing_clean = drop_duplicates_sort(
        cosing_clean,
        subset=['Chemical name','EC Number','CAS Number','Identified INGREDIENTS or substances e.g.'],
        sort_by=['Chemical name','Identified INGREDIENTS or substances e.g.','CAS Number','EC Number',
                 'Name of Common Ingredients Glossary','Chemical/IUPAC Name','Product Type, body parts']
    )

    # Reiniciamos índice
    cosing_clean.reset_index(drop=True, inplace=True)

    # Exportamos base de datos limpia
    cosing_clean.to_parquet(os.path.join(processed_dir, "cosing_clean.parquet"), index=False)
    #cosing_clean.to_excel(os.path.join(processed_dir, "cosing_clean.xlsx"), index=False)
    
    return cosing_clean


if __name__ == "__main__":
    df = procesar_cosing()
    print(df.info())