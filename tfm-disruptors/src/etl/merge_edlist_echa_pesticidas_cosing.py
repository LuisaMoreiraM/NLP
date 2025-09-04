import os
import pandas as pd
import numpy as np
# from utils_etl import data_dirs   # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs


def nombre_mas_informativo_generico(df, col1, col2, nueva_col):
    """Crea una nueva columna en el DataFrame que contiene el valor más informativo entre dos columnas dadas."""
    def elegir_mas_informativo(row):
        val1 = row[col1]
        val2 = row[col2]

        if pd.isna(val1):
            return val2
        if pd.isna(val2):
            return val1
        if val1 == val2:
            return val1
        return val1 if len(str(val1)) > len(str(val2)) else val2

    df[nueva_col] = df.apply(elegir_mas_informativo, axis=1)
    return df

def merge_edlist_echa_pesticidas(data_processed=None):
    """Une las tablas de edlist, echa y pesticidas.
    Elimina los pesticidas unidos.
    Une con la tabla cosing que contiene los nombres de etiqueta.
    Devuelve un DataFrame con los disruptores con etiqueta de cosing"""

    # Rutas absolutas seguras para importar y exportar datos
    raw_dir, processed_dir = data_dirs(__file__, None, data_processed)

  
    # leemos los datos de la lista de edlist_clean,  echa_clean, cosinng_clean
    edlist_clean= pd.read_parquet(os.path.join(processed_dir,"edlist_clean.parquet"))
    echa_clean= pd.read_parquet(os.path.join(processed_dir,"echa_clean.parquet"))
    cosing_clean= pd.read_parquet(os.path.join(processed_dir,"cosing_clean.parquet"))
    pesticidas= pd.read_parquet(os.path.join(processed_dir,"pesticidas.parquet"))

    # Reemplazamos None por NaN
    edlist_clean = edlist_clean.replace({None: np.nan})

    # PRIMER MERGE. Unión que se llevará a cabo es un full outer join, es decir queremos todo de ambas tablas, porque así ganamos registros 
    edlist_echa = edlist_clean.merge( echa_clean, on=['CAS Number', 'EC Number'],  how='outer', suffixes=('', '_ECHA'))

    # Combinamos para que si "fuente_original" está vacío, se rellene con "fuente_original_ECHA"
    edlist_echa['fuente_original'] = edlist_echa['fuente_original'].combine_first(edlist_echa['fuente_original_ECHA'])

    # Eliminamos la columna sobrante
    edlist_echa.drop(columns=['fuente_original_ECHA'], inplace=True)

    # Seleccionamos el nombre más informativo
    nombre_mas_informativo_generico(edlist_echa, 'Name and abbreviation', 'Substance name', 'Name_Edlist_Echa')

    # Elegimos las variables que queremos conservar en el dataframe final
    edlist_echa =  edlist_echa[['Name_Edlist_Echa','fuente_original','Appears on lists', 'Health effects','CAS Number', 'EC Number'  ]]

    # SEGUNDO MERGE. La unión se realizará con un left join por 'CAS Number'
    edlist_echa_pesticidas = edlist_echa.merge(pesticidas, on=['CAS Number'], how='left' )
 
    # Filtramos los disruptores sin pesticidas
    disruptores_clean = edlist_echa_pesticidas[edlist_echa_pesticidas['Name_Pesticida'].isna()]
    
    # Elegimos las variables que queremos conservar en el dataframe final
    disruptores_clean = disruptores_clean[['Name_Edlist_Echa', 'fuente_original', 'Appears on lists', 'Health effects', 'CAS Number', 'EC Number']]
    
    # EXPORTACIÓN del Segundo Merge para otras fuentes de búsquedas del nombre INCI.
    disruptores_clean.to_parquet(os.path.join(processed_dir,"disruptores_clean.parquet"), index=False)

    # Reemplazamos None por NaN
    cosing_clean = cosing_clean.replace({None: np.nan})

    # TERCER MERGE - Unión final. La unión se realizará de la siguiente manera:  y depués un inner join por 'EC Number', luego se uniran ambas tablas.**
    # Unión por CAS Number
    union_inner_cas= disruptores_clean.merge(cosing_clean, on=['CAS Number'],  how='inner', suffixes=('', '_cosing'))

    # Reemplazamos None por NaN
    union_inner_cas = union_inner_cas.replace({None: np.nan})

    # Procedemos a eliminar las variables donde CAS Number es nulo
    union_inner_cas= union_inner_cas[~(union_inner_cas['CAS Number'].isna())]

    # Unión por EC Number
    union_inner_ec= edlist_echa.merge( cosing_clean, on=['EC Number'],  how='inner', suffixes=('', '_cosing'))
 
    union_inner_ec= union_inner_ec[~(union_inner_ec['EC Number'].isna())]

    # Unión de ambas tablas - Concatenamos
    union_completa = pd.concat([union_inner_cas,union_inner_ec], ignore_index=True)

    # Unificación de nombres
    nombre_mas_informativo_generico(union_completa, 'Chemical name', 'Chemical/IUPAC Name', 'Name_Chemical_Cosing')

    # Combinamos las vatiables de nombre 
    union_completa["nombre_etiqueta"] = union_completa["Name of Common Ingredients Glossary"].combine_first(
        union_completa["Identified INGREDIENTS or substances e.g."]
    )

    # Elegimos las variables que hacen referencia a nombres y las que nos van a servir más adelante para otros fines.
    union_completa =  union_completa[['Name_Edlist_Echa', 'fuente_original', 'Appears on lists', 'Health effects','CAS Number', 'EC Number', 
    'Name_Chemical_Cosing', 'nombre_etiqueta', 'Product Type, body parts','anexo_cosing' ]]

    # Separamos nombre_etiqueta por /
    union_completa['nombre_etiqueta'] = union_completa['nombre_etiqueta'].str.split(r'[/]')

    # Descomponemos fila por cada nombre_etiqueta
    union_completa = union_completa.explode('nombre_etiqueta')

    # pasamos a minusculas la variable nombre_etiqueta
    union_completa['nombre_etiqueta'] = union_completa['nombre_etiqueta'].str.lower()

    # Eliminamos duplicados por nombre_etiqueta
    union_completa = union_completa.drop_duplicates(subset='nombre_etiqueta', keep='first')

    # **LISTA DEFINITIVA DE ESTE ETL**
    disruptores_etiqueta = union_completa[(union_completa['nombre_etiqueta'].notnull())]

    # Dejamos las variables que necesitamos
    disruptores_etiqueta = disruptores_etiqueta[['Name_Edlist_Echa', 'Name_Chemical_Cosing','fuente_original','anexo_cosing','Appears on lists', 'Health effects','CAS Number','EC Number','nombre_etiqueta']]

    # Exportamos 
    disruptores_etiqueta.to_excel(os.path.join(processed_dir,"disruptores_etiqueta.xlsx"), index=False)
    disruptores_etiqueta.to_parquet(os.path.join(processed_dir,"disruptores_etiqueta.parquet"), index=False)

    return disruptores_etiqueta

if __name__ == "__main__":
    df = merge_edlist_echa_pesticidas()
    print(df.info())
    
