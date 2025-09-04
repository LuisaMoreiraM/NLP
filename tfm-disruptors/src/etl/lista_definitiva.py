import os
import pandas as pd
import numpy as np
# from utils_etl import data_dirs   # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs

def lista_disruptores_definitiva(data_processed=None):
    """ Une las tablas de disruptores_clean con disruptores_etiqueta.
    Filtra los disruptores_etiqueta para quedarnos con los que no tienen nombre aun.
    Exporta archivo disruptores_sin_etiqueta para búsqueda manual.
    Importamos archivo relleno y unimos con disruptores_etiqueta para obtener la lista definitiva.
    """

    # Rutas absolutas seguras para importar y exportar datos
    raw_dir, processed_dir = data_dirs(__file__, None, data_processed)

    # leemos datasets 
    disruptores_clean = pd.read_parquet(os.path.join(processed_dir,"disruptores_clean.parquet"))
    disruptores_etiqueta = pd.read_parquet(os.path.join(processed_dir,"disruptores_etiqueta.parquet"))

    # Hacemos merge entre disrupores_clean y disrupores_etiqueta
    disruptores_provisional = disruptores_clean.merge(disruptores_etiqueta[[ 'CAS Number', 'EC Number','Name_Chemical_Cosing', 'nombre_etiqueta',  'anexo_cosing']], on=['CAS Number', 'EC Number'], how='left' )

    # Eliminamos duplicados
    disruptores_provisional = disruptores_provisional.drop_duplicates(subset=['Name_Edlist_Echa'], keep='first')

    # Filtramos los disruptores sin etiqueta
    disruptores_sin_etiqueta = disruptores_provisional[(disruptores_provisional['nombre_etiqueta'].isnull())]

    # Eliminamos variables que ya no tienen registros.
    disruptores_sin_etiqueta = disruptores_sin_etiqueta[['CAS Number', 'EC Number', 'Name_Edlist_Echa', 'fuente_original', 'Appears on lists','Health effects' ]]

    #**EXPORTAMOS PARA SU TRATAMIENTO MANUAL**
    disruptores_sin_etiqueta.to_excel(os.path.join(processed_dir,"disruptores_sin_etiqueta.xlsx"), index=False)

    #**IMPORTAMOS LOS ARCHIVOS RELLENOS MANUALMENTE**
    inci_manual = pd.read_excel(os.path.join(processed_dir,"notebooks/consultas_manuales/disruptores_sin_etiqueta_manual.xlsx"))
    inci = pd.read_excel(os.path.join(processed_dir,"notebooks/consultas_manuales/disruptores_etiqueta_manual.xlsx"))

    # Filtramos por nombre manual
    inci_manual = inci_manual[inci_manual['nombre_etiqueta'].notnull()]
    # Quitamos los repetidos
    inci_manual = inci_manual[inci_manual['repetido'].isnull()]

    # Filtramos por productos cosméticos
    inci_manual= inci_manual[inci_manual['productos_cosmeticos']==1]

    # Quitamos la variable 'repetido' porque ya no tiene sentido
    inci_manual = inci_manual.drop(columns=['repetido'], errors='ignore')

    # Convertimos las columnas a tipo entero
    cols = ["Health_effects","productos_cosmeticos", "imagen_cantidad"]
    inci_manual[cols] = inci_manual[cols].fillna(0).astype(int)
   
    # Convertimos las columnas a tipo entero
    inci["Health_effects"] = inci["Health_effects"].fillna(0).astype(int)
    inci["Health_effects"] = (inci["Health_effects"] == 1).astype(int)

    # Concatenamos los DataFrames
    disruptores_final = pd.concat([inci, inci_manual], ignore_index=True)

    # Pasamos variables a tipo string
    cols = ['CAS Number', 'EC Number']
    disruptores_final[cols] = (
        disruptores_final[cols]
        .apply(lambda col: col.astype('string').str.strip())
    )

    # las variables EC Number y Anexo_cosIng tienen valores nulos que vamos a rellenar
    disruptores_final["EC Number"] = disruptores_final["EC Number"].fillna("")
    disruptores_final["Anexo_cosIng"] = disruptores_final["Anexo_cosIng"].fillna("sin info")

    # Filtramos filas con varios nombres que no se dividió a mano, porque no había foto
    disruptores_final[disruptores_final['nombre_etiqueta'].str.contains(r'[/;]', regex= True,na=False)]

    # Separamos por / y descomponemos
    disruptores_final['nombre_etiqueta']= disruptores_final['nombre_etiqueta'].str.split(r'[/]')
    disruptores_final = disruptores_final.explode('nombre_etiqueta')

    # Diccionario de mapeo
    map_fuente = {
        "Edlist_1": "EDlist_1 (confirmado UE)",
        "Edlist_2": "EDlist_2 (evaluando)",
        "Edlist_3": "EDlist_3 (confirmado país miembro, pero no UE)",
        "ECHA - ED_pendiente": "ECHA (evaluando o pendiente)",
        "ECHA - ED_Env": "ECHA (efectos ambiente)",
        "ECHA - ED_Humans&Env": "ECHA (efectos salud y ambiente)"
    }

    # Aplicamos el mapeo en la columna fuente_original
    disruptores_final["Fuente_original"] = disruptores_final["Fuente_original"].replace(map_fuente)

    # Diccionario de mapeo
    map_cosing = {
        "Anexo_2": "Anexo II (Ingrediente Prohibido)",
        "Anexo_3": "Anexo III (Ingrediente Restringido)",
        "Anexo_4": "Anexo IV (Colorante Permitido)",
        "Anexo_5": "Anexo V (Conservante Permitido)",
        "Anexo_6": "Anexo VI (Filtro UV Permitido)",
        "sin info": "Sin información"
    }
    # Aplicamos el mapeo en la columna anexo_cosing
    disruptores_final["Anexo_cosIng"] = disruptores_final["Anexo_cosIng"].replace(map_cosing)


    # RESUMEN
    n_dis_inicial=disruptores_provisional.shape[0]
    n_alias = disruptores_final["nombre_etiqueta"].nunique()
    n_cas = disruptores_final["CAS Number"].nunique()
    n_ec = disruptores_final["EC Number"].nunique()
    n_name_edlist = disruptores_final["Name_Edlist_Echa"].nunique()
    n_texto = disruptores_final["texto"].nunique()

    print("RESUMEN:\n")
    print(f"Número de Disruptores iniciales antes de incluir nombre de etiqueta: {n_dis_inicial}\n")
    print(f"Alias totales conseguidos (filas de nombre_etiqueta): {n_alias}\n")
    print(f"CAS totales (registros CAS Number único): {n_cas}\n")
    print(f"EC totales (registros EC Number único): {n_ec}\n")
    print(f"Disruptores con nombre INCI (registros Name_Edlist_Echa único): {n_name_edlist}\n")
    print(f"Textos totales (registros texto único): {n_texto}\n")


    # Reiniciamos índice
    disruptores_final.reset_index(drop=True, inplace=True)

    # Exportamos 
    disruptores_final.to_excel(os.path.join(processed_dir,"disruptores_final.xlsx"), index=False)
    disruptores_final.to_parquet(os.path.join(processed_dir,"disruptores_final.parquet"), index=False)

    return disruptores_final

if __name__ == "__main__":
    df = lista_disruptores_definitiva ()
    print(df.info())
