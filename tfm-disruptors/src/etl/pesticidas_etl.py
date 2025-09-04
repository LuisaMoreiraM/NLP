import os
import requests
import pandas as pd
import ast
# from utils_etl import data_dirs    # descomentar esta línea y comentar la de abajo si se quiere ejecutar solo este archivo
from src.etl.utils_etl import data_dirs

def procesar_pesticidas(data_raw=None, data_processed=None):
    """Procesa la(s) tabla(s) de pesticidas y devuelve un DataFrame limpio."""
    # Rutas del proyecto (igual que en el resto de ETL)
    raw_dir, processed_dir = data_dirs(__file__, data_raw, data_processed)

    # Extrción de datos de la API de la Unión Europea sobre pesticidas
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
        'Referer': 'https://api.datalake.sante.service.ec.europa.eu'
    }

    url = "https://api.datalake.sante.service.ec.europa.eu/sante/pesticides/active_substances?format=json&api-version=v2.0"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        pesticidas = pd.json_normalize(data['value'])
    else:
        print("Error:", response.status_code, response.text[:200])

    # Nos quedamos con las variables que nos interesan
    pesticidas = pesticidas[['substance_name', 'as_cas_number']]

    # Renombramos las columnas
    pesticidas.rename(columns={'substance_name': 'Name_Pesticida', 'as_cas_number': 'CAS Number'}, inplace=True)

    # Exportamos el DataFrame
    pesticidas.to_parquet(os.path.join(processed_dir, "pesticidas.parquet"), index=False)


    return pesticidas


if __name__ == "__main__":
    df = procesar_pesticidas()
    print(df.info())