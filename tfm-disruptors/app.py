from src.etl.edlist_etl import procesar_edlist
from src.etl.echa_etl import procesar_echa
from src.etl.cosing_etl import procesar_cosing
from src.etl.pesticidas_etl import procesar_pesticidas
from src.etl.merge_edlist_echa_pesticidas_cosing import merge_edlist_echa_pesticidas
from src.etl.lista_definitiva import lista_disruptores_definitiva

from src.ocr.ocr_process import procesar_ocr
from src.nlp.nlp_functions import normalize
from src.nlp.nlp_entityruler import cargar_entity_ruler, analizar_texto


if __name__ == "__main__":
    ### proceso ETL - Ejecutar SOLO 1 vez para generación de archivos ###
    # procesar_edlist()
    # procesar_echa()
    # procesar_cosing()
    # procesar_pesticidas()
    # merge_edlist_echa_pesticidas()
    # lista_disruptores_definitiva()

    # Carga de modelo NLP y Proceso OCR
    nlp_model = cargar_entity_ruler()
    texto_ocr = procesar_ocr("etiqueta_153.jpg") # probar con: "etiqueta_001.jpg" a "etiqueta_170.jpg"

    # Analizamos texto
    entidades = analizar_texto(texto_ocr, nlp_model)

    # Mostramos el texto normalizado
    texto_normalizado = normalize(texto_ocr)
    print("texto_normalizado:\n", texto_normalizado)

    print("Entidades encontradas:")
    for entity, ent_id, label in entidades:
        print(f"{entity} - {ent_id} - {label}")


