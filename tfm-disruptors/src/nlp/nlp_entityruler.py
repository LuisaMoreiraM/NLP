import os
import spacy
import pandas as pd
from spacy.pipeline import EntityRuler
import re

from src.nlp.nlp_functions import normalize, build_patterns_from_df


def cargar_entity_ruler():
    """
    Función que crea un modelo de NER para detectar disruptores hormonales en productos cosméticos.
    Utiliza un EntityRuler de spaCy para añadir patrones de entidades basados en una lista de ingredientes comunes.
    """
    # Base del proyecto: dos niveles arriba desde este archivo
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_dir = os.path.join(base_dir, "data", "processed")

    # Leemos los datos de la lista de disruptores_final
    disruptores_final = pd.read_parquet(os.path.join(processed_dir, "disruptores_final.parquet"))

    # Cargamos modelo 
    nlp = spacy.load("en_core_web_md", disable = ["ner"])  # Deshabilitamos el componente NER predefinido

    # Añadimos el EntityRuler al pipeline. Sobreescribo las entidades existentes
    # tambien añado validación para detectar patrones mal formados
    ruler = nlp.add_pipe("entity_ruler", config={"overwrite_ents": True, "validate": True}, before="ner") 
    
    # Generamos la lista de patrones
    patrones = build_patterns_from_df(disruptores_final)
    print(f"Patrones generados: {len(patrones)}")
    ruler.add_patterns(patrones)

    # Guardamos patrones
    ruler.to_disk("./entity_ruler_patterns.jsonl")

    return nlp  


def analizar_texto(texto: str, nlp_model):
    """
    Devuelve solo las entidades detectadas como:
      [(span_text, ent_id, label), ...]
    """
    norm_out = normalize(texto)
    texto_proc = " ".join(map(str, norm_out)) if isinstance(norm_out, (list, tuple)) else str(norm_out)
    doc = nlp_model(texto_proc)
    return [(ent.text, ent.ent_id_, ent.label_) for ent in doc.ents]

