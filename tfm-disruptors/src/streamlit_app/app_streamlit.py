import os
from pathlib import Path
import tempfile
import pandas as pd
import spacy
import streamlit as st
from src.nlp.nlp_entityruler import cargar_entity_ruler,  analizar_texto
from src.nlp.nlp_functions import normalize, make_ent_id
from src.ocr.ocr_process import procesar_ocr
 

# PARA EJECUTAR CODIGO PONER EN LA TERMINAL->   python -m streamlit run src/streamlit_app/app_streamlit.py

st.set_page_config(page_title="Detector de Disruptores", layout="centered")
st.title("🧪 Detector de Disruptores Endocrinos en Cosméticos")

uploaded_file = st.file_uploader("📸 Sube una foto de la etiqueta del producto", type=["jpg", "jpeg", "png"])

if uploaded_file is not None:
    # Mostramos imagen subida
    st.image(uploaded_file, caption="📷 Imagen subida", use_column_width=True)

    # Guardamos temporalmente
    temp_path = "temp_img.jpg"
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Ejecutamos flujo OCR - NLP
    with st.spinner("Analizando imagen..."):
        nlp_model = spacy.load("en_core_web_md", disable=["ner"])
        ruler = nlp_model.add_pipe("entity_ruler", config={"overwrite_ents": True, "validate": True}, before="ner" )
        ruta_patrones = "./entity_ruler_patterns.jsonl"
        ruler.from_disk(ruta_patrones) # Cargar patrones desde archivo JSONL
        # nlp_model = cargar_entity_ruler()  # Alternativa si se quiere cargar desde función descomentar y comentar las 4 líneas anteriores.
        texto_ocr = procesar_ocr(temp_path)
        entidades = analizar_texto(texto_ocr, nlp_model)
        # Normalización SOLO para mostrarla en pantalla
        texto_normalizado = normalize(texto_ocr)

    # Mostramos textos
    st.subheader("📝 Texto extraído:")
    st.write(texto_ocr)
    st.subheader(" ✅ Texto normalizado:")
    st.write(texto_normalizado)

    # Mostramos entidades reconocidas
    st.subheader("⚠️ Disruptores detectados confirmados o en evaluación:")
    if entidades:
        for nombre, ids, etiqueta in entidades:
            st.write(f"🔹 **{nombre}** - {ids} - {etiqueta}")

        # Ficha de disruptores detectados (filtrando POR NOMBRE y sin CAS/EC)
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        processed_dir = os.path.join(base_dir, "data", "processed")
        df = pd.read_parquet(os.path.join(processed_dir, "disruptores_final.parquet"))


        # Elegimos columnas a usar
        df = df[["EC Number", "CAS Number", "Fuente_original", "Anexo_cosIng", "Health_effects", "uso"]].copy()

        # Mismo ID que en los patrones
        df["detected_ids"] = df.apply(lambda r: make_ent_id(r["CAS Number"], r["EC Number"]), axis=1)

        # Obtenemos IDs y NOMBRES detectados desde el analizador
        ids_detectados = {ent[1] for ent in (entidades or []) if isinstance(ent, (list, tuple)) and len(ent) >= 2}
        id_a_nombre = {ent[1]: ent[0] for ent in (entidades or []) if isinstance(ent, (list, tuple)) and len(ent) >= 2}

        # Filtramos por ID y añadimos el nombre detectado
        df_match = df[df["detected_ids"].isin(ids_detectados)].copy()
        df_match["nombre_etiqueta"] = df_match["detected_ids"].map(id_a_nombre)

        # Elimina duplicados por nombre para que solo muestre los nombres detectados no otros con el mismo CAS/EC Number
        df_view = df_match.drop_duplicates(subset=["nombre_etiqueta"])
        df_view = df_view[["nombre_etiqueta", "Fuente_original", "Anexo_cosIng", "Health_effects", "uso"]]

        st.subheader("ℹ️ Información de los disruptores detectados.")
        st.text("RECUERDE: La Endocrine Society advierte que los EDC pueden tener efectos relevantes incluso en dosis muy bajas.")
        st.table(df_view)
        st.text("Amplia información buscando por CAS Number o por EC Number en las fuentes oficiales:")
        st.text("https://echa.europa.eu/es/ed-assessment")
        st.text("https://edlists.org/")

    else:
        st.info("No se detectaron disruptores en el texto.")

