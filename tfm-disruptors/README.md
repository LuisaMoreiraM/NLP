# TFM — Detección de Disruptores Endocrinos en Cosméticos (OCR + NLP + Streamlit)

Proyecto de **Trabajo Fin de Máster** cuyo objetivo es identificar **disruptores endocrinos (EDC)** en etiquetas de productos cosméticos mediante un flujo combinado de **ETL, OCR y NLP**. El resultado se presenta en una aplicación web con **Streamlit**.

## 1. Autoría

**Luisa Moreira Mendoza**
TFM en **\[Máster en Data Science y Business Analytics / IMF Business School y UCAV]**
Año: **2025**

## 2. Flujo general (arquitectura)

1. **Raw data**: recopilación de sustancias desde ECHA, EDList, CosIng, Pesticidas y EDCs DataBank; y capturas de etiquetas (PNG/JPG).
2. **ETL**: extracción, transformación, carga y normalización para generar un listado consolidado con **nombre INCI**.
3. **OCR**: lectura de imagen y extracción de texto.
4. **Procesamiento NLP**: creación de patrones con `EntityRuler`, normalización previa y análisis del texto en el modelo NLP.
5. **App (Streamlit)**: interfaz para subir imágenes y visualizar resultados de **detección** de EDC. En la app se cargan patrones ya creados (con opción de regenerarlos).

## 4. Estructura de carpetas

```
tfm-disruptors/
│
├── data/
│   ├── raw/                      # Datos originales (CSV/JSON, imágenes)
│   └── processed/                # Resultados intermedios tras ETL
│
├── notebooks/                    # implementación inicial del código con explicación de todo
│   ├── etl/
│   ├── nlp/                      # creación y evalucación del modelo
│   └── ocr/
│
├── src/                          # Código fuente
│   ├── etl/
│   ├── nlp/
│   ├── ocr/
│   └── streamlit_app/
│
├── app.py                        # Punto de entrada (orquesta ETL/OCR/NLP)
├── entity_ruler_patterns.jsonl   # Patrones exportados para EntityRuler
├── requirements.txt              # Dependencias
└── README.md
```

## 4. Notebooks (IMPORTANTE)
En `notebooks/` están los cuadernos de exploración (`etl/`, `nlp/`, `ocr/`). Aquí se implementó inicialmente todo el código que después se migró y modularizó en `src/`.  
En la carpeta nlp se encuentran los documentos donde se pone a prueba el modelo y el **único** sitio donde se puede realizar esta consulta:  

1. **NLP_entity_ruler:** es donde se crea el modelo y se evalua con texto puro y texto proveniente de lectura de imagenes. Posterioremente se migra solo la parte del modelo al script nlp_entityruler  

2. **NLP_analisis_beauty:** aquí se pone a prueba el modelo importando solo los patrones ya creados en el notebook anterior. La prueba se realiza sobre un dataset heterogeneo de 16.000 registros válidos para poner a prueba los resultados que saca, pero no se dispone de métodos de evaluación.  

Los notebooks están redactados paso a paso (qué se hace y por qué) para garantizar trazabilidad metodológica y facilitar su revisión. 
Para una lectura cómoda, ver los HTML.


## 5. Instalación

```bash
git clone [URL-del-repositorio]
cd tfm-disruptors
pip install -r requirements.txt
```

## 5. Ejecución streamlit

```bash
python -m streamlit run src/streamlit_app/app_streamlit.py
```

## 6. Inicializar Git (opcional)

```bash
git init
git add README.md
git commit -m "Add README"
```


