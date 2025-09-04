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
4. **Procesamiento NLP**: creación de patrones con `EntityRuler`, normalización previa y análisis del texto en el modelo NLP, que devuelve el "DISRUPTOR" si este es detectado en el texto.
5. **App (Streamlit)**: interfaz para subir imágenes y visualizar resultados de **detección** de EDC. En esta app se cargan patrones ya creados para no crearlos en cada lectura (con opción de regenerarlos).

## 3. Estructura de carpetas

```
tfm-disruptors/
│
├── data/
│   ├── raw/                      # Datos originales (CSV/JSON, imágenes)
│   └── processed/                # Resultados intermedios tras ETL - se deja solo disruptores_final.parquet para ejecución demo
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


## 5. Reproducibilidad
- **Demo rápida (app.py):** usa `data/processed/disruptores_final.parquet` (incluido) y, opcionalmente, `data/raw/etiquetas/` (incluido) o tus propias imágenes.

- **Demo rápida (Streamlit):** usa `entity_ruler_patterns.jsonl` (incluido), `data/processed/disruptores_final.parquet` (incluido) y, opcionalmente, `data/raw/etiquetas/` (incluido) o tus propias imágenes.

- **Reconstrucción completa con ETL incluida**:
  1) Descarga de la repo la carpeta data completa.
  2) En `app.py`, **descomenta** el bloque de ETL para generar todos los arcvhivos de `data/processed/` (incluido `disruptores_final.parquet`).
  4) Ejecuta `python app.py` o lanza la UI de Streamlit.


## 6. Instalación y ejecución

```bash
git clone https://github.com/LuisaMoreiraM/NLP.git
cd NLP/tfm-disruptors
pip install -r requirements.txt
```
### 6.1. Pipeline (ETL/OCR/NLP)
Ejecuta el flujo principal (sin interfaz). Le una imagen y detecta si hay un disruptor.

```bash
python app.py
```

### 6.2. Ejecución streamlit
Lanza la interfaz para subir imagen y detectar si hay un disruptor.

```bash
python -m streamlit run src/streamlit_app/app_streamlit.py
```

### Datos incluidos
Este repositorio incluye los datos de **data/raw/** (≈53 MB) para que el proyecto sea reproducible sin descargas externas.  
Los resultados de **data/processed/** se generan al ejecutar el ETL (salvo `disruptores_final.parquet` si se incluye como demo).

### Créditos de datos (origen)
- EDLists — https://edlists.org/
- CosIng (Anexos) — https://ec.europa.eu/growth/tools-databases/cosing/reference/annexes
- ECHA — https://echa.europa.eu/es/ed-assessment
- Open Beauty Facts — https://world.openbeautyfacts.org/data

Consulte las condiciones de uso de cada fuente. Este repo incluyen los fichero ya descargados con fines académicos.



