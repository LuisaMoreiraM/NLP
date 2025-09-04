import os
import easyocr

def procesar_ocr (archivo = "etiqueta_01.jpg") :
    """
    Realiza OCR sobre una imagen de etiqueta y guarda el texto extraído en un archivo.
    Exporta el texto como string.
    """
    
    # Base del proyecto: dos niveles arriba desde este archivo
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    # ruta_img = os.path.join(base_dir, "data", "raw", "etiquetas")
    # processed_dir = os.path.join(base_dir, "data", "processed")

    # Detectar si archivo es ya una ruta completa
    if os.path.isfile(archivo):
        ruta_completa = archivo
    else:
        ruta_img = os.path.join(base_dir, "data", "raw", "etiquetas")
        ruta_completa = os.path.join(ruta_img, archivo)

    # OCR
    reader = easyocr.Reader(['es', 'en']) 
    results = reader.readtext(ruta_completa, detail=0)

    # Une el texto
    texto_extraido = ' '.join(results)

    print("Texto extraído:\n", texto_extraido)
    
    return texto_extraido

if __name__ == "__main__":
    texto_extraido= procesar_ocr("etiqueta_075.jpg")
    texto_extraido


