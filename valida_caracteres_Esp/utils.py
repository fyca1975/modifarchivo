# utils.py
import os
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def limpiar_texto(texto):
    if pd.isnull(texto):
        return texto

    texto = str(texto)
    # Reemplazo de vocales con tilde y Ñ/ñ
    traducciones = str.maketrans(
        "áéíóúÁÉÍÓÚñÑ",
        "aeiouAEIOUnN"
    )
    texto = texto.translate(traducciones)

    # Reemplazos exactos de patrones
    texto = texto.replace(";033;", ";33;")
    texto = texto.replace(";011001;", ";11001;")

    return texto

def procesar_archivo(ruta_archivo, carpeta_salida):
    nombre_archivo = os.path.basename(ruta_archivo)
    logging.info(f"Procesando archivo: {nombre_archivo}")

    for encoding in ['latin1', 'utf-8']:
        try:
            df = pd.read_csv(ruta_archivo, sep=';', dtype=str, encoding=encoding)
            break
        except Exception as e:
            logging.warning(f"Error con codificación {encoding}: {e}")
    else:
        logging.error(f"No se pudo leer el archivo {nombre_archivo}")
        return

    df = df.applymap(limpiar_texto)

    salida = os.path.join(carpeta_salida, nombre_archivo)
    df.to_csv(salida, sep=';', index=False, encoding='utf-8')
    logging.info(f"Archivo guardado en: {salida}")

def procesar_archivos(carpeta_entrada, carpeta_salida):
    for archivo in os.listdir(carpeta_entrada):
        if archivo.lower().endswith('.csv'):
            ruta = os.path.join(carpeta_entrada, archivo)
            procesar_archivo(ruta, carpeta_salida)
