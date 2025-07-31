import os
import unicodedata
import logging

# Configura el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Función para limpiar caracteres especiales
def limpiar_texto(texto):
    """
    Normaliza texto:
      - Cambia ñ/Ñ por n/N
      - Quita tildes de vocales
    """
    # Reemplaza ñ/Ñ por n/N
    texto = texto.replace('ñ', 'n').replace('Ñ', 'N')
    # Quita tildes usando unicodedata
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join([c for c in texto if not unicodedata.combining(c)])
    return texto

# Función principal para procesar los archivos
def procesar_archivos(directorio_entrada, directorio_salida):
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)
        logging.info(f"Carpeta '{directorio_salida}' creada.")
    archivos = [f for f in os.listdir(directorio_entrada) if os.path.isfile(os.path.join(directorio_entrada, f))]
    if not archivos:
        logging.warning("No hay archivos para procesar.")
        return

    for archivo in archivos:
        entrada_path = os.path.join(directorio_entrada, archivo)
        salida_path = os.path.join(directorio_salida, archivo)
        logging.info(f"Procesando archivo: {archivo}")

        with open(entrada_path, 'r', encoding='utf-8') as f_in, \
             open(salida_path, 'w', encoding='utf-8') as f_out:
            for linea in f_in:
                linea = limpiar_texto(linea)
                linea = linea.replace(';033;', ';33;')
                linea = linea.replace(';011001;', ';11001;')
                f_out.write(linea)
        logging.info(f"Archivo procesado guardado en: {salida_path}")

if __name__ == "__main__":
    procesar_archivos('data', 'procesados')
