import os
import unicodedata

# Carpetas
INPUT_FOLDER = 'data'
OUTPUT_FOLDER = 'procesados'

# Asegura que la carpeta de salida exista
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def limpiar_texto(texto):
    """
    Normaliza texto quitando tildes y reemplazando Ñ por N.
    """
    texto = texto.replace('Ñ', 'N').replace('ñ', 'n')
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

def procesar_archivo(nombre_archivo):
    input_path = os.path.join(INPUT_FOLDER, nombre_archivo)
    output_path = os.path.join(OUTPUT_FOLDER, nombre_archivo)

    with open(input_path, 'r', encoding='utf-8') as entrada, \
         open(output_path, 'w', encoding='utf-8') as salida:

        for linea in entrada:
            linea = limpiar_texto(linea)
            linea = linea.replace(';033;', ';33;')
            linea = linea.replace(';011001;', ';11001;')
            salida.write(linea)

    print(f"Archivo procesado: {nombre_archivo}")

def main():
    archivos = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.csv')]
    
    if not archivos:
        print("No hay archivos CSV en la carpeta 'data'.")
        return

    for archivo in archivos:
        procesar_archivo(archivo)

if __name__ == '__main__':
    main()
