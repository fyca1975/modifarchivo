# main.py
from utils import procesar_archivos
import os

if __name__ == "__main__":
    carpeta_entrada = "data"
    carpeta_salida = "procesados"

    os.makedirs(carpeta_salida, exist_ok=True)
    procesar_archivos(carpeta_entrada, carpeta_salida)
