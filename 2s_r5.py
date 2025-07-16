import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import os

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def extraer_fecha(nombre_archivo: str, tipo: str) -> datetime:
    """Extrae la fecha del nombre de archivo según el tipo."""
    if tipo == 'csv':
        return datetime.strptime(nombre_archivo.split('_')[-1].replace('.csv', ''), '%Y%m%d')
    elif tipo == 'dat':
        return datetime.strptime(nombre_archivo.split('_')[-1].replace('.dat', ''), '%d%m%Y')
    else:
        raise ValueError("Tipo de archivo no reconocido para extraer fecha")

def cargar_archivos(ruta_csv: Path, ruta_dat: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga los archivos .csv y .dat en DataFrames de pandas."""
    df_csv = pd.read_csv(ruta_csv, sep=';', dtype=str)
    df_dat = pd.read_csv(ruta_dat, sep=';', dtype=str)
    logging.info("Archivos cargados correctamente.")
    return df_csv, df_dat

def convertir_fechas(df: pd.DataFrame, col: str, formato: str) -> pd.DataFrame:
    """Convierte columna de fecha a datetime."""
    df[col] = pd.to_datetime(df[col], format=formato, errors='coerce')
    return df

def procesar(df_csv: pd.DataFrame, df_dat: pd.DataFrame) -> pd.DataFrame:
    """Procesa y actualiza los valores según las reglas del negocio."""
    df_csv = convertir_fechas(df_csv, 'fecha_cobro', '%d/%m/%Y')
    df_dat = convertir_fechas(df_dat, 'M_DATE', '%d/%m/%Y')

    df_csv[['der_intereses', 'obl_intereses', 'der_vp', 'obl_vp']] = df_csv[
        ['der_intereses', 'obl_intereses', 'der_vp', 'obl_vp']
    ].fillna(0).astype(float)

    for _, row in df_dat.iterrows():
        mask = (
            (df_csv['cod_emp'] == row['M_CONTRACT_']) &
            (df_csv['fecha_cobro'] == row['M_DATE'])
        )
        if mask.any():
            try:
                discflow = float(row['M_DISCFLOW'])
                flow_col = float(row['M_FLOW_COL'])
            except ValueError:
                logging.warning(f"Valores no numéricos en fila: {row}")
                continue

            if discflow > 0:
                df_csv.loc[mask, 'der_intereses'] = discflow
            elif discflow < 0:
                df_csv.loc[mask, 'obl_intereses'] = abs(discflow)

            if flow_col > 0:
                df_csv.loc[mask, 'der_vp'] = flow_col
            elif flow_col < 0:
                df_csv.loc[mask, 'obl_vp'] = abs(flow_col)

    logging.info("Archivo procesado correctamente.")
    return df_csv

def guardar_archivo(df: pd.DataFrame, nombre: str, carpeta: Path):
    """Guarda el DataFrame modificado en la carpeta destino."""
    carpeta.mkdir(exist_ok=True)
    ruta_salida = carpeta / nombre
    df.to_csv(ruta_salida, sep=';', index=False)
    logging.info(f"Archivo guardado en: {ruta_salida}")

def main():
    data_folder = Path("data")
    salida_folder = Path("procesados")

    # Buscar archivos
    archivos_csv = list(data_folder.glob("flujos_swap_gbo_*.csv"))
    archivos_dat = list(data_folder.glob("COL_ESTIM_FLOWS_*.dat"))

    if not archivos_csv or not archivos_dat:
        logging.error("Faltan archivos .csv o .dat en la carpeta data.")
        return

    ruta_csv = archivos_csv[0]
    ruta_dat = archivos_dat[0]

    try:
        fecha_csv = extraer_fecha(ruta_csv.name, 'csv')
        fecha_dat = extraer_fecha(ruta_dat.name, 'dat')
    except Exception as e:
        logging.error(f"Error extrayendo fechas: {e}")
        return

    if fecha_csv != fecha_dat:
        logging.error("Las fechas en los nombres de archivo no coinciden.")
        return

    try:
        df_csv, df_dat = cargar_archivos(ruta_csv, ruta_dat)
        df_procesado = procesar(df_csv, df_dat)
        guardar_archivo(df_procesado, ruta_csv.name, salida_folder)
    except Exception as e:
        logging.exception(f"Ocurrió un error procesando archivos: {e}")

if __name__ == "__main__":
    main()
