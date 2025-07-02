import pandas as pd
import os
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Función para buscar y emparejar archivos CSV y DAT en la carpeta de entrada
def emparejar_archivos(ruta_data):
    archivos = os.listdir(ruta_data)
    pares = []

    archivos_csv = [f for f in archivos if f.startswith("flujos_swap_gbo_") and f.endswith(".csv")]
    archivos_dat = [f for f in archivos if f.startswith("COL_ESTIM_FLOWS_") and f.endswith(".dat")]

    for csv in archivos_csv:
        fecha_csv = csv.replace("flujos_swap_gbo_", "").replace(".csv", "")
        try:
            fecha_csv_dt = datetime.strptime(fecha_csv, "%Y%m%d")
            fecha_dat_buscar = fecha_csv_dt.strftime("%d%m%Y")

            for dat in archivos_dat:
                if fecha_dat_buscar in dat:
                    pares.append((csv, dat))
        except ValueError:
            logging.warning(f"Formato de fecha inválido en el archivo: {csv}")

    return pares

# Función principal para procesar cada par de archivos
def procesar_archivos(ruta_data, ruta_procesados):
    pares = emparejar_archivos(ruta_data)
    if not pares:
        logging.info("No se encontraron archivos compatibles para procesar.")
        return

    for archivo_csv, archivo_dat in pares:
        logging.info(f"Procesando: {archivo_csv} y {archivo_dat}")

        ruta_csv = os.path.join(ruta_data, archivo_csv)
        ruta_dat = os.path.join(ruta_data, archivo_dat)

        df_csv = pd.read_csv(ruta_csv, delimiter=';', dtype=str)
        df_dat = pd.read_csv(ruta_dat, delimiter=';', dtype=str)

        df_dat['M_DATE'] = pd.to_datetime(df_dat['M_DATE'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

        for _, fila_dat in df_dat.iterrows():
            m_contract = fila_dat['M_CONTRACT']
            m_date = fila_dat['M_DATE']
            m_leg = fila_dat['M_LEG']
            m_flow_col = abs(float(fila_dat['M_FLOW_COL']))
            m_discflow = abs(float(fila_dat['M_DISCFLOW']))

            condicion = (df_csv['nro_papeleta'] == m_contract) & (df_csv['fecha_cobro'] == m_date)

            if condicion.any():
                if m_leg == '1':
                    df_csv.loc[condicion, 'der_intereses'] = m_flow_col
                    df_csv.loc[condicion, 'der_vp'] = m_discflow
                elif m_leg == '2':
                    df_csv.loc[condicion, 'obl_intereses'] = m_flow_col
                    df_csv.loc[condicion, 'obl_vp'] = m_discflow

        os.makedirs(ruta_procesados, exist_ok=True)
        ruta_salida = os.path.join(ruta_procesados, archivo_csv)
        df_csv.to_csv(ruta_salida, index=False, sep=';')
        logging.info(f"Archivo procesado guardado en: {ruta_salida}")

# Ejecución de ejemplo
data_dir = "data"
procesados_dir = "procesados"

if __name__ == "__main__":
    procesar_archivos(data_dir, procesados_dir)
