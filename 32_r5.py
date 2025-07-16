#!/usr/bin/env python3
"""
Script para procesamiento de archivos swap con validación de fechas y transformación de datos.

Este script procesa archivos relacionados con operaciones swap:
- flujos_swap_gbo_*.csv
- COL_ESTIM_FLOWS_*.dat  
- Informe_R5_GBO_*.csv

Autor: Expert Python Developer
Versión: 1.0
Python: 3.13+
"""

import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
import sys


class SwapProcessor:
    """
    Procesador principal para archivos swap con validación y transformación de datos.
    """
    
    def __init__(self, data_dir: str = "data", output_dir: str = "procesados", log_dir: str = "logs"):
        """
        Inicializa el procesador de swaps.
        
        Args:
            data_dir: Directorio donde se encuentran los archivos de entrada
            output_dir: Directorio donde se guardarán los archivos procesados
            log_dir: Directorio para archivos de log
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        
        # Crear directorios si no existen
        self._create_directories()
        
        # Configurar logging
        self._setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("SwapProcessor inicializado correctamente")
    
    def _create_directories(self) -> None:
        """Crea los directorios necesarios si no existen."""
        for directory in [self.data_dir, self.output_dir, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _setup_logging(self) -> None:
        """Configura el sistema de logging."""
        log_file = self.log_dir / "procesamiento.log"
        
        # Configurar formato de logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Configurar handlers
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Configurar formatters
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configurar logger raíz
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler],
            format=log_format
        )
    
    def validar_fecha_archivos(self, fecha_referencia: str) -> Dict[str, Optional[Path]]:
        """
        Valida que existan archivos para la fecha especificada.
        
        Args:
            fecha_referencia: Fecha en formato YYYYMMDD (ej: 20250603)
            
        Returns:
            Diccionario con las rutas de los archivos encontrados
            
        Raises:
            ValueError: Si la fecha no tiene el formato correcto
            FileNotFoundError: Si no se encuentran archivos requeridos
        """
        try:
            # Validar formato de fecha
            datetime.strptime(fecha_referencia, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Formato de fecha incorrecto: {fecha_referencia}. Esperado: YYYYMMDD")
        
        # Convertir formatos de fecha
        fecha_obj = datetime.strptime(fecha_referencia, "%Y%m%d")
        
        # Formatos esperados para cada archivo
        formato_flujos = fecha_referencia  # 20250603
        formato_estim = fecha_obj.strftime("%d%m%Y")  # 03062025
        formato_informe = fecha_obj.strftime("%y%m%d")  # 250603
        
        # Buscar archivos
        archivos = {}
        
        # Archivo flujos_swap_gbo (requerido)
        patron_flujos = f"flujos_swap_gbo_{formato_flujos}.csv"
        archivos['flujos'] = self._buscar_archivo(patron_flujos)
        
        # Archivo COL_ESTIM_FLOWS (requerido)
        patron_estim = f"COL_ESTIM_FLOWS_{formato_estim}.dat"
        archivos['estimaciones'] = self._buscar_archivo(patron_estim)
        
        # Archivo Informe_R5_GBO (opcional)
        patron_informe = f"Informe_R5_GBO_{formato_informe}.csv"
        archivos['informe'] = self._buscar_archivo(patron_informe, requerido=False)
        
        self.logger.info(f"Archivos encontrados para fecha {fecha_referencia}:")
        for tipo, ruta in archivos.items():
            if ruta:
                self.logger.info(f"  {tipo}: {ruta}")
            else:
                self.logger.warning(f"  {tipo}: No encontrado")
        
        return archivos
    
    def _buscar_archivo(self, patron: str, requerido: bool = True) -> Optional[Path]:
        """
        Busca un archivo con el patrón especificado.
        
        Args:
            patron: Patrón del nombre del archivo
            requerido: Si el archivo es requerido
            
        Returns:
            Ruta del archivo encontrado o None
            
        Raises:
            FileNotFoundError: Si el archivo requerido no existe
        """
        archivos_encontrados = list(self.data_dir.glob(patron))
        
        if not archivos_encontrados:
            if requerido:
                raise FileNotFoundError(f"Archivo requerido no encontrado: {patron}")
            return None
        
        if len(archivos_encontrados) > 1:
            self.logger.warning(f"Múltiples archivos encontrados para {patron}, usando el primero")
        
        return archivos_encontrados[0]
    
    def cargar_archivo(self, ruta: Path, separador: str = ';') -> pd.DataFrame:
        """
        Carga un archivo CSV/DAT usando pandas.
        
        Args:
            ruta: Ruta del archivo
            separador: Separador a usar
            
        Returns:
            DataFrame con los datos cargados
            
        Raises:
            Exception: Si hay error al cargar el archivo
        """
        try:
            self.logger.info(f"Cargando archivo: {ruta}")
            
            # Detectar encoding
            encodings = ['utf-8', 'latin-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(ruta, sep=separador, encoding=encoding)
                    self.logger.info(f"Archivo cargado con encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise Exception(f"No se pudo cargar el archivo con ningún encoding: {ruta}")
            
            self.logger.info(f"Archivo cargado exitosamente: {len(df)} filas, {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            self.logger.error(f"Error al cargar archivo {ruta}: {str(e)}")
            raise
    
    def procesar_flujos_swap(self, df_flujos: pd.DataFrame, df_estimaciones: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa y modifica el archivo flujos_swap_gbo basado en estimaciones.
        
        Args:
            df_flujos: DataFrame del archivo flujos_swap_gbo
            df_estimaciones: DataFrame del archivo COL_ESTIM_FLOWS
            
        Returns:
            DataFrame modificado de flujos_swap_gbo
        """
        self.logger.info("Iniciando procesamiento de flujos swap")
        
        # Crear copia para no modificar el original
        df_resultado = df_flujos.copy()
        
        # Validar columnas requeridas
        cols_flujos = ['cod_emp', 'fecha_cobro', 'der_intereses', 'obl_intereses', 'der_vp', 'obl_vp']
        cols_estimaciones = ['M_CONTRACT_', 'M_DATE', 'M_DISCFLOW', 'M_FLOW_COL']
        
        self._validar_columnas(df_flujos, cols_flujos, "flujos_swap_gbo")
        self._validar_columnas(df_estimaciones, cols_estimaciones, "COL_ESTIM_FLOWS")
        
        # Convertir M_DATE a formato datetime para comparación
        df_estimaciones['M_DATE'] = pd.to_datetime(df_estimaciones['M_DATE'], format='%d/%m/%Y', errors='coerce')
        df_resultado['fecha_cobro'] = pd.to_datetime(df_resultado['fecha_cobro'], errors='coerce')
        
        modificaciones = 0
        
        # Procesar cada fila de estimaciones
        for idx, row_est in df_estimaciones.iterrows():
            # Buscar coincidencias en flujos
            mask = (
                (df_resultado['cod_emp'] == row_est['M_CONTRACT_']) &
                (df_resultado['fecha_cobro'] == row_est['M_DATE'])
            )
            
            indices_coincidentes = df_resultado[mask].index
            
            if len(indices_coincidentes) > 0:
                for idx_flujo in indices_coincidentes:
                    # Procesar M_DISCFLOW
                    if pd.notna(row_est['M_DISCFLOW']):
                        if row_est['M_DISCFLOW'] > 0:
                            df_resultado.loc[idx_flujo, 'der_intereses'] = row_est['M_DISCFLOW']
                        elif row_est['M_DISCFLOW'] < 0:
                            df_resultado.loc[idx_flujo, 'obl_intereses'] = abs(row_est['M_DISCFLOW'])
                    
                    # Procesar M_FLOW_COL
                    if pd.notna(row_est['M_FLOW_COL']):
                        if row_est['M_FLOW_COL'] > 0:
                            df_resultado.loc[idx_flujo, 'der_vp'] = row_est['M_FLOW_COL']
                        elif row_est['M_FLOW_COL'] < 0:
                            df_resultado.loc[idx_flujo, 'obl_vp'] = abs(row_est['M_FLOW_COL'])
                    
                    modificaciones += 1
        
        self.logger.info(f"Procesamiento completado. Modificaciones realizadas: {modificaciones}")
        return df_resultado
    
    def procesar_informe_r5(self, df_informe: pd.DataFrame, df_flujos_modificado: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa y modifica el archivo Informe_R5_GBO basado en flujos modificados.
        
        Args:
            df_informe: DataFrame del archivo Informe_R5_GBO
            df_flujos_modificado: DataFrame modificado de flujos_swap_gbo
            
        Returns:
            DataFrame modificado de Informe_R5_GBO
        """
        self.logger.info("Iniciando procesamiento de informe R5")
        
        # Crear copia para no modificar el original
        df_resultado = df_informe.copy()
        
        # Validar columnas requeridas
        cols_informe = ['codigo_operacion', 'cupon', 'cupon_1']
        cols_flujos = ['cod_emp', 'der_vp', 'obl_vp']
        
        self._validar_columnas(df_informe, cols_informe, "Informe_R5_GBO")
        self._validar_columnas(df_flujos_modificado, cols_flujos, "flujos_swap_gbo modificado")
        
        modificaciones = 0
        
        # Procesar cada fila del informe
        for idx, row_informe in df_informe.iterrows():
            codigo_operacion = row_informe['codigo_operacion']
            
            # Buscar coincidencias en flujos modificados
            mask = df_flujos_modificado['cod_emp'] == codigo_operacion
            flujos_coincidentes = df_flujos_modificado[mask]
            
            if len(flujos_coincidentes) > 0:
                # Calcular cupon (suma de der_vp / 1000000)
                suma_der_vp = flujos_coincidentes['der_vp'].fillna(0).sum()
                df_resultado.loc[idx, 'cupon'] = suma_der_vp / 1000000
                
                # Calcular cupon_1 (suma de obl_vp / 1000000)
                suma_obl_vp = flujos_coincidentes['obl_vp'].fillna(0).sum()
                df_resultado.loc[idx, 'cupon_1'] = suma_obl_vp / 1000000
                
                modificaciones += 1
                
                self.logger.debug(f"Código {codigo_operacion}: cupon={suma_der_vp/1000000:.6f}, cupon_1={suma_obl_vp/1000000:.6f}")
        
        self.logger.info(f"Procesamiento de informe R5 completado. Modificaciones realizadas: {modificaciones}")
        return df_resultado
    
    def _validar_columnas(self, df: pd.DataFrame, columnas_requeridas: list, nombre_archivo: str) -> None:
        """
        Valida que el DataFrame tenga las columnas requeridas.
        
        Args:
            df: DataFrame a validar
            columnas_requeridas: Lista de columnas requeridas
            nombre_archivo: Nombre del archivo para logging
            
        Raises:
            ValueError: Si faltan columnas requeridas
        """
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
        
        if columnas_faltantes:
            error_msg = f"Columnas faltantes en {nombre_archivo}: {columnas_faltantes}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info(f"Validación de columnas exitosa para {nombre_archivo}")
    
    def guardar_archivo(self, df: pd.DataFrame, ruta: Path, separador: str = ';') -> None:
        """
        Guarda un DataFrame en un archivo CSV.
        
        Args:
            df: DataFrame a guardar
            ruta: Ruta donde guardar el archivo
            separador: Separador a usar
        """
        try:
            self.logger.info(f"Guardando archivo: {ruta}")
            
            # Crear directorio si no existe
            ruta.parent.mkdir(parents=True, exist_ok=True)
            
            # Guardar archivo
            df.to_csv(ruta, sep=separador, index=False, encoding='utf-8')
            
            self.logger.info(f"Archivo guardado exitosamente: {ruta}")
            
        except Exception as e:
            self.logger.error(f"Error al guardar archivo {ruta}: {str(e)}")
            raise
    
    def procesar_fecha(self, fecha_referencia: str) -> None:
        """
        Procesa todos los archivos para una fecha específica.
        
        Args:
            fecha_referencia: Fecha en formato YYYYMMDD
        """
        try:
            self.logger.info(f"=== Iniciando procesamiento para fecha: {fecha_referencia} ===")
            
            # Paso 1: Validar archivos
            archivos = self.validar_fecha_archivos(fecha_referencia)
            
            # Paso 2: Cargar archivos
            df_flujos = self.cargar_archivo(archivos['flujos'])
            df_estimaciones = self.cargar_archivo(archivos['estimaciones'])
            
            # Paso 3: Procesar flujos swap
            df_flujos_modificado = self.procesar_flujos_swap(df_flujos, df_estimaciones)
            
            # Guardar archivo flujos modificado
            nombre_flujos_procesado = f"flujos_swap_gbo_{fecha_referencia}_procesado.csv"
            ruta_flujos_procesado = self.output_dir / nombre_flujos_procesado
            self.guardar_archivo(df_flujos_modificado, ruta_flujos_procesado)
            
            # Paso 4: Procesar informe R5 si existe
            if archivos['informe']:
                df_informe = self.cargar_archivo(archivos['informe'])
                df_informe_modificado = self.procesar_informe_r5(df_informe, df_flujos_modificado)
                
                # Guardar archivo informe modificado
                fecha_obj = datetime.strptime(fecha_referencia, "%Y%m%d")
                formato_informe = fecha_obj.strftime("%y%m%d")
                nombre_informe_procesado = f"Informe_R5_GBO_{formato_informe}_procesado.csv"
                ruta_informe_procesado = self.output_dir / nombre_informe_procesado
                self.guardar_archivo(df_informe_modificado, ruta_informe_procesado)
            else:
                self.logger.info("Archivo de informe R5 no encontrado, omitiendo procesamiento")
            
            self.logger.info(f"=== Procesamiento completado exitosamente para fecha: {fecha_referencia} ===")
            
        except Exception as e:
            self.logger.error(f"Error durante el procesamiento: {str(e)}")
            raise


def main():
    """
    Función principal del script.
    """
    try:
        # Configurar fecha de procesamiento
        # Cambiar por la fecha deseada en formato YYYYMMDD
        fecha_procesamiento = "20250603"
        
        # Crear procesador
        processor = SwapProcessor()
        
        # Procesar archivos
        processor.procesar_fecha(fecha_procesamiento)
        
        print(f"\n✅ Procesamiento completado exitosamente para fecha: {fecha_procesamiento}")
        print(f"📁 Archivos procesados guardados en: {processor.output_dir}")
        print(f"📋 Logs disponibles en: {processor.log_dir}")
        
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()