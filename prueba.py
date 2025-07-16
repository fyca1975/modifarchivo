from pathlib import Path
import pandas as pd

# Crear carpetas necesarias
Path("data").mkdir(exist_ok=True)
Path("procesados").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Crear archivo COL_ESTIM_FLOWS_03062025.dat
estimaciones = pd.DataFrame({
    "M_CONTRACT_": ["ABC123", "XYZ789"],
    "M_DATE": ["03/06/2025", "03/06/2025"],
    "M_DISCFLOW": [1000.0, -2000.0],
    "M_FLOW_COL": [-1500.0, 3000.0]
})
estimaciones.to_csv("data/COL_ESTIM_FLOWS_03062025.dat", sep=";", index=False)

# Crear archivo flujos_swap_gbo_20250603.csv
flujos = pd.DataFrame({
    "cod_emp": ["ABC123", "XYZ789", "ZZZ999"],
    "fecha_cobro": ["2025-06-03", "2025-06-03", "2025-06-03"],
    "der_intereses": [0, 0, 0],
    "obl_intereses": [0, 0, 0],
    "der_vp": [0, 0, 0],
    "obl_vp": [0, 0, 0]
})
flujos.to_csv("data/flujos_swap_gbo_20250603.csv", sep=";", index=False)

# Crear archivo Informe_R5_GBO_250603.csv
informe = pd.DataFrame({
    "codigo_operacion": ["ABC123", "XYZ789", "NO_MATCH"],
    "cupon": [0, 0, 0],
    "cupon_1": [0, 0, 0]
})
informe.to_csv("data/Informe_R5_GBO_250603.csv", sep=";", index=False)

"Archivos de prueba generados exitosamente en la carpeta 'data/'."
