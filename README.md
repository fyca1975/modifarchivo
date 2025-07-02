# Modificador de Flujos de Swaps

## 🎯 Objetivo

Este script en Python procesa archivos de flujos para operaciones de swaps, modificando los valores en los archivos `.csv` usando información proveniente de archivos `.dat`. El proceso cumple las siguientes condiciones:

- Los archivos deben tener la misma fecha:
  - CSV: `flujos_swap_gbo_aaaammdd.csv`
  - DAT: `COL_ESTIM_FLOWS_ddmmaaaa.dat`



---

## 📂 Estructura del Proyecto

modificador_swaps/
├── data/ # Carpeta de entrada
│ ├── flujos_swap_gbo_aaaammdd.csv
│ └── COL_ESTIM_FLOWS_ddmmaaaa.dat
├── procesados/ # Carpeta de salida con archivos modificados
├── script_modificador.py
├── .gitignore
└── README.md