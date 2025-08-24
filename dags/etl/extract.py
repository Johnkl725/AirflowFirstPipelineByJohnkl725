import pandas as pd

EXCEL_PATH = "dags/data/¡VOTA POR LOS PRÓXIMOS EXCELENCIA TI, OPS & DATA 2025-1!(1-1628).xlsx"
PARQUET_PATH = "dags/data/votos.parquet"

def extract_excel_to_parquet(**context):
    df = pd.read_excel(EXCEL_PATH)
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"[EXTRACT] Archivo Parquet guardado en {PARQUET_PATH}, filas: {len(df)}")
