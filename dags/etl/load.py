import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
TOP_PARQUET_PATH = "dags/data/top_nominados.parquet"

def load_top_to_postgres(**context):
    top_nominados = pd.read_parquet(TOP_PARQUET_PATH)
    engine = create_engine(POSTGRES_CONN)

    try:
        existing = pd.read_sql('SELECT * FROM tabla_auditoria_votados', engine)
        merged = pd.merge(top_nominados, existing, on=['categoria', 'persona'], how='left', indicator=True)
        new_rows = merged[merged['_merge'] == 'left_only'][top_nominados.columns]

        if not new_rows.empty:
            new_rows.to_sql('tabla_auditoria_votados', engine, if_exists='append', index=False)
            print(f"[LOAD] {len(new_rows)} filas nuevas insertadas")
        else:
            print("[LOAD] No hay filas nuevas para insertar")
    except Exception as e:
        top_nominados.to_sql('tabla_auditoria_votados', engine, if_exists='replace', index=False)
        print("[LOAD] Tabla creada desde cero:", e)
