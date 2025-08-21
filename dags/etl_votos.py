from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

POSTGRES_CONN = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
EXCEL_PATH = "dags/data/¡VOTA POR LOS PRÓXIMOS EXCELENCIA TI, OPS & DATA 2025-1!.xlsx"
PARQUET_PATH = "dags/data/votos.parquet"
CONTROL_FILE_PATH = "dags/data/control_file.txt"

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def read_last_processed_time():
    if os.path.exists(CONTROL_FILE_PATH):
        with open(CONTROL_FILE_PATH, 'r') as f:
            last_processed_time = f.read().strip()
        return last_processed_time
    return None

def update_last_processed_time(last_time):
    with open(CONTROL_FILE_PATH, 'w') as f:
        f.write(str(last_time))



def extract_excel_to_parquet(**context):
    df = pd.read_excel(EXCEL_PATH)
    print(f"Filas leídas del archivo Excel: {df.shape[0]}")
    df.to_parquet(PARQUET_PATH)
    print(f"Archivo Parquet guardado en {PARQUET_PATH}, filas: {len(df)}")



def transform_parquet(**context):
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = df.columns.str.strip()
    df_melted = pd.melt(df,
                        id_vars=['Id', 'Hora de inicio', 'Hora de finalización'],
                        value_vars=['Nominados – AEF', 'Nominados - AST', 'Nominados – CTO',
                                    'Nominados – CIO', 'Nominados – Operaciones', 'Nominados – Data'],
                        var_name='categoria',
                        value_name='persona')
    result = df_melted.groupby(['categoria', 'persona']).size().reset_index(name='votos')
    top_nominados = result.groupby('categoria').apply(lambda x: x.nlargest(5, 'votos')).reset_index(drop=True)
    top_nominados['Fecha_Carga'] = datetime.now()
    top_nominados['Proceso'] = 'ETL_Top_Votados'
    top_parquet_path = "dags/data/top_nominados.parquet"
    top_nominados.to_parquet(top_parquet_path)
    print(f"Top nominados guardado en {top_parquet_path}, filas: {len(top_nominados)}")



def load_top_to_postgres(**context):
    top_parquet_path = "dags/data/top_nominados.parquet"
    top_nominados = pd.read_parquet(top_parquet_path)
    engine = create_engine(POSTGRES_CONN)
    try:
        existing = pd.read_sql('SELECT * FROM tabla_auditoria_votados', engine)
        if {'categoria', 'persona'}.issubset(top_nominados.columns) and {'categoria', 'persona'}.issubset(existing.columns):
            merged = pd.merge(top_nominados, existing, on=['categoria', 'persona'], how='left', indicator=True)
            new_rows = merged[merged['_merge'] == 'left_only'][top_nominados.columns]
            duplicados = merged[merged['_merge'] == 'both'][top_nominados.columns]
            if not duplicados.empty:
                print(f"Intento de ingresar {len(duplicados)} valores que ya existen en la tabla.")
        else:
            new_rows = top_nominados
        if not new_rows.empty:
            new_rows.to_sql('tabla_auditoria_votados', engine, if_exists='append', index=False)
            print(f'Nuevos valores ingresados: {len(new_rows)} filas añadidas a la tabla de auditoría.')
        else:
            print('Valores no han cambiado, no se han añadido filas nuevas.')
    except Exception:
        top_nominados.to_sql('tabla_auditoria_votados', engine, if_exists='replace', index=False)
        print('Tabla creada y datos cargados en PostgreSQL.')

with DAG(
    dag_id='etl_top_votados_incremental',
    default_args=default_args,
    description='ETL para carga incremental y auditoría en PostgreSQL',
    schedule='@daily',  # Ejecución diaria
    start_date=datetime(2021, 7, 29),
    catchup=False
) as dag:
    extract = PythonOperator(
        task_id='extract_excel_to_parquet',
        python_callable=extract_excel_to_parquet
    )
    transform = PythonOperator(
        task_id='transform_parquet',
        python_callable=transform_parquet
    )
    load = PythonOperator(
        task_id='load_top_to_postgres',
        python_callable=load_top_to_postgres
    )
    extract >> transform >> load