from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.utils import limpiar_votos_por_correos
from etl.extract import extract_excel_to_parquet
from etl.transform import transform_parquet
from etl.load import load_top_to_postgres

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_top_votados_incremental2',
    default_args=default_args,
    description='ETL modular para carga incremental en PostgreSQL',
    schedule='@daily',
    start_date=datetime(2021, 7, 29),
    catchup=False
) as dag:

    limpiar_correos = PythonOperator(
        task_id='limpiar_votos_por_correos',
        python_callable=limpiar_votos_por_correos,
    )

    extract = PythonOperator(
        task_id='extract_excel_to_parquet',
        python_callable=extract_excel_to_parquet,
    )

    transform = PythonOperator(
        task_id='transform_parquet',
        python_callable=transform_parquet,
    )

    load = PythonOperator(
        task_id='load_top_to_postgres',
        python_callable=load_top_to_postgres,
    )

    limpiar_correos >> extract >> transform >> load