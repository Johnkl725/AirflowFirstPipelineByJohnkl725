from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import io

POSTGRES_CONN = 'postgresql://usuario:password@localhost:5432/tu_db'  # Cambia por tu conexión
TABLE_NAME = 'sbs_web_scrap'
URL = 'https://www.contextures.com/xlSampleData01.html'


def scrap_sbs_table_bs4(**kwargs):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    response = requests.get(URL, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    if not tables:
        raise Exception('No se encontró ninguna tabla en la página')
    table_html = str(tables[0])
    try:
        df = pd.read_html(io.StringIO(table_html))[0]
        print('Head de la tabla extraída:')
        print(df.head())
        kwargs['ti'].xcom_push(key='scrap_df', value=df.to_json())
    except Exception as e:
        print(f"Error parsing table HTML: {e}")
        raise


def load_to_postgres(**kwargs):
    engine = create_engine(POSTGRES_CONN)
    import json
    df_json = kwargs['ti'].xcom_pull(key='scrap_df', task_ids='scrap_sbs_table_bs4')
    if df_json is None:
        raise ValueError("No se recibió ningún DataFrame del scraping (XCom está vacío)")
    df = pd.read_json(df_json)
    df['fecha_carga'] = datetime.now()
    print('Head antes de cargar a Postgres:')
    print(df.head())
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f'Tabla {TABLE_NAME} guardada en PostgreSQL')


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sbs_web_scrap_etl_selenium',
    default_args=default_args,
    description='Web scraping SBS con Selenium y carga a PostgreSQL',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

scrap_task = PythonOperator(
    task_id='scrap_sbs_table_bs4',
    python_callable=scrap_sbs_table_bs4,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

scrap_task >> load_task
