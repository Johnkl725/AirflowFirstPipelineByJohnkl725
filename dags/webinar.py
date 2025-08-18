from sqlalchemy import create_engine
POSTGRES_CONN = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
OUTPUT_DQ = '/opt/airflow/dags/data/data_quality_report.html'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import pandas as pd

default_args = {
    'owner': 'coder2j',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


PATH = Variable.get('titanic_csv_path', default_var='/opt/airflow/data/titanic.csv')
URL = Variable.get('titanic_csv_url', default_var='https://raw.githubusercontent.com/datasciencedojo/datasets/refs/heads/master/titanic.csv')
PARQUET_PATH = Variable.get('titanic_parquet_path', default_var='/opt/airflow/data/titanic.parquet')
EMAIL = Variable.get('etl_notify_email', default_var='tu_email@gmail.com')

def validate_csv(**context):
    try:
        df = pd.read_csv(PATH)
        nulls = df.isnull().sum().sum()
        dtypes = df.dtypes.to_dict()
        duplicates = df.duplicated().sum()
        context['ti'].xcom_push(key='nulls', value=nulls)
        context['ti'].xcom_push(key='dtypes', value=str(dtypes))
        context['ti'].xcom_push(key='duplicates', value=duplicates)
        print(f"Nulls: {nulls}, Duplicates: {duplicates}, Dtypes: {dtypes}")
    except Exception as e:
        print(f"Error in validation: {e}")
        raise

def clean_csv(**context):
    try:
        df = pd.read_csv(PATH)
        df = df.drop_duplicates()
        df = df.fillna(0)
        df.to_csv(PATH, index=False)
        context['ti'].xcom_push(key='rows_cleaned', value=len(df))
        print(f"Cleaned CSV, rows: {len(df)}")
    except Exception as e:
        print(f"Error in cleaning: {e}")
        raise

def process_csv_to_parquet(**context):
    try:
        df = pd.read_csv(PATH)
        df.to_parquet(PARQUET_PATH)
        context['ti'].xcom_push(key='rows_parquet', value=len(df))
        print(f"Parquet saved, rows: {len(df)}")
    except Exception as e:
        print(f"Error in parquet conversion: {e}")
        raise

def show_parquet_head(**context):
    try:
        df = pd.read_parquet(PARQUET_PATH)
        print(df.head())
        context['ti'].xcom_push(key='head', value=df.head().to_json())
    except Exception as e:
        print(f"Error in show head: {e}")
        raise
def load_to_postgres(**context):
    df = pd.read_parquet(PARQUET_PATH)
    df['fecha_carga'] = datetime.now()
    engine = create_engine(POSTGRES_CONN)
    # Leer datos existentes
    try:
        existing = pd.read_sql('SELECT * FROM titanic', engine)
        # Suponiendo que hay una columna 'PassengerId' como clave única
        new_rows = df[~df['PassengerId'].isin(existing['PassengerId'])]
        if not new_rows.empty:
            new_rows['fecha_carga'] = datetime.now()
            new_rows.to_sql('titanic', engine, if_exists='append', index=False)
            print(f'Se agregaron {len(new_rows)} filas nuevas a la tabla titanic.')
        else:
            print('No hay filas nuevas para agregar.')
    except Exception:
        # Si la tabla no existe, crea y carga todo
        df.to_sql('titanic', engine, if_exists='replace', index=False)
        print('Tabla creada y datos cargados en la tabla titanic de PostgreSQL')

with DAG(
    dag_id='etl_titanic_parquet',
    default_args=default_args,
    description='ETL Titanic: descarga, valida, limpia, transforma y notifica',
    start_date=datetime(2021, 7, 29, 2),
    schedule='@daily',
    catchup=False
) as dag:
    download_csv = BashOperator(
        task_id='download_titanic_csv',
        bash_command='mkdir -p {{ params.dir }} && curl -o {{ params.path }} {{ params.url }}',
        params={
            'dir': '/opt/airflow/data',
            'path': PATH,
            'url': URL
        }
    )

    validate = PythonOperator(
        task_id='validate_csv',
        python_callable=validate_csv
    )

    clean = PythonOperator(
        task_id='clean_csv',
        python_callable=clean_csv
    )

    csv_to_parquet = PythonOperator(
        task_id='csv_to_parquet',
        python_callable=process_csv_to_parquet
    )

    show_head = PythonOperator(
        task_id='show_parquet_head',
        python_callable=show_parquet_head
    )

    load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    download_csv >> validate >> clean >> csv_to_parquet >> show_head >> load_postgres