from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nhom6',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Task definitions will go here
    pass
