from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Add your ETL folder to path inside the container
sys.path.insert(0, '/opt/airflow/etl')

from extract import extract_deliveries
from transform import transform_deliveries
from load import load_deliveries

def run_etl():
    raw_df = extract_deliveries()
    clean_df = transform_deliveries(raw_df)
    load_deliveries(clean_df)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG('etl_pipeline_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl
    )

    etl_task
