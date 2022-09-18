from datetime import datetime

import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

PG_USER = os.environ.get('POSTGRES_USER')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
PG_PORT = os.environ.get('POSTGES_PORT')
PG_DB = os.environ.get('POSTGRES_DB')
PG_HOST = os.environ.get('PG_HOST')

path_to_local_home = os.environ.get("AIRFLOW_HOME", "opt/airflow/")

URL_PREFIX =  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
URL_TEMPLATE = URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet" 

with DAG(
    dag_id='Local_ingestion_dag',
    schedule_interval = "0 6 2 * *",
    start_date=datetime(2022,1,17)
) as dag:

    wget_task = BashOperator(
        task_id="wget_task",
        bash_command=f'curl -sSL {URL_TEMPLATE} -o {OUTPUT_FILE_TEMPLATE}',
        # bash_command='echo "{{ execution_date.strftime(\'%Y-%m\') }}" '
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs = {
            "user":PG_USER,
            "password":PG_PASSWORD,
            "host":PG_HOST,
            "port":PG_PORT,
            "db":PG_DB,
            "table_name":
            "file_name": OUTPUT_FILE_TEMPLATE
        }
    )

    wget_task  >> ingest_task