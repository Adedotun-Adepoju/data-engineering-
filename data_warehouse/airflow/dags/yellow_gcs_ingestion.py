import os 
import logging

from datetime import datetime 

from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "opt/airflow")

yellow_taxi_url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'

file_suffix = '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

yellow_url_template = yellow_taxi_url_prefix + file_suffix
yellow_file_template = f'{path_to_local_home}/yellow_{file_suffix}'


def upload_to_gcs(bucket, local_file, object_name):
    # prevent timeout for files > 6 MB on 800 kbps upload speed
    storage.blob.MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5 MB
    storage.blob.DEFAULT_CHUNKSIZE = 5 * 1024 * 1024 # 5 M

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2021,12,30)
}

with DAG(
    dag_id="yellow_data_ingestion",
    schedule_interval = "0 6 2 * *",
    catchup = True,
    max_active_runs = 1,
    default_args = default_args
) as dag: 

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -SS { yellow_url_template } -o {yellow_file_template}"
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs = {
            "bucket": BUCKET,
            "local_file": yellow_file_template,
            "object_name": f"yellow_taxi_data/{file_suffix}"
        }
    )

    clean_up_task = BashOperator(
        task_id="clean_up_task",
        bash_command=f"rm { yellow_file_template } "
    )

    download_dataset_task >> upload_to_gcs_task >> clean_up_task