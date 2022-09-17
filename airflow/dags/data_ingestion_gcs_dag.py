import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq 

import gdown

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = "https://drive.google.com/file/d/1PVxj7DIHZb3RIvuKTrhl873rBUcaonwd/view?usp=sharing"   
file_id = dataset_url.split("/")[-2]
path_to_local_home = os.environ.get("AIRFLOW_HOME", "opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def download_from_drive(url, output):
    gdown.download(url, output, quiet=False)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Cam only accept source files in CSV format, for the moment")
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
    

def upload_to_gcs(bucket, local_file, object_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    # The ID of your GCS bucket
    :param bucket_name: "your-bucket-name
    :local_file: path to the file to upload
    :object_name: "destination_object_name"
    """

    # prevent timeout for files > 6 MB on 800 kbps upload speed
    storage.blob.MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5 MB
    storage.blob.DEFAULT_CHUNKSIZE = 5 * 1024 * 1024 # 5 MB 

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:

    # download_dataset_task = BashOperator(
    #     task_id="download_Dataset_task",
    #     bash_command=f"gdown --id ${file_id} -O {dataset_file}"
    # )

    download_dataset_task = PythonOperator(
        task_id="download_Dataset_task",
        python_callable = download_from_drive,
        op_kwargs= {
            'url':dataset_url,
            'output':dataset_file
        }
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}"
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId":"external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet >> local_to_gcs_task >> bigquery_external_table_task
