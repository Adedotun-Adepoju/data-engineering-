import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
  
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="yellow_data_to_BQ",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['Yellow']
) as dag:

    # CREATE EXTERNAL TABLE
    gcs_to_ext_tab_task = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_ext_tab_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId":"external_yellow_tripdata",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/yellow_taxi_data/*"],
            },
        },
    )

    CREATE_PARTITIONED_TABLE_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET }.external_yellow_partitioned \
        PARTITION BY DATE(tpep_pickup_datetime) AS \
        SELECT * FROM {BIGQUERY_DATASET}.external_table"

    # TASK TO CREATE PARTITIONED TABLE ON BIGQUERY
    bq_ext_to_part_task = BigQueryInsertJobOperator(
        task_id="bq_ext_to_part_task",
        configuration={
            "query": {
                "query": CREATE_PARTITIONED_TABLE_QUERY, 
                "useLegacySql": False
            }
        },
    )

    gcs_to_ext_tab_task >> bq_ext_to_part_task
