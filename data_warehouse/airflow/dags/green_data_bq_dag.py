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

fields = [{"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"}, 
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"}]

with DAG(
    dag_id="green_data_to_BQ",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['Green']
) as dag:

    # gcs_to_ext_tab_task = BigQueryCreateExternalTableOperator(
    #     task_id="gcs_to_ext_tab_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId":"external_green_tripdata"
    #         },
    #         "externalDataConfiguration": {
    #             "autodetect": True,
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{ BUCKET }/green_taxi_data/*"],
    #             "schema_fields": fields
    #         },
    #     }
    # )

    gcs_to_ext_tab_task = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_ext_tab_task",
        bucket = BUCKET,
        source_objects = ["/green_taxi_data/*.parquet"],
        destination_project_dataset_table=f"{ BIGQUERY_DATASET }.external_green_tripdata",
        source_format='PARQUET',
        schema_fields = fields 
    )

    CREATE_PARTITIONED_TABLE_QUERY = f"CREATE OR REPLACE TABLE { BIGQUERY_DATASET }.external_green_partitioned \
        PARTITION BY DATE(lpep_pickup_datetime) AS \
        SELECT * FROM { BIGQUERY_DATASET }.external_green_tripdata"

    bq_ext_to_part_task = BigQueryInsertJobOperator(
        task_id="bq_ext_to_part_task",
        configuration={
            "query": {
                "query": CREATE_PARTITIONED_TABLE_QUERY,
                "useLegacySql": False
            }
        }
    )

    gcs_to_ext_tab_task >> bq_ext_to_part_task

