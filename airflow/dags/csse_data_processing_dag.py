import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from pyspark_processing_data import create_rewrite_datasests

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'csse_data')

DATASET = "csse_data"
INPUT_PART = "clean"
INPUT_FILETYPE = "parquet"
TABLES = {'monthly_cases_grouped':'Date',
        'fulldataset':'Last_Update',
        'cases_last_3m':'Date'}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(hours=12),
    "depends_on_past": True,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="csse_data_processing",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=2,
    tags=['CSSE-data'],
) as dag:


    wait_for_archive_data = ExternalTaskSensor(
        task_id='wait_for_archive_data',
        external_task_id='local_to_gcs_task',
        external_dag_id='csse_actual_data',
        poke_interval=30,
    )

    process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=create_rewrite_datasests,
    )

    with TaskGroup(group_id='bq_tg') as bigquery_tables_tg:
        for table in TABLES:
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                        task_id=f"bq_{DATASET}_external_{table}_task",
                        table_resource={
                            "tableReference": {
                                "projectId": PROJECT_ID,
                                "datasetId": BIGQUERY_DATASET,
                                "tableId": f"{DATASET}_external_{table}",
                            },
                            "externalDataConfiguration": {
                                "autodetect": "True",
                                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                                "sourceUris": [f"gs://{BUCKET}/clean/{table}/*"],
                            },
                        },
                    )
            if table == "fulldataset":         
                CREATE_BQ_TBL_QUERY = (
                    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET}_{table}_partitioned \
                    PARTITION BY DATE({TABLES.get(table)}) \
                    AS \
                    SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_{table};"
                )
                # Create a partitioned table from external table
                bq_create_partitioned_table_job = BigQueryInsertJobOperator(
                    task_id=f"bq_create_{DATASET}_partitioned_{table}_task",
                    configuration={
                        "query": {
                            "query": CREATE_BQ_TBL_QUERY,
                            "useLegacySql": False,
                        }
                    }
                )
                bigquery_external_table_task >> bq_create_partitioned_table_job
        
        bigquery_external_table_task
    
   


    wait_for_archive_data >> process_data_task >> bigquery_tables_tg
