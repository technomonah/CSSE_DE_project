import os

from datetime import datetime
from google.cloud import storage
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pyspark_archive_data_transform import archive_data_standartize
from pyspark_archive_data_transform import archive_data_reschema


archive_year = "{{ execution_date.strftime(\'%Y\') }}"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def upload_to_gcs(bucket,year):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    local_data_path = f'{path_to_local_home}/data/pq/{year}/'
    client = storage.Client()
    bucket = client.bucket(bucket)
    part = 0
    for subdir, dirs, files in os.walk(local_data_path):
        for item in os.listdir(local_data_path):
            if not item.startswith(('.','_')) and os.path.isfile(os.path.join(local_data_path, item)):
                part+=1
                blob = bucket.blob(f'raw/{year}/archive_data_{year}_p{part}.parquet')
                blob.upload_from_filename(local_data_path+item)


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="csse_archive_data",
    schedule_interval="@yearly",
    default_args=default_args,
    start_date=datetime(2020,1,1),
    catchup=True,
    max_active_runs=3,
    tags=['CSSE-data'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"/opt/airflow/dags/archive_datawget.sh {archive_year} {path_to_local_home}/data/raw"
    )

    normalize_data_task = PythonOperator(
        task_id="normalize_data_task",
        python_callable=archive_data_standartize,
        op_kwargs={
            "path_to_data":f"{path_to_local_home}/data",
            "year":f"{archive_year}",
        },
    )

    reschema_data_task = PythonOperator(
        task_id="reschema_data_task",
        python_callable=archive_data_reschema,
        op_kwargs={
            "path_to_data":f"{path_to_local_home}/data",
            "year":f"{archive_year}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "year":f"{archive_year}",
        },
    ) 

    clean_up_task = BashOperator(
        task_id="clean_up_raw_task",
        bash_command=f"rm -r {path_to_local_home}/data/raw/{archive_year} \
            {path_to_local_home}/data/clean/{archive_year} \
            {path_to_local_home}/data/pq/{archive_year}"
    )    

    
    download_dataset_task >> normalize_data_task >> reschema_data_task >> local_to_gcs_task >> clean_up_task