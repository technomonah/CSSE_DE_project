# Airflow DAGs

Airflow will be runned in docker-compose network. 

`Dockerfile`, `docker-compose.yaml` and `requirements.txt` have everything and project spec to run Airflow in Docker containers. 

`.env` text file with GCP and Airflow creds. Should be updated with user's GCP credentials before building images for Docker.

**dag** dir contains code for DAGs and needed scripts: 
- **csse_archive_data_dag.py**. Runs 5 tasks yearly from 2020 to actual date. 
  - **download_dataset_task**. Downloads dataset files for year and compress them to `.gzip` via bash script `archive_datawget.sh`. 
  - **normalize_data_task**. Reads and transform downloaded data via PySpark script `pyspark_archive_data_transform.py`.
  - **reschema_data_task**. Apply schema with correct datatypes to data via PySpark script `pyspark_archive_data_transform.py`.
  - **local_to_gcs_task**. Uploads data from local storage to GCP Data Lake via `google.cloud` lib and function `upload_to_gcs` in DAGs code.
  - **clean_up_task**. Runs a bash command to remove downloaded files from local storage. 

- **csse_archive_data_dag.py**. Runs 5 tasks daily from actual date.
  - **download_dataset_task**. Downloads dataset files for yesterday and compress them to `.gzip` via bash script `archive_datawget.sh`. 
  - **normalize_data_task**. Reads and transform downloaded data via PySpark script `pyspark_archive_data_transform.py`.
  - **reschema_data_task**. Apply schema with correct datatypes to data via PySpark script `pyspark_archive_data_transform.py`.
  - **local_to_gcs_task**. Uploads data from local storage to GCP Data Lake via `google.cloud` lib and function `upload_to_gcs` in DAGs code.
  - **clean_up_task**. Runs a bash command to remove downloaded files from local storage. 

- **csse_data_processing_dag.py**. Runs 3 tasks daily from actual date. 
  - **wait_for_archive_data**. Waits for completion of the archive data DAG and trigger next this DAG. 
  - **process_data_task**. Reads data from project Data Lake, transform and calculate fields, write tables to Data Lake via `pyspark_processing_data.py`.
  - **bigquery_tables_tg**. Uploads transformed tables from Data Lake to BigQuery and partitioning them if needed. 

#### How to run
1. Copy `airflow` dir from repo.
2. Make dirs for logs and plugins in `/airflow`:
```
mkdir -p ./logs ./plugins
```
3. Edit `.env` file with your credits: 
- `GCP_PROJECT_ID`
- `GCP_GCS_BUCKET`
4. Build images for Docker.
```bash
docker-compose build
```
5. Run Docker containers.
```
docker-compose up
```
6. Now Airflow running on 8080 port, so can forward it and open in browser at localhost:8080.


Trigger DAG with these sequence: 
1. `csse_archive_data_dag`
3. Wait for complete all dag runs.
2. `csse_actual_data_dag` and `csse_data_processing_dag`
