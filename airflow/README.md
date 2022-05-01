#### DAGs
**csse_archive_data_dag.py** 
These DAG running yearly from 2020 to current year. And contains tasks for downloading, preprocessing and uploadind to the cloud storage. 

Pipeline steps: 
1. Downloads all archive daily reports from 2020 and up to 2 days before the execution date. 
2. Transform and reschema data
3. Upload data to GCP data lake
4. Remove unnecessary files 

**csse_actual_data_dag.py**
These DAG runs daily and run all task as previous one, but for yesterdays daily reports data.

Pipeline steps: 
1. Downloads yesterdays daily report 
2. Transform and reschema data
3. Upload data to GCP data lake
4. Remove unnecessary files

**csse_data_processing_dag.py**
These DAG runs daily and waits for data downloading at `csse_actual_data_dag`. After that it's reads all storage data and creates tables for reports and queries. 

Pipeline steps: 
1. Wait for actual data to complete
2. Read all data from data lake and write separate tables for bigquery dataset
3. Create external tables for reports and partitioned table for all data at bigquery dataset

#### How to run
1. Copy airflow dir from repo
2. Make dirs for logs and plugins in it:
```
mkdir -p ./logs ./plugins
```
3. Edit `.env` file with your creds: 
- `GCP_PROJECT_ID`
- `GCP_GCS_BUCKET`
4. Build images
```bash
docker-compose build
```
5. Run containers
```
docker-compose up
```
6. Now airflow running on 8080 port, so can forward it and open in browser at localhost:8080
7. Trigger DAG with these sequence: 
  - csse_archive_data_dag (wait for complete all dag runs)
  - csse_actual_data_dag and csse_data_processing_dag

