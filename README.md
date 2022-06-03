# Project about 
It's course project at data-engineering-zoomcamp by [DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp).

For this project I've tried to build a batch pipeline to process COVID data from git-repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University. The goal is to build a dasboard for monitoring confirmed COVID cases in the world, where user can choose city and look at current incidence rate, number of daily cases and so on.

Something like a weather checker useful not to decide on pick up an umbrella, but put on a mask or stay at home.

## Dataset
[COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

## Technologies
- **Google cloud platform** (GCP):
  - VM Instance to run project on it.
  - Cloud Storage to store raw and processed data.
  - BigQuery as data source for dashboard.
- **Terraform** to create cloud infrastructure.
- **Airflow** to run data pipelines as DAGs.
- **PySpark** to transform raw data.
- **Google data studio** to visualize data.

# Results 
## Cloud infrastructure
Except the VM Instance, all project infra setups with terraform: 
- Data Lake for all of the project data.
- BigQuery for transformed data tablels as source for dashboard.

## Data pipelines
The dataset data download, process and upload to cloud storage via Airflow DAGs:
1. **Archive data DAG** 
  - Runs once for the year since 2020 to the actual date — date, when the DAG was triggered. 
  - Download all yearly dataset updates. This task runs by a bash script, which download and compress the data. 
  - Normalize data with PySpark scripts, becouse the schema of the dataset updates have been changed several times since 2020. 
  - Provide actual schema to data.
  - Upload data to project Cloud Storage.
  - Local clean up.
2. **Actual data DAG**
  - Runs daily from actual date. 
  - Download yesterdays dataset update.
  - Provide actual schema to data.
  - Upload data to project Cloud Storage.
  - Local clean up.
3. **Data processing DAG** 
  - Triggered by the ending of the archive data DAG.
  - Runs daily from the actual date.
  - With PySpark script read, transform and rewrite all data from project cloud storage via spark-gcs connector. 
  - Create or replace tables at project BigQuery with partitioning where it's needed. 

## Dashboard
Simple dashboard at Google Data studio with few graphs.
- Histogram of the daily confirmed COVID cases at the choosen city or province for last three month. 
- Heat map of COVID cases.

# How to run project? 
Project was build on GCP Ubuntu VM Instance, so you can find code snippets for these particular case [here](https://github.com/technomonah/CSSE_data_de/blob/main/prereqs-setup.md).

## Prereqs
- Anaconda
- Docker + Docker-compose
- GCP project
- Terraform

## Setup & Deploy
1. Create cloud infrasctructure via Terraform. Look at instructions at [terraform dir](https://github.com/technomonah/CSSE_data_de/tree/main/terraform).
2. Run Airflow in docker and trigger DAGs. Look at instructions at [airflow dir](https://github.com/technomonah/CSSE_data_de/tree/main/airflow).
3. Connect Google Data Studio dashboard to project BigQuery as a source.
