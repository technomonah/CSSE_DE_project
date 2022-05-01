## What this project about? 
It's a learning project at data-engineering-zoomcamp course by [DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp)

Here I've tried to build batch pipeline with to process COVID data from data repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University. The goal was to build a dasboard for monitoring confirmed COVID cases in the world, where user can choose city and look at current incidence rate.

Something like a weather checker, useful not to decide on pick up a umbrella, but put on a mask or stay at home.

### Dataset
[COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

### Technologies
- **Google cloud platform** (GCP)
  - VM Instance to run project on it
  - Cloud Storage to store raw and processed data
  - BigQuery to create tables which to use as data source for dashboard 
- **Terraform** to create infrastricture for project on GCP
- **Airflow** to run data pipelines 
- **PySpark** to code transform data scripts
- **Google data studio** to visualize data 

### Results 
Pipeline process all archive data from dataset and schedule daily updates: 
- All data was standartized (schema of dataset reports had multiple changes since 2020)
- Dataset have only cumulative sum of confirmed covid cases, so daily cases were calculated as difference between close date reports

On the exit pipeline creates:
- Two tables with precalculated data for visualization to reduce cost of using BigQuery
- One partitioned by date table with all data for query experiments

Dashboard for project: https://datastudio.google.com/reporting/9bfe705c-bc0f-4b00-8211-e1cca72d1f0c/page/910qC

## How to run project? 

### Prereqs
- Anaconda
- Docker + Docker-compose
- GCP project
- Terraform

I've builded project on GCP Ubuntu VM, so you can find code snippets for these particular case [here](https://github.com/technomonah/CSSE_data_de/blob/main/prereqs-setup.md).

### Deploy
1. Create cloud infrasctructure via Terraform. Look at instructions at [terraform dir](https://github.com/technomonah/CSSE_data_de/tree/main/terraform)
2. Run Airflow in docker and trigger DAGs. Look at instructions at [airflow dir](https://github.com/technomonah/CSSE_data_de/tree/main/airflow)

