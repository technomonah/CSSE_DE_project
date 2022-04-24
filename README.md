## What this project about? 
Here I've tried to build batch pipeline with to process COVID data from data repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University. The goal was to build a dasboard for monitoring confirmed COVID cases in the world, where user can choose city and look at current incidence rate.

Something like a weather checker, useful not to decide on pick up a umbrella, but put on a mask or stay at home.

### Dataset
- COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University (https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

### Technologies
- **Google cloud platform** (GCP)
  - VM Instance to run project on it
  - Cloud storage to store raw and processed data
  - BigQuery to create tables which to use as data source for dashboard 
- **Terraform** to create cloud infrastricture on GCP
- **Airflow** to run data pipelines 
- **PySpark** to code transform data scripts 
- **docker-compose** to run Airflow with all dependencies
- **Google data studio** to visualize data 

### Results 
Pipeline process all archive data from dataset and schedule daily updates: 
- All data was standartized (schema of dataset reports have multiple changes since 2020)
- Dataset have only cumulative sum of confirmed covid cases, so daily cases were calculated as difference between close date reports
- Seems like dataset have mistakes, when cumulative sum at report was lower then in previouos. Those rows are filtered from datset in project and dataset team is informed about problem (https://github.com/CSSEGISandData/COVID-19/issues/5671)
 
On the exit pipeline creates:
- Two tables with precalculated data for visualization to reduce cost of using BigQuery
- One partitioned by date table with all data for query experiments

Dashboard for project: https://datastudio.google.com/reporting/9bfe705c-bc0f-4b00-8211-e1cca72d1f0c/page/910qC
