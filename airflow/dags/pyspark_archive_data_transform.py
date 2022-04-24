#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, DateType, IntegerType, DoubleType
from pyspark.sql import types
import pandas as pd


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

csse_data_schema = types.StructType([
    types.StructField("Province_State", types.StringType(), True),
    types.StructField("Country_Region", types.StringType(), True),
    types.StructField("Last_Update", types.TimestampType(), True),
    types.StructField("Confirmed", types.IntegerType(), True),
    types.StructField("Deaths", types.IntegerType(), True),
    types.StructField("Lat", types.DoubleType(), True),
    types.StructField("Long_", types.DoubleType(), True),
    types.StructField("Incident_Rate", types.DoubleType(), True),
    types.StructField("Case_Fatality_Ratio", types.DoubleType(), True),
])

# Function for reading, normalizing and rewriting 2020s data
def archive_data_2020(path_to_data,archive_year):
    
    #Declare input and output path for data
    input_path = path_to_data +'/raw/'+ archive_year
    output_path = path_to_data +'/clean/'+ archive_year
    
    #Read, normalize and rewrite data from 01-22-2020 to 02-29-2020
    for date in pd.date_range(start="01-22-2020",end="02-29-2020"):
        
        fdate = date.strftime('%m-%d-%Y')

        df = spark.read \
            .option("header","true") \
            .csv(f'{input_path}/{fdate}.csv.gz')

        df = df.select('Province/State','Country/Region','Last Update',
            'Confirmed','Deaths') \
            .withColumnRenamed('Province/State','Province_State') \
            .withColumnRenamed('Country/Region','Country_Region') \
            .withColumnRenamed('Last Update','Last_Update') \
            .withColumn('Lat', lit(None).cast(StringType())) \
            .withColumn('Long_', lit(None).cast(StringType())) \
            .withColumn('Incident_Rate', lit(None).cast(StringType())) \
            .withColumn('Case_Fatality_Ratio', lit(None).cast(StringType())) 

        df.write.csv(f'{output_path}',mode='append',header='true')

    #Read, normalize and rewrite data from 03-01-2020 to 03-21-2020
    for date in pd.date_range(start="03-01-2020",end="03-21-2020"):
        fdate = date.strftime('%m-%d-%Y')

        df = spark.read \
            .option("header","true") \
            .csv(f'{input_path}/{fdate}.csv.gz')

        df = df.select('Province/State','Country/Region','Last Update',
            'Confirmed','Deaths','Latitude','Longitude') \
            .withColumnRenamed('Province/State','Province_State') \
            .withColumnRenamed('Country/Region','Country_Region') \
            .withColumnRenamed('Last Update','Last_Update') \
            .withColumnRenamed('Latitude','Lat') \
            .withColumnRenamed('Longitude','Long_') \
            .withColumn('Incident_Rate', lit(None).cast(StringType())) \
            .withColumn('Case_Fatality_Ratio', lit(None).cast(StringType())) 

        df.write.csv(f'{output_path}',mode='append',header='true')

    #Read, normalize and rewrite data from 03-22-2020 to 05-28-2020
    for date in pd.date_range(start="03-22-2020",end="05-28-2020"):
        fdate = date.strftime('%m-%d-%Y')

        df = spark.read \
            .option("header","true") \
            .csv(f'{input_path}/{fdate}.csv.gz')

        df = df.select('Province_State','Country_Region','Last_Update',
            'Confirmed','Deaths','Lat','Long_') \
            .withColumn('Incident_Rate', lit(None).cast(StringType())) \
            .withColumn('Case_Fatality_Ratio', lit(None).cast(StringType())) 

        df.write.csv(f'{output_path}',mode='append',header='true') 


    #Read, normalize and rewrite data from 05-29-2020 to 11-08-2020
    for date in pd.date_range(start="05-29-2020",end="11-08-2020"):
        fdate = date.strftime('%m-%d-%Y')

        df = spark.read \
            .option("header","true") \
            .csv(f'{input_path}/{fdate}.csv.gz')    

        df = df.select('Province_State','Country_Region','Last_Update',
            'Confirmed','Deaths','Lat','Long_','Incidence_Rate','Case-Fatality_Ratio') \
            .withColumnRenamed('Case-Fatality_Ratio','Case_Fatality_Ratio') \
            .withColumnRenamed('Incidence_Rate','Incident_Rate')

        df.write.csv(f'{output_path}',mode='append',header='true') 

    #Read, normalize and rewrite data from 11-09-2020 to 12-31-2020
    for date in pd.date_range(start="11-09-2020",end="12-31-2020"):
        fdate = date.strftime('%m-%d-%Y')

        df = spark.read \
            .option("header","true") \
            .csv(f'{input_path}/{fdate}.csv.gz')   

        df = df.select('Province_State','Country_Region','Last_Update',
            'Confirmed','Deaths','Lat','Long_','Incident_Rate','Case_Fatality_Ratio')

        df.write.csv(f'{output_path}',mode='append',header='true') 
    
    df = spark.read \
        .option("header","true") \
        .csv(f'{output_path}/*')
    rowcount = df.count()
    return print(f'{archive_year} data normalization completed. Total row count in dataset: {rowcount}')


# Function for reading, normalizing and rewrting 2021s and futher data 
def archive_data_latest(path_to_data,archive_year):
    
    #Declare input and output path for data
    input_path = path_to_data +'/raw/'+ archive_year
    output_path = path_to_data +'/clean/'+ archive_year
    
    df = spark.read \
        .option("header","true") \
        .csv(f'{input_path}/*')
         
    df = df.select('Province_State','Country_Region','Last_Update',
            'Confirmed','Deaths','Lat','Long_','Incident_Rate','Case_Fatality_Ratio')

    df.write.csv(f'{output_path}',mode='append',header='true')

    df = spark.read \
        .option("header","true") \
        .csv(f'{output_path}/*')
    rowcount = df.count()
    return print(f'{archive_year} data normalization completed. Total row count in dataset: {rowcount}')
    
# Function for choosing the way of archive data normalization (2020s or latest)
def archive_data_standartize(path_to_data,year): 
    if int(year) < 2021:
        print(f'Normalizing and rewriting data for {year}')
        archive_data_2020(f'{path_to_data}',f'{year}')
    else:
        print(f'Normalizing and rewriting data for {year}')        
        archive_data_latest(f'{path_to_data}',f'{year}')
        

def archive_data_reschema(path_to_data, year):
    
    csse_data_schema = types.StructType([
    types.StructField("Province_State", types.StringType(), True),
    types.StructField("Country_Region", types.StringType(), True),
    types.StructField("Last_Update", types.TimestampType(), True),
    types.StructField("Confirmed", types.IntegerType(), True),
    types.StructField("Deaths", types.IntegerType(), True),
    types.StructField("Lat", types.DoubleType(), True),
    types.StructField("Long_", types.DoubleType(), True),
    types.StructField("Incident_Rate", types.DoubleType(), True),
    types.StructField("Case_Fatality_Ratio", types.DoubleType(), True),
    ])

    input_path = path_to_data + '/clean/' + year
    output_path = path_to_data + '/pq/' + year
    
    df = spark.read \
        .option("header","true") \
        .schema(csse_data_schema) \
        .csv(f'{input_path}')
    
    df.write.parquet(f'{output_path}',mode='append')