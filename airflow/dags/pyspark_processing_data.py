#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.sql.window import Window
import pyspark.sql.functions as f

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/opt/spark/jars/gcs-connector-hadoop2-2.1.1.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")

sc = SparkContext(conf=conf)

sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/.google/credentials/google_credentials.json")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

def create_rewrite_datasests():
    df = spark.read.parquet("gs://csse_data_lake_iron-figure-338711/raw/*")
    df = df.select("Province_State","Country_Region","Last_Update",
        "Confirmed","Deaths",
        f.concat_ws(',',df.Lat,df.Long_).alias("Coordinates"),
        "Incident_Rate","Case_Fatality_Ratio") \
        .withColumn('Province_State', f.coalesce(df.Province_State, df.Country_Region)) \
        .na.drop(subset=["Last_Update"])

    window = Window.partitionBy("Coordinates").orderBy("Last_Update")
    df = df.withColumn("Diff", f.col("Confirmed") - f.lag(f.col("Confirmed"), 1, 0).over(window))
    df.registerTempTable('csse_data')
    
    monthly_cases_grouped = spark.sql("""
    SELECT
        Coordinates,
        Province_State,
        SUM(Diff) as monthly_cases
    FROM
        csse_data
    WHERE
        DATE(Last_Update) >= DATE_ADD(CURRENT_DATE(), -180)
    AND Diff > 0
    GROUP BY
        Coordinates, Province_State
    """)
    
    monthly_cases_grouped \
        .coalesce(1) \
        .write.parquet('gs://csse_data_lake_iron-figure-338711/clean/monthly_cases_grouped',mode='overwrite')
    
    cases_last_3m = spark.sql("""
    SELECT
        Coordinates,
        Province_State,
        DATE(Last_Update) AS Date,
        Diff AS Cases,
        Incident_Rate
    FROM
        csse_data
    WHERE
        DATE(Last_Update) >= DATE_ADD(CURRENT_DATE(), -180)
    AND
        Diff > 0    
    """)
    cases_last_3m \
        .coalesce(1) \
        .write.parquet('gs://csse_data_lake_iron-figure-338711/clean/cases_last_3m',mode='overwrite')
    
    df \
        .coalesce(1) \
        .write.parquet('gs://csse_data_lake_iron-figure-338711/clean/fulldataset',mode='overwrite')