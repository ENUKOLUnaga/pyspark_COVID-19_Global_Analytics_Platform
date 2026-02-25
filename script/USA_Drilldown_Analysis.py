#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum,row_number, col, max as _max,max,min
#USA Drilldown Analysis
#creating spark session
spark=SparkSession.builder\
.appName("USADrilldownAnalysis")\
.master("yarn")\
.getOrCreate()

#reading data from parquet files
df_USA = spark.read.parquet("/data/covid/staging/usa_country_wise")
df_USA.show(10)

#Aggregate County Data to State Level
df_state_level=df_USA.groupBy("Province_state","Date").agg(
    _sum("Confirmed").alias("State_Confirmed"),
    _sum("Deaths").alias("State_Deaths")
)
df_state_level.show(10)

#Identify Top 10 Affected States
latest_date=df_state_level.agg(_max("Date").alias("max_date")).collect()[0]["max_date"]
df_latest=df_state_level.filter(col("Date")==latest_date)
df_top10_states=df_latest.orderBy(col("State_Confirmed").desc()).limit(10)
df_top10_states.show(10)

#Detect Data Skew Across States
df_skew=df_USA.groupBy("Province_state").count() \
.orderBy(col("count").desc())
df_skew.show(10)
#skew ratio 
max_count=df_skew.agg(max("count")).collect()[0][0]
min_count=df_skew.agg(min("count")).collect()[0][0]
skew_ratio=max_count/min_count
print("Data skew ratio:",skew_ratio)

#Explain Skew Impact in Distributed Systems
#Data skew happens when some partitions contain much more data than others.
#Example:
#California: 3000 rows
#Vermont: 20 rows
#->Why is data skew bad?
#Uneven workload → Some executors finish quickly, others take too long.
#Long job duration → Overall job waits for the slowest partition.
#Shuffle overload → Heavy partitions cause large shuffle files.
#OOM errors → Too much data in one executor partition.

#storing result in hdfs
df_state_level.write.mode("overwrite").parquet("/data/covid/analytics/state_level")
df_top10_states.write.mode("overwrite").parquet("/data/covid/analytics/top10_states")
df_skew.write.mode("overwrite").parquet("/data/covid/analytics/state_data_skew")

#stopping spark session
spark.stop()
