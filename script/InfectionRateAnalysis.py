#!/usr/bin/env python
# coding: utf-8

#Infection Rate Analysis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round,avg
#creating session
spark=SparkSession.builder\
.appName("InfectionRateAnalysis")\
.master("yarn")\
.getOrCreate()

#reading data from parquet files
df_world=spark.read.parquet("/data/covid/staging/worldometer_data")
df_world.show(10)

#Confirmed cases per 1000 population.
df_infection=df_world.withColumn(
    "Confirmed_per_1000",
    round(col("Total_Cases")/col("Population")*1000,3)
)
df_infection

#Active cases per 1000 population.
df_infection=df_infection.withColumn(
    "Active_cases_per_1000",
    round(col("Active_Cases")/col("Population")*1000,3)
)
df_infection

#Top 10 countries by infection rate
top_10_infection_rate=df_infection.select(
    "Country_Region","Confirmed_per_1000"
).orderBy(col("Confirmed_per_1000").desc()).limit(10)
top_10_infection_rate.show()

#WHO Region infection ranking
df_region=df_infection.groupBy("WHO_Region").agg(
    round(avg("Confirmed_per_1000"),3).alias("Avg_Confirmed_per_1000")
).orderBy(col("Avg_Confirmed_per_1000").desc())
df_region.show(truncate=False)

#writing infection analysis result in hadoop
df_infection.write.mode("overwrite").parquet("/data/covid/analytics/infection_rates")
top_10_infection_rate.write.mode("overwrite").parquet("/data/covid/analytics/top_10_infection_rate")
df_region.write.mode("overwrite").parquet("/data/covid/analytics/regional_infection_rates")

#stopping spark session
spark.stop()
