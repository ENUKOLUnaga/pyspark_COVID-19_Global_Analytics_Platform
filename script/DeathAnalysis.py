#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import sum as _sum
import pyspark.sql.functions as F
#creating session
spark=SparkSession.builder \
.appName("DeathAnalysis") \
.master("yarn") \
.getOrCreate()

#Compute daily death percentage per country by using full_grouped.csv
df_full = spark.read.parquet("/data/covid/staging/fully_grouped")
df_world=spark.read.parquet("/data/covid/staging/worldometer_data")
df_death_percentage_per_country=df_full.withColumn(
    "Death_Percentage",
    when(col("Confirmed")>0,((col("Deaths")/col("Confirmed"))*100)).otherwise(0)
)
df_death_percentage_per_country.show(10)

#Compute Global Daily Death Percentage
df_global_death_percentage=df_full.groupBy("Date").agg(
    (_sum("Deaths")/_sum("Confirmed")*100).alias("Global_Death_Percentage")
)
df_global_death_percentage.show(10)

#Compute Continent-Wise Death Percentage
df_join=df_death_percentage_per_country.join(
    df_world.select("Country_Region","Continent"),
    df_death_percentage_per_country.Country_region==df_world.Country_Region,
    "left"
)
df_continent=df_join.groupBy("Date","Continent").agg(
    (_sum("Deaths")/_sum("Confirmed")*100).alias("Continenet_Death_Percentage")
)
df_continent.show(10)

#Country With Highest Death Percentage
latest_date = df_death_percentage_per_country.agg(F.max("Date")).collect()[0][0]
df_latest = df_death_percentage_per_country.filter(col("Date") == latest_date)

df_highest_death_percentage =df_latest.orderBy(col("Death_Percentage").desc()).limit(1)
df_highest_death_percentage_top10 =df_latest.orderBy(col("Death_Percentage").desc()).limit(10)

df_highest_death_percentage.show(10)
df_highest_death_percentage_top10.show()

#Writing Results to HDFS under /data/covid/analytics
df_death_percentage_per_country.write.mode("overwrite").parquet("/data/covid/analytics/country_daily_death_percentage")
df_global_death_percentage.write.mode("overwrite").parquet("/data/covid/analytics/global_daily_death_percentage")
df_continent.write.mode("overwrite").parquet("/data/covid/analytics/continent_death_percentage")
df_highest_death_percentage.write.mode("overwrite").parquet("/data/covid/analytics/highest_death_percentage_country")
df_highest_death_percentage_top10.write.mode("overwrite").parquet("/data/covid/analytics/highest_death_percentage_country_top1o")

#stopping spark session
spark.stop()