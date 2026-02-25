#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
#Creating spark session
spark=SparkSession.builder \
.appName("RDDBasedImplementation") \
.master("yarn") \
.getOrCreate()

#reading data
df=spark.read.parquet("/data/covid/staging/fully_grouped")

#Calculating total confirmed per country.
rdd = df.select("Country_Region", "Confirmed", "Deaths") \
        .rdd.map(lambda r: (r["Country_Region"], (int(r["Confirmed"]), int(r["Deaths"]))))
rdd

#Total Confirmed Cases per Country
rdd_confirmed = rdd.map(lambda x: (x[0], x[1][0])) \
                   .reduceByKey(lambda a, b: a + b)

df_confirmed = rdd_confirmed.toDF(["Country", "Total_Confirmed"])
df_confirmed.show(10)

#Total Deaths per Country
rdd_deaths = rdd.map(lambda x: (x[0], x[1][1])) \
                .reduceByKey(lambda a, b: a + b)

df_deaths = rdd_deaths.toDF(["Country", "Total_Deaths"])
df_deaths.show(10)

#Compute Death Percentage
rdd_joined = rdd_confirmed.join(rdd_deaths)
rdd_death_percentage = rdd_joined.map(
    lambda x: (
        x[0],
        x[1][0],
        x[1][1],
        round((x[1][1] / x[1][0]) * 100, 2) if x[1][0] > 0 else 0
    )
)

df_death_percentage = rdd_death_percentage.toDF(
    ["Country", "Total_Confirmed", "Total_Deaths", "Death_Percentage"]
)
df_death_percentage.show(10)

#storing Output to HDFS
df_confirmed.write.mode("overwrite") \
    .parquet("/data/covid/analytics/rdd_confirmed")
df_deaths.write.mode("overwrite") \
    .parquet("/data/covid/analytics/rdd_deaths")
df_death_percentage.write.mode("overwrite") \
    .parquet("/data/covid/analytics/rdd_death_percentage")

#stopping spark session
spark.stop()
