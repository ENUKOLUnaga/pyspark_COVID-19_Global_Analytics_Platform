#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
#creating spark session
spark=SparkSession.builder \
.appName("SparkSQLImplementation") \
.master("yarn") \
.getOrCreate()

#Load Data & Create Temporary View
df = spark.read.parquet("/data/covid/staging/fully_grouped")
df.createOrReplaceTempView("covid")

#Load Data & Create Temporary View
df_top10_infection = spark.sql("""
SELECT 
    Country_Region,
    SUM(Confirmed) AS Total_Confirmed
FROM covid
GROUP BY Country_Region
ORDER BY Total_Confirmed DESC
LIMIT 10
""")
df_top10_infection.show()

#Death Percentage Ranking (SQL)
df_death_percentage_sql = spark.sql("""
SELECT
    Country_Region,
    SUM(Confirmed) AS Total_Confirmed,
    SUM(Deaths) AS Total_Deaths,
    ROUND(SUM(Deaths) / SUM(Confirmed) * 100, 2) AS Death_Percentage
FROM covid
GROUP BY Country_Region
HAVING SUM(Confirmed) > 0
ORDER BY Death_Percentage DESC
""")
df_death_percentage_sql.show()

#Rolling 7-Day Average (SQL Window Function)
df_rolling = spark.sql("""
SELECT
    Country_Region,
    Date,
    Confirmed,
    AVG(Confirmed) OVER (
        PARTITION BY Country_Region
        ORDER BY Date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS Rolling_7day_Avg
FROM covid
ORDER BY Country_Region, Date
""")
df_rolling.show()

#Comparing Physical Plans with DataFrame API
from pyspark.sql import functions as F
spark.sql("""
SELECT 
    Country_Region,
    SUM(Confirmed) AS Total_Confirmed
FROM covid
GROUP BY Country_Region
""").explain(True)

df.groupBy("Country_Region") \
  .agg(F.sum("Confirmed").alias("Total_Confirmed")) \
  .explain(True)

#storing Output to HDFS
df_top10_infection.write.mode("overwrite").parquet("/data/covid/analytics/sql_top10_infection")
df_death_percentage_sql.write.mode("overwrite").parquet("/data/covid/analytics/sql_death_percentage")
df_rolling.write.mode("overwrite").parquet("/data/covid/analytics/sql_rolling_7day")

#Stopping spark session
spark.stop()
