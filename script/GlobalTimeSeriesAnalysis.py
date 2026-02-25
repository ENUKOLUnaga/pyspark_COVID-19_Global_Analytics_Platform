#!/usr/bin/env python
# coding: utf-8

#Global Time-Series Analysis
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window
from pyspark.sql.functions import col, mean, stddev, row_number
from pyspark.sql.functions import date_format, sum as _sum, lag
#creating session
spark=SparkSession.builder\
.appName("GlobalTimeSeriesAnalysis")\
.master("yarn")\
.getOrCreate()

#reading data from parquet files
df_day=spark.read.parquet("/data/covid/staging/day_wise")
df_day.show(10)

#Global Daily Average New Cases
df_global_avg=df_day.groupBy("Date").agg(
    avg("New_cases").alias("Average_New_Cases")
).orderBy("Date")
df_global_avg.show(10)

#Detect spike days using Z-score.
w_all=Window.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
df_Zscore=df_day.withColumn("Mean",mean("New_Cases").over(w_all)) \
.withColumn("Std_dev",stddev("New_Cases").over(w_all)) \
.withColumn("Z_score",(col("New_Cases")-col("Mean"))/col("Std_dev")) \
.withColumn("Spike_Flag",(col("Z_score")>2).cast('int'))
df_Zscore.filter("Spike_Flag=1").orderBy(col("Z_score").desc()).show(10)


#Identify Peak Death Date Globally
w=Window.orderBy(col("Deaths").desc())
df_peak_death=df_day.withColumn("rn",row_number().over(w)) \
.filter("rn=1") \
.select("Date","Deaths")
df_peak_death.show()

#Month-over-Month (MoM) Death Growth Rate
df_monthly=df_day.withColumn("YearMonth",date_format("Date","yyyy-mm")) \
.groupBy("YearMonth") \
.agg(_sum("Deaths").alias("Monthly_Deaths")) \
.orderBy("YearMonth")
w_month=Window.orderBy("YearMonth")
df_mom=df_monthly.withColumn(
    "Prev_Month_Deaths",
    lag("Monthly_Deaths").over(w_month)
).withColumn(
    "MoM_Growth_Rate",
    ((col("Monthly_Deaths")-col("Prev_Month_Deaths")) / col("Prev_Month_Deaths"))*100
)
df_mom.show(10)

#writing all results to HDFS
df_global_avg.write.mode("overwrite").parquet("/data/covid/analytics/global_avg_new_cases")
df_Zscore.write.mode("overwrite").parquet("/data/covid/analytics/spike_days")
df_peak_death.write.mode("overwrite").parquet("/data/covid/analytics/peak_death_day")
df_mom.write.mode("overwrite").parquet("/data/covid/analytics/monthly_mom_growth")

#stopping spark session
spark.stop()
