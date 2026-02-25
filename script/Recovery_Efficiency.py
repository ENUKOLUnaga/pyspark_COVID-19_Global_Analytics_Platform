#!/usr/bin/env python
# coding: utf-8

#Recovery Efficiency
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg,col,when,lag,row_number
#creating session
spark=SparkSession.builder\
.appName("RecoveryEfficiency")\
.master("yarn")\
.getOrCreate()

df_fully_grouped=spark.read.parquet("/data/covid/staging/fully_grouped")
#Recovered Percentage per Country
df_recovery_percentage=df_fully_grouped.withColumn(
    "Recovery_Percentage",
    when(col("Confirmed")>0,(col("Recovered")/col("Confirmed"))*100)
    .otherwise(0)
)
df_recovery_percentage.show(10)

#7-Day Rolling Recovery Average
w7=(Window.partitionBy("Country_Region")
    .orderBy("Date")
    .rowsBetween(-6,0)
   )
df_rolling_recovery=df_recovery_percentage.withColumn(
    "Recovery_7day_Average",
    avg("Recovery_Percentage").over(w7)
)
df_rolling_recovery.show(10)

#Country with fastest recovery growth.
w=Window.partitionBy("Country_Region").orderBy("Date")
df_growth=df_rolling_recovery.withColumn(
    "Prev_Recovery_pct",
    lag("Recovery_Percentage",1).over(w)
).withColumn(
    "Recovery_Growth",
    col("Recovery_Percentage")-col("Prev_Recovery_pct")
)
df_fastest_growth=df_growth.groupBy("Country_Region") \
.agg({"Recovery_Growth":"max"}) \
.withColumnRenamed("max(Recovery_Growth)","Max_Recovery_Growth") \
.orderBy(col("Max_Recovery_Growth").desc())
df_fastest_growth.show()

#Peak Recovery Day per Country
wmax=(
    Window.partitionBy("Country_Region")
    .orderBy(col("Recovered").desc())
)
df_peak_day=df_fully_grouped.withColumn(
    "rn",
    row_number().over(wmax)
).filter(col("rn")==1).select(
    "Country_Region","Date","Recovered"
)
df_peak_day.show(10)

#writing infection analysis result in hadoop
df_recovery_percentage.write.mode("overwrite").parquet("/data/covid/analytics/recovery_Percentages")
df_rolling_recovery.write.mode("overwrite").parquet("/data/covid/analytics/rolling_recovery_7days")
df_fastest_growth.write.mode("overwrite").parquet("/data/covid/analytics/fastest_growth")
df_peak_day.write.mode("overwrite").parquet("/data/covid/analytics/peak_day")

#stopping spark session
spark.stop()
