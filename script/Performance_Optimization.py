#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import rand, floor
from pyspark.sql.functions import broadcast
#creating spark session
spark=SparkSession.builder \
.appName("PerformanceOptimization") \
.master("yarn") \
.getOrCreate()

#Performance Optimization
#Partition Strategy
#Repartition by Date
df = spark.read.parquet("/data/covid/staging/covid19_clean_complete")
df_date_partitioned = df.repartition("Date")
df_date_partitioned.write.mode("overwrite").parquet("/data/covid/analytics/partition_by_date")

#Repartition by Country
df_country_partition = df.repartition("Country_Region")
df_country_partition.write.mode("overwrite") \
    .partitionBy("Country_Region") \
    .parquet("/data/covid/optimized/partition_by_country")

#Data Skew Handling
#Identify Skewed Countries
skew = df.groupBy("Country_Region") \
    .count() \
    .orderBy(F.desc("count"))

skew.show(10)

#Fix Skew Using SALTING
df_skew = df.withColumn("salt", floor(rand() * 5))
df_skew.show(10)

#Broadcast Join Optimization
world_df=spark.read.parquet("/data/covid/staging/worldometer_data")
df_bj = df.join(broadcast(world_df), on="Country_Region", how="left")
df_bj.explain("formatted")

#Shuffle Optimization
spark.conf.set("spark.sql.shuffle.partitions", 50)

#Stopping Spark Session
spark.stop()
