#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
#creating spark session
spark=SparkSession.builder \
.appName("ExecutionPlanAnalysis") \
.master("yarn") \
.getOrCreate()

#Execution Plan Analysis
#Aggregation (Global Confirmed Cases by Country)
df = spark.read.parquet("/data/covid/staging/covid19_clean_complete")
df_aggregation=df.groupBy("Country_Region") \
.agg(_sum("Confirmed").alias("Total_Confirmed"))
df_aggregation.explain("extended")

#WholeStageCodegen
"""HashAggregate(keys=[Country_Region#1], functions=[sum(Confirmed#5)], output=[Country_Region#1, Total_Confirmed#31L])"""
#Exchange
"""Exchange hashpartitioning(Country_Region#1, 200), [plan_id=11]"""

#Broadcast Join
from pyspark.sql.functions import broadcast
world_df=spark.read.parquet("/data/covid/staging/worldometer_data")
df_join=df.join(
    broadcast(world_df),
    on="Country_Region",
    how="left"
)
df_join.explain("extended")
#output
#Broadcast Hashjoin & exchange
"""BroadcastHashJoin [Country_Region#1], [Country_Region#127], LeftOuter, BuildRight, false"""
"""BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=35]"""

#Natural Join
df_sorted_join=df.join(
    df_aggregation,
    on="Country_Region",
    how="inner"
)
df_sorted_join.explain("extended")
#output
#exchange
"""Exchange hashpartitioning(Country_Region#186, 200), [plan_id=73]"""

#stopping spark session
spark.stop()

