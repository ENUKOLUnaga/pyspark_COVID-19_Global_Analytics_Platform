#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date, when
#creating session
spark=SparkSession.builder\
.appName("CovidDataIngestion")\
.master("yarn")\
.getOrCreate()

#schema for cleaned complete data
schema_covid19_clean_complete = StructType([
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("WHO_Region", StringType(), True)
])

#schema for Country_wise_latest data
schema_country_wise_latest = StructType([
    StructField("Country_Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New_recovered", IntegerType(), True),
    StructField("Deaths_per_100 Cases", DoubleType(), True),
    StructField("Recovered_per_100 Cases", DoubleType(), True),
    StructField("Deaths_per_100 Recovered", DoubleType(), True),
    StructField("Confirmed_last_week", IntegerType(), True),
    StructField("one_week_change", IntegerType(), True),
    StructField("one_week_%_increase", DoubleType(), True),
    StructField("WHO_Region", StringType(), True)
])

#schema for day wise
schema_day_wise = StructType([
    StructField("Date", DateType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New recovered", IntegerType(), True),
    StructField("Deaths_per_100_Cases", DoubleType(), True),
    StructField("Recovered_per_100_Cases", DoubleType(), True),
    StructField("Deaths_per_100_Recovered", DoubleType(), True),
    StructField("Number_of_countries", IntegerType(), True),
])

#schema for Fully_Grouped_Data 
schema_fully_grouped_data = StructType([
    StructField("Date", DateType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New_recovered", IntegerType(), True),
    StructField("WHO_Region", StringType(), True)
])

#schema for usa_country_wise data
schema_usa_country_wise=StructType([
    StructField("UID",LongType(), True),
    StructField("iso2",StringType(), True),
    StructField("iso3",StringType(), True),
    StructField("code3",IntegerType(), True),
    StructField("FIPS",IntegerType(), True),
    StructField("Admin2",StringType(), True),
    StructField("Province_state",StringType(), True),
    StructField("Country_region",StringType(), True),
    StructField("Lat",DoubleType(), True),
    StructField("Long_",DoubleType(), True),
    StructField("Combined_key",StringType(), True),
    StructField("Date",DateType(), True),
    StructField("Confirmed",IntegerType(), True),
    StructField("Deaths",IntegerType(), True)
])

#schema for worldometer_data data
schema_worldometer_data = StructType([
    StructField("Country_Region", StringType(), True),
    StructField("Continent", StringType(), True),
    StructField("Population", LongType(), True),
    StructField("Total_Cases", LongType(), True),
    StructField("New_Cases", LongType(), True),
    StructField("Total_Deaths", LongType(), True),
    StructField("New_Deaths", LongType(), True),
    StructField("Total_Recovered", LongType(), True),
    StructField("New_Recovered", LongType(), True),
    StructField("Active_Cases", LongType(), True),
    StructField("Serious_Critical", LongType(), True),
    StructField("Total_Cases_per_1M_pop", LongType(), True),
    StructField("Deaths_per_1M_pop", LongType(), True),
    StructField("Total_Tests", LongType(), True),
    StructField("Tests_per_1M_pop", LongType(), True),
    StructField("WHO_Region", StringType(), True)
])

#Reading all raw CSV files from HDFS.
#reading csv file covid19_clean_complete
df_covid19_clean_complete=spark.read.csv(
    "/data/covid/raw/covid_19_clean_complete.csv",
    header=True,
    schema=schema_covid19_clean_complete
)

#reading csv file country_wise_latest
df_country_wise_latest=spark.read.csv(
    "/data/covid/raw/country_wise_latest.csv",
    header=True,
    schema=schema_country_wise_latest
)

#reading csv file day_wise
df_day_wise=spark.read.csv(
    "/data/covid/raw/day_wise.csv",
    header=True,
    schema=schema_day_wise
)

#reading csv file fully_grouped_data
df_fully_grouped_data=spark.read.csv(
    "/data/covid/raw/full_grouped.csv",
    header=True,
    schema=schema_fully_grouped_data
)

#reading csv file usa_country_wise
df_usa_country_wise=spark.read.csv(
    "/data/covid/raw/usa_county_wise.csv",
    header=True,
    schema=schema_usa_country_wise
)

#reading csv file worldometer_data
df_worldometer_data=spark.read.csv(
    "/data/covid/raw/worldometer_data.csv",
    header=True,
    schema=schema_worldometer_data
)

#Handling Null Values
#handling null values in df_covid19_clean_complete
df_covid19_clean_complete=df_covid19_clean_complete.fillna({
    'Province_State':"Unknown",
    'Country_Region':"Unknown",
    'Lat':0.0,
    'Long':0.0,
    'Confirmed':0,
    'Deaths':0,
    'Recovered':0,
    'Active':0,
    'WHO_Region':"Unknown"
})
#handling null value in date column
df_covid19_clean_complete = df_covid19_clean_complete.withColumn(
    "Date",
    when(df_covid19_clean_complete.Date.isNull(), current_date())
    .otherwise(df_covid19_clean_complete.Date)
)

#handling null values in df_country_wise_latest
df_country_wise_latest=df_country_wise_latest.fillna({
 'Country_Region':"Unknown",
 'Confirmed':0,
 'Deaths':0,
 'Recovered':0,
 'Active':0,
 'New_cases':0,
 'New_deaths':0,
 'New_recovered':0,
 'Deaths_per_100 Cases':0.0,
 'Recovered_per_100 Cases':0.0,
 'Deaths_per_100 Recovered':0.0,
 'Confirmed_last_week':0,
 'one_week_change':0,
 'one_week_%_increase':0.0,
 'WHO_Region':"Unknown"
})

#handling null value in date column
df_day_wise = df_day_wise.withColumn(
    "Date",
    when(df_day_wise.Date.isNull(), current_date())
    .otherwise(df_day_wise.Date)
)
df_day_wise=df_day_wise.fillna({
 'Confirmed':0,
 'Deaths':0,
 'Recovered':0,
 'Active':0,
 'New_cases':0,
 'New_deaths':0,
 'New recovered':0,
 'Deaths_per_100_Cases':0.0,
 'Recovered_per_100_Cases':0.0,
 'Deaths_per_100_Recovered':0.0,
 'Number_of_countries':0
})

#handling null values in df_fully_grouped_data
df_fully_grouped_data=df_fully_grouped_data.withColumn(
    "Date",
    when(df_fully_grouped_data.Date.isNull(), current_date())
    .otherwise(df_fully_grouped_data.Date))
df_fully_grouped_data=df_fully_grouped_data.fillna({
 'Country_region':"Unknown",
 'Confirmed':0,
 'Deaths':0,
 'Recovered':0,
 'Active':0,
 'New_cases':0,
 'New_deaths':0,
 'New_recovered':0,
 'WHO_Region':"Unknown"
})

#handling null values in df_usa_country_wise
df_usa_country_wise=df_usa_country_wise.fillna({
 'UID':0,
 'iso2':"Unknown",
 'iso3':"Unknown",
 'code3':0,
 'FIPS':0,
 'Admin2':"Unknown",
 'Province_state':"Unknown",
 'Country_region':"Unknown",
 'Lat':0.0,
 'Long_':0.0,
 'Combined_key':"Unknown",
 'Confirmed':0,
 'Deaths':0
})
df_usa_country_wise=df_usa_country_wise.withColumn(
    "Date",
    when(df_usa_country_wise.Date.isNull(),current_date())
    .otherwise(df_usa_country_wise.Date))
df_usa_country_wise

#Handling null values df_worldometer_data
df_worldometer_data=df_worldometer_data.fillna({
 'Country_Region':"Unknown",
 'Continent':"Unknown",
 'Population':0,
 'Total_Cases':0,
 'New_Cases':0,
 'Total_Deaths':0,
 'New_Deaths':0,
 'Total_Recovered':0,
 'New_Recovered':0,
 'Active_Cases':0,
 'Serious_Critical':0,
 'Total_Cases_per_1M_pop':0,
 'Deaths_per_1M_pop':0,
 'Total_Tests':0,
 'Tests_per_1M_pop':0,
 'WHO_Region':"Unknown"
})
df_worldometer_data

#Write Data to Parquet Format
df_covid19_clean_complete.write.mode("overwrite").parquet("/data/covid/staging/covid19_clean_complete")
df_country_wise_latest.write.mode("overwrite").parquet("/data/covid/staging/country_wise_latest")
df_day_wise.write.mode("overwrite").parquet("/data/covid/staging/day_wise")
df_fully_grouped_data.write.mode("overwrite").parquet("/data/covid/staging/fully_grouped")
df_worldometer_data.write.mode("overwrite").parquet("/data/covid/staging/worldometer_data")
df_usa_country_wise.write.mode("overwrite").parquet("/data/covid/staging/usa_country_wise")

#stopping spark session
spark.stop()
