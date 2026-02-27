# COVID-19 Global Analytics Platform
Distributed Big Data Processing using Apache Spark & Hadoop (HDFS + YARN)
## Project Overview
 This project implements a fully distributed analytics pipeline for global COVID-19 data using:
 - Apache Hadoop (HDFS + YARN)
 - Apache Spark (PySpark, Spark SQL, RDD)
 - Local pseudo-distributed Hadoop cluster
 - Optimized Parquet storage
 - Performance tuning, partitioning, broadcast joins, skew handling, caching

The goal is to design an end-to-end COVID data analytics platform built with scalable big-data technologies, following industry-level data engineering practices.

## Tech Stack
- Apache Hadoop (HDFS, YARN)
- Apache Spark 3.x
- PySpark
- YARN Cluster Mode
- WSL 

Kaggle COVID-19 datasets - https://www.kaggle.com/datasets/imdevskp/corona-virus-report?

## Dataset Description
All datasets are from the Kaggle COVID-19 report collection.
Files used:
- Table 1 – Time-series & case-level data
  - full_grouped.csv
  - covid_19_clean_complete.csv

- Table 2 – Country-level summary
   - country_wise_latest.csv
   - day_wise.csv

- Table 3 – USA county drilldown
  - usa_county_wise.csv

- Table 4 – Worldometer dataset
  - worldometer_data.csv

## Tasks Implemented
### Task 1 – Hadoop Integration
- Created HDFS directory structure
- Uploaded raw data
- Validated with HDFS commands
- All Spark jobs read/write only through HDFS
- /data/covid/raw
- /data/covid/staging
- /data/covid/curated
- /data/covid/analytics

### Task 2 – Data Ingestion & Optimization
- Read CSV from HDFS with defined schema
- Null handling
- Converted to Parquet
- Stored in /data/covid/staging
- Compared CSV vs Parquet for:
  - File size
  - Read speed
  - Execution plan differences

### Task 3 – Death Percentage Analysis
- Daily death % per country
- Global death trend
- Continent-level death % (with joins)
- Top 10 countries by deaths per capita
- Output stored in /data/covid/analytics

### Task 4 – Infection Rate Analysis
- Cases per 1000 population
- Active cases per 1000
- Top 10 infected countries
- WHO region infection ranking

### Task 5 – Recovery Analysis
- Recovery % per country
-  7-day rolling average using window functions
-  Fastest recovery growth
-  Peak recovery day per country

### Task 6 – Global Time-Series Analysis
- Global average new cases
- Spike detection using Z-Score
- Peak global death date
- Month-over-month death growth

### Task 7 – USA Drilldown Analysis
- County to state aggregation
- Top 10 affected states
- Skew detection for large states
- Explanation of skew impacts (shuffle pressure, slow tasks)

### Task 8 – RDD-Based Implementation
- Total confirmed per country
- Total deaths per country
- Death percentage using reduceByKey
- RDD vs DataFrame performance comparison
- Why reduceByKey > groupByKey
  - Less shuffle volume
  - Combiner optimization

### Task 9 – Spark SQL Implementation
- Temporary views
- SQL for:
  - Top 10 infected countries
  - Death % ranking
  - 7-day rolling averages
- Compared SQL vs DataFrame physical plan

Task 10 – Performance Optimization (Mandatory)
- Partition Strategy
  - Repartition by Date / Country
  - Difference explained:
    - repartition() – shuffle, increases partitions
    - coalesce() – shrink partitions, no shuffle
  - Applied partitioning before writing Parquet.
- Data Skew Handling
  - Identified skewed keys (e.g., USA)
  - Implemented salting or skew join techniques
  - Explained shuffle impact
- Broadcast Join
  - Used broadcast for smaller dataset join
  - Verified via BroadcastHashJoin in explain plan
- Shuffle Optimization
  - Tuned spark.sql.shuffle.partitions
  - Avoided wide transformations
  - Merged filters before joins
- Caching Strategy
  - Cached frequently reused DataFrames
  - Used MEMORY_AND_DISK persist
  - Explained when caching hurts performance (OOM, recompute cost)

### Task 11 – Execution Plan Analysis
- For major queries, analyzed:
  - Exchange (shuffle boundaries)
  - BroadcastHashJoin
  - SortMergeJoin
  - WholeStageCodegen

### Task 12 – Resource & Memory Planning
- Executor memory breakdown
- Executor cores strategy
- Ideal partition size rule: 128–256 MB
- YARN container allocation
- OOM behavior
- Spill-to-disk explanation

### How to Run the Project
1. Start HDFS & YARN
start-dfs.sh
start-yarn.sh
2. Submit Spark Job
spark-submit --master yarn --deploy-mode cluster scripts/ingestion.py
3. View HDFS Output
hdfs dfs -ls /data/covid/analytics

## Final Summary
  This project showcases a complete end-to-end distributed analytics pipeline using Spark + Hadoop, covering ingestion, storage optimization, analytics, SQL, RDDs, performance tuning, and cluster-level execution planning.
An ideal real-world style project for Data Engineering portfolios.
