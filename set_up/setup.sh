#!/bin/bash

echo "===== COVID-19 ANALYTICS PLATFORM SETUP ====="

echo "Step 1: Creating HDFS directory structure..."

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/curated
hdfs dfs -mkdir -p /data/covid/analytics

echo "HDFS directories created."

echo "Step 2: Uploading CSV files to /data/covid/raw ..."
LOCAL_DATA_DIR="/mnt/e/covid_data"

if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "ERROR: Local directory $LOCAL_DATA_DIR does not exist."
    echo "Fix the path and re-run."
    exit 1
fi

echo "Uploading files from: $LOCAL_DATA_DIR"

hdfs dfs -put -f $LOCAL_DATA_DIR/*.csv /data/covid/raw/

echo "Upload complete."

echo "Step 3: Verifying upload..."

hdfs dfs -ls /data/covid/raw

echo "===== SETUP COMPLETED SUCCESSFULLY ====="
