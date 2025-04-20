#!/bin/bash

source .venv/bin/activate
#set -e  # Exit on any command failure


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this
# hdfs dfs -rm -r /data || true
hdfs dfs -rm -r /index/data || true
# hdfs dfs -rm -r /tmp/index-output || true

hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "done data preparation!"