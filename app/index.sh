#!/bin/bash

INPUT_PATH=${1:-/index/data}
OUTPUT_PATH="/tmp/index-output"

hdfs dfs -rm -r $OUTPUT_PATH

# Run MapReduce streaming job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -input $INPUT_PATH \
    -output $OUTPUT_PATH \
    -mapper mapper1.py \
    -reducer reducer1.py \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

# Copy output to local for storing into Cassandra
hdfs dfs -getmerge $OUTPUT_PATH reducer_output.txt

# Store to Cassandra
python3 store_to_cassandra.py
