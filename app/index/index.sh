#!/bin/bash

set -e

# Default input path in HDFS
DEFAULT_INPUT_PATH="/index/data"
INPUT_PATH=${1:-$DEFAULT_INPUT_PATH}
OUTPUT_DIR="/index/output"

echo "APP: Input path: $INPUT_PATH"
echo "APP: Output directory: $OUTPUT_DIR"

# 1: Clear previous output in HDFS
echo "APP: Removing previous output data from HDFS..."
hdfs dfs -rm -r -f "$OUTPUT_DIR" || true

# 2: Check if input is a local file or directory
if [[ -f "$INPUT_PATH" ]]; then
  echo "APP: Detected local file. Uploading to HDFS..."
  LOCAL_FILE="$INPUT_PATH"
  INPUT_PATH="/tmp/index_local_input"
  hdfs dfs -rm -r -f "$INPUT_PATH" || true
  hdfs dfs -mkdir -p "$(dirname "$INPUT_PATH")"
  hdfs dfs -put "$LOCAL_FILE" "$INPUT_PATH"
elif [[ -d "$INPUT_PATH" ]]; then
  echo "APP: Detected local directory. Concatenating files..."
  cat "$INPUT_PATH"/* | hdfs dfs -put - /tmp/index_local_dir/part-00000
  INPUT_PATH="/tmp/index_local_dir"
elif hdfs dfs -test -e "$INPUT_PATH"; then
  echo "APP: Detected HDFS input path."
else
  echo "APP: Error: Input path $INPUT_PATH not found in HDFS or local file system."
  exit 1
fi

# 3: Run MapReduce job
echo "APP: Running MapReduce job to index files..."
mapred streaming \
 -D mapreduce.framework.name=yarn \
 -files mapreduce/mapper1.py,mapreduce/reducer1.py \
 -mapper "python3 mapper1.py" \
 -reducer "python3 reducer1.py" \
 -input "$INPUT_PATH" \
 -output "$OUTPUT_DIR"

# 4: Initialize Cassandra schema
echo "APP: Initializing Cassandra schema..."
python3 index/init_cassandra.py

# 5: Store documents in Cassandra
echo "APP: Storing documents in Cassandra..."
if hdfs dfs -test -d "$INPUT_PATH"; then
  hdfs dfs -cat "$INPUT_PATH"/* | python3 index/store_documents.py
else
  hdfs dfs -cat "$INPUT_PATH" | python3 index/store_documents.py
fi

echo "APP: Storing MapReduce results in Cassandra..."
hdfs dfs -cat "$OUTPUT_DIR/part-*" | python3 index/store_to_cassandra.py

echo "APP: Indexing completed successfully!"