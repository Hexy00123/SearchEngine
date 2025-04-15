#!/bin/bash

set -e

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python) 
export PYSPARK_PYTHON=./.venv/bin/python

QUERY="$1"
echo "APP: Running query: $QUERY"

unset PYSPARK_DRIVER_PYTHON
spark-submit --master yarn \
  --conf "spark.pyspark.python=/app/.venv/bin/python" \
  --conf "spark.pyspark.driver.python=/app/.venv/bin/python" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  --deploy-mode cluster \
  --archives /app/.venv.tar.gz#.venv \
  /app/query.py "$QUERY" > /dev/null 2>&1

if hdfs dfs -test -e /query/part-*.json; then
    hdfs dfs -cat /query/part-*.json
else
    echo "Nothing retrieved"
fi