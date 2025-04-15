set -e 

export PYSPARK_DRIVER_PYTHON=$(which python)

# Clear HDFS directories before running
echo "APP: Clearing HDFS directories..."
hdfs dfs -rm -r -f /data /index/data || echo "APP: Directories not found, skipping removal."

echo "APP: Load a.parquet"
hdfs dfs -put -f a.parquet /  > /dev/null 2>&1

echo "APP: run prepare_data.py"
spark-submit prepare_data.py # > /dev/null 2>&1

echo "APP: Putting sampled data to hdfs"
hdfs dfs -put data / > /dev/null 2>&1

echo "APP: Prepare data for indexing "
spark-submit prepare_data_indexing.py > /dev/null 2>&1

echo "APP: done data preparation!"
