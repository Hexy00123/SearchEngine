import os 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Preparation for MapReduce") \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
    
print("APP: Reading documents from /data...")
rdd = spark.sparkContext.wholeTextFiles("/data")

def process_file(file):
    file_path, content = file
    content = ' '.join(content.split())
    file_name = os.path.basename(file_path)
    doc_id, doc_title = file_name.split("_", 1)
    doc_title = doc_title.replace(".txt", "").replace("_", " ")
    return f"{doc_id}\t{doc_title}\t{content.strip()}"
  
print("APP: Transforming RDD...")
transformed_rdd = rdd.map(process_file)
print("APP: Saving transformed RDD to HDFS...")
transformed_rdd.coalesce(1).saveAsTextFile("/index/data")
print("APP: Data preparation completed!")