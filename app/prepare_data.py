from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os

os.makedirs("data", exist_ok=True)

print("APP: Creating spark session...")
spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.parquet.columnarReaderBatchSize", "1024") \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")

print("APP: Reading & sampling file...")
df = spark.read.parquet("/a.parquet")

n = 100
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)
print(f'APP: Sampled {df.count()} rows')

def create_doc(row):
    try:
        if row['id'] is None or row['title'] is None or row['text'] is None:
            print(f"APP: Skipping row with missing fields: {row}")
            return
        filename = sanitize_filename(str(row['id']) + "_" + str(row['title'])).replace(" ", "_") + ".txt"
        with open("data/" + filename, "w", encoding='utf-8') as f:
            doc_id = row['id']
            doc_title = row['title']
            content = ' '.join(row['text'].split())
            f.write(f"{doc_id}\t{doc_title}\t{content.strip()}")
    except Exception as e:
        print(f"APP: Error writing file for row {row}: {e}")
        
print("APP: Creating documents...")
df.foreach(create_doc)
print("APP: Created!")
