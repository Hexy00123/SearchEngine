import sys
import math
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def compute_bm25(term_doc_pair):
    term, (doc_id, tf) = term_doc_pair
    if term not in bc_query.value or term not in bc_df.value:
        return None
    df = bc_df.value[term]
    idf = math.log(1 + (bc_N.value - df + 0.5) / (df + 0.5))
    return (doc_id, (term, tf, idf))

def bm25_score(doc_id_terms):
    doc_id, terms = doc_id_terms
    if doc_id not in bc_doc_info.value:
        return None
    title, dl = bc_doc_info.value[doc_id]
    score = 0.0
    for term, tf, idf in terms:
        denom = tf + k1 * (1 - b + b * dl / bc_avgdl.value)
        score += idf * tf * (k1 + 1) / denom
    return (score, doc_id, title)

def log_error_to_hdfs(spark, error_message):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    error_path = f"/query/errors/error_{timestamp}.log"
    error_df = spark.createDataFrame([(timestamp, error_message)], ["timestamp", "error"])
    error_df.write.mode("overwrite").json(error_path)
    print(f"APP: Error written to {error_path}")

try: 
    k1 = 1.5
    b = 0.75

    query = sys.argv[1].lower().split()

    print(f'APP: Creating Spark session for query: {query}')
    spark = SparkSession.builder \
        .appName("BM25Ranker") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    sc = spark.sparkContext
    print('APP: Spark session created')

    collection_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="collection_stats", keyspace="bigdata") \
        .load() \
        .filter(col("key") == "global") \
        .limit(1) \
        .collect()[0]

    N = collection_df.total_docs
    avg_dl = collection_df.avg_doc_length
    print(f'APP: Global stats - N={N}, avg_dl={avg_dl}')

    bc_query = sc.broadcast(query)
    bc_N = sc.broadcast(N)
    bc_avgdl = sc.broadcast(avg_dl)

    print('APP: Reading Cassandra tables...')
    vocab_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vocabulary_index", keyspace="bigdata") \
        .load() \
        .rdd \
        .map(lambda row: (row.term, (row.doc_id, row.tf)))

    term_stats_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_stats", keyspace="bigdata") \
        .load() \
        .rdd \
        .map(lambda row: (row.term, row.df))

    doc_info_rdd = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="document_info", keyspace="bigdata") \
        .load() \
        .rdd \
        .map(lambda row: (row.doc_id, (row.title, row.doc_length)))
    print('APP: Cassandra data retrieved')

    query_terms = set(query)
    term_stats = term_stats_rdd.filter(lambda x: x[0] in query_terms).collectAsMap()
    bc_df = sc.broadcast(term_stats)

    doc_info_dict = doc_info_rdd.collectAsMap()
    bc_doc_info = sc.broadcast(doc_info_dict)

    bm25_input = vocab_rdd.map(compute_bm25).filter(lambda x: x is not None)

    doc_term_scores = bm25_input.groupByKey().mapValues(list)

    print("APP: Ranking documents...")
    ranked_docs = doc_term_scores.map(bm25_score) \
                                .filter(lambda x: x is not None) \
                                .sortBy(lambda x: -x[0]) \
                                .take(10)
    print(f"APP: Got {len(ranked_docs)} results")

    print("APP: Ranked docs:")
    for score, doc_id, title in ranked_docs:
        print(f"[{doc_id}] {title} — Score: {score:.4f}")

    result_rows = [(doc_id, title, float(score)) for score, doc_id, title in ranked_docs]
    result_df = spark.createDataFrame(result_rows, ["doc_id", "title", "score"])

    output_path = "/query/"
    print(f"APP: Saving results to HDFS path: {output_path}")
    result_df.write.mode("overwrite").json(output_path)

    print("APP: Done.")

except Exception as e:
    print(f"APP: End with error: {e}")
    try:
        spark
    except NameError:
        spark = SparkSession.builder.getOrCreate()
    error_msg = traceback.format_exc()
    log_error_to_hdfs(spark, error_msg)
