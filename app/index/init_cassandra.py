from cassandra.cluster import Cluster

def initialize_cassandra():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

    session.set_keyspace('bigdata')

    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary_index (
            term TEXT,
            doc_id TEXT,
            tf INT,
            PRIMARY KEY (term, doc_id)  
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS term_stats (
            term TEXT PRIMARY KEY,
            df INT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS document_info (
            doc_id TEXT PRIMARY KEY,
            title TEXT,
            doc_length INT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS collection_stats (
            key TEXT PRIMARY KEY,
            total_docs INT,
            avg_doc_length DOUBLE
        )
    """)

    print("APP: Cassandra keyspace and tables initialized.")
    return session

if __name__ == "__main__":
    initialize_cassandra()
