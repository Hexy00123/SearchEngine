import sys
from collections import defaultdict
from init_cassandra import initialize_cassandra

# Initialize Cassandra session
session = initialize_cassandra()

# Prepare queries for inserting and updating data in Cassandra
insert_vocab = session.prepare(
    "INSERT INTO vocabulary_index (term, doc_id, tf) VALUES (?, ?, ?)"
)
select_vocab = session.prepare(
    "SELECT COUNT(*) FROM vocabulary_index WHERE term = ? AND doc_id = ?"
)
select_termstats = session.prepare(
    "SELECT df FROM term_stats WHERE term = ?"
)
update_termstats = session.prepare(
    "UPDATE term_stats SET df = ? WHERE term = ?"
)
insert_termstats = session.prepare(
    "INSERT INTO term_stats (term, df) VALUES (?, ?)"
)
select_global = session.prepare(
    "SELECT total_docs, avg_doc_length FROM collection_stats WHERE key = 'global'"
)
update_global = session.prepare(
    "UPDATE collection_stats SET total_docs = ?, avg_doc_length = ? WHERE key = 'global'"
)
insert_global = session.prepare(
    "INSERT INTO collection_stats (key, total_docs, avg_doc_length) VALUES ('global', ?, ?)"
)

# Data structures for term statistics and document lengths
term_docs = defaultdict(dict)  # term -> {doc_id -> tf}
doc_lengths = defaultdict(int)  # doc_id -> total terms
doc_ids = set()  # Unique document IDs

# Process input data
for line in sys.stdin:
    try:
        term, doc_id, tf = line.strip().split('\t')
        tf = int(tf)

        # Check if the (term, doc_id) pair is already in vocabulary_index
        rows = session.execute(select_vocab, (term, doc_id))
        if rows.one().count > 0:
            print(f"APP: Skipping already indexed term-document pair: ({term}, {doc_id})")
            continue

        term_docs[term][doc_id] = tf
        doc_lengths[doc_id] += tf
        doc_ids.add(doc_id)
    except ValueError:
        # Skip malformed lines
        print(f"APP: Skipping malformed line: {line.strip()}")

# Insert term-document mappings into Cassandra
for term, postings in term_docs.items():
    for doc_id, tf in postings.items():
        session.execute(insert_vocab, (term, doc_id, tf))

# Update term statistics (document frequency) in Cassandra
for term, postings in term_docs.items():
    df = len(postings)
    existing_df = 0
    rows = session.execute(select_termstats, (term,))
    for row in rows:
        existing_df = row.df
    if existing_df > 0:
        session.execute(update_termstats, (existing_df + df, term))
    else:
        session.execute(insert_termstats, (term, df))

# Update global statistics in Cassandra
total_docs = len(doc_lengths)
total_doc_length = sum(doc_lengths.values())
avg_doc_length = total_doc_length / total_docs if total_docs > 0 else 0.0

existing_total_docs = 0
existing_avg_doc_length = 0.0
rows = session.execute(select_global)
for row in rows:
    existing_total_docs = row.total_docs
    existing_avg_doc_length = row.avg_doc_length

if existing_total_docs > 0:
    # Update global stats
    new_total_docs = existing_total_docs + total_docs
    new_avg_doc_length = (
        (existing_avg_doc_length * existing_total_docs + total_doc_length)
        / new_total_docs
    )
    session.execute(update_global, (new_total_docs, new_avg_doc_length))
else:
    # Insert global stats
    session.execute(insert_global, (total_docs, avg_doc_length))

print("APP: Index written to Cassandra")