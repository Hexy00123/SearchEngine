import re
import sys
from init_cassandra import initialize_cassandra

# Initialize Cassandra session
session = initialize_cassandra()

# Prepare the query to insert document information
insert_docinfo = session.prepare(
    "INSERT INTO document_info (doc_id, title, doc_length) VALUES (?, ?, ?)"
)

line_count = 0

# Inside the loop
for line in sys.stdin:
    line_count += 1
    print(f"Processing line {line_count}:", file=sys.stderr)
    try:
        # Parse the input line
        fields = line.strip().split('\t', 2)
        if len(fields) != 3:
            raise ValueError(f"Malformed line: {line.strip()}")
        
        doc_id, doc_title, doc_text = fields

        # Sanitize the title (limit length and replace invalid characters)
        sanitized_title = doc_id + "_" + "_".join(doc_title.split())

        # Calculate document length using the same regex as mapper1.py
        words = re.findall(r'\b\w{3,}\b', doc_text.lower())
        doc_length = len(words)

        # Insert into Cassandra
        session.execute(insert_docinfo, (doc_id, sanitized_title, doc_length))
        print(f"APP: Successfully added document {doc_id} with length {doc_length}", file=sys.stderr)

    except Exception as e:
        # Log errors and skip malformed lines
        print(f"Error processing line: {line.strip()} - {e}", file=sys.stderr)
        continue