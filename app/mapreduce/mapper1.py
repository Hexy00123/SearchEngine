import sys
import re

for data in sys.stdin:
    try:
        doc_id, doc_title, doc_text = data.strip().split('\t', 2)
        words = re.findall(r'\b\w{3,}\b', doc_text.lower())

        for word in words:
            print(f"{word}\t{doc_id}")
    except Exception as e:
        print(e, file=sys.stderr)
        continue 
