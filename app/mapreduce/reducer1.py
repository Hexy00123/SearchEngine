import sys
from collections import defaultdict

term_doc_freqs = defaultdict(lambda: defaultdict(int))

for line in sys.stdin:
    word, doc_id = line.strip().split('\t')
    term_doc_freqs[word][doc_id] += 1

for term, doc_dict in term_doc_freqs.items():
    for doc_id, tf in doc_dict.items():
        print(f"{term}\t{doc_id}\t{tf}")
