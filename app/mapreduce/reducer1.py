#!/usr/bin/env python3
import sys
from collections import defaultdict

current_term = None
postings = defaultdict(int)

for line in sys.stdin:
    term, doc_id, tf = line.strip().split("\t")
    tf = int(tf)

    if term != current_term:
        if current_term:
            for doc, count in postings.items():
                print(f"{current_term}\t{doc}\t{count}")
        current_term = term
        postings = defaultdict(int)

    postings[doc_id] += tf

# Last term
if current_term:
    for doc, count in postings.items():
        print(f"{current_term}\t{doc}\t{count}")