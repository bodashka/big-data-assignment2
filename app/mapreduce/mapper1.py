#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split("\t", 2)
        words = tokenize(text)
        for word in words:
            print(f"{word}\t{doc_id}\t1")
    except:
        continue