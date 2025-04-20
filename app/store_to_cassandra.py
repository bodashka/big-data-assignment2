from cassandra.cluster import Cluster
from collections import defaultdict

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# Use keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS search_engine WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("search_engine")

# Create tables
session.execute("""
CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id int PRIMARY KEY,
    doc_len int,
    title text
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY,
    doc_freq int
)""")

session.execute("""
CREATE TABLE IF NOT EXISTS inverted_index (
    term text,
    doc_id int,
    term_freq int,
    PRIMARY KEY (term, doc_id)
)""")

# Data structures
vocab = defaultdict(set)
doc_lengths = defaultdict(int)

# Read reducer output
with open('reducer_output.txt', 'r') as f:
    for line in f:
        term, doc_id_str, tf_str = line.strip().split('\t')
        doc_id = int(doc_id_str)
        tf = int(tf_str)

        vocab[term].add(doc_id)
        doc_lengths[doc_id] += tf

        session.execute(
            "INSERT INTO inverted_index (term, doc_id, term_freq) VALUES (%s, %s, %s)",
            (term, doc_id, tf)
        )

# Insert vocabulary and doc stats
for term, doc_ids in vocab.items():
    session.execute(
        "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
        (term, len(doc_ids))
    )

for doc_id, doc_len in doc_lengths.items():
    # Replace with real title retrieval logic
    title = f"Document {doc_id}"  # or fetch it from somewhere meaningful
    session.execute(
        "INSERT INTO doc_stats (doc_id, doc_len, title) VALUES (%s, %s, %s)",
        (doc_id, doc_len, title)
    )


print("Stored data in Cassandra.")
