import sys
import math
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

query = sys.argv[1]

spark = SparkSession.builder.appName("BM25 Search").getOrCreate()
sc = spark.sparkContext

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

k = 1.5
b = 0.75

query_terms = query.lower().split()

# Load vocabulary
vocab = {}
rows = session.execute("SELECT * FROM vocabulary")
for row in rows:
    vocab[row.term] = row.doc_freq

# Load doc stats
doc_lengths = {}
doc_titles = {}
rows = session.execute("SELECT * FROM doc_stats")
for row in rows:
    doc_lengths[row.doc_id] = row.length
    doc_titles[row.doc_id] = row.title

avg_dl = sum(doc_lengths.values()) / len(doc_lengths)
N = len(doc_lengths)

# Broadcasts
bc_vocab = sc.broadcast(vocab)
bc_lengths = sc.broadcast(doc_lengths)
bc_avg_dl = sc.broadcast(avg_dl)

def score(term):
    if term not in bc_vocab.value:
        return []
    df = bc_vocab.value[term]
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    rows = session.execute("SELECT * FROM inverted_index WHERE term=%s", (term,))
    scores = []
    for row in rows:
        dl = bc_lengths.value.get(row.doc_id, 0)
        tf = row.term_freq
        score = idf * ((tf * (k + 1)) / (tf + k * (1 - b + b * (dl / bc_avg_dl.value))))
        scores.append((row.doc_id, score))
    return scores

all_scores = sc.parallelize(query_terms).flatMap(score) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1]) \
    .take(10)

for doc_id, score in all_scores:
    title = doc_titles.get(doc_id, "Unknown")
    print(f"{doc_id}\t{title}\t{score:.4f}")
