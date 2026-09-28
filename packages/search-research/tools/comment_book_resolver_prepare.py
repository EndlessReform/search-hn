"""Freeze title-only top-100 retrieval; retain complete contexts outside the checkout."""

import argparse
import hashlib
import json
import sqlite3
import time
from pathlib import Path

import duckdb
import tantivy

parser = argparse.ArgumentParser()
parser.add_argument("--root", type=Path, default=Path.cwd())
args = parser.parse_args()
root = args.root
old = root / "data/probes/books-resolver-smoke-v1"
out = root / "data/probes/books-resolver-iteration-v1"
out.mkdir(exist_ok=True)
fixture = json.loads(
    (
        root
        / "packages/search-research/docs/comment-classification/assets/resolver-iteration-v1.json"
    ).read_text()
)
db_source = sqlite3.connect(
    f"file:{root}/data/comment-2025/index.sqlite?mode=ro", uri=True
)
rows = []
for sample in fixture["rows"]:
    context = db_source.execute(
        "SELECT text FROM comments WHERE comment_id=?", (sample["comment_id"],)
    ).fetchone()[0]
    assert hashlib.sha256(context.encode()).hexdigest() == sample["context_sha256"], (
        sample["sample_id"]
    )
    assert context[sample["start"] : sample["end"]] == sample["title"], sample[
        "sample_id"
    ]
    rows.append(sample | {"context": context})
assert len(rows) == 250
index = tantivy.Index.open(str(old / "title-bm25"))
analyzer = (
    tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
    .filter(tantivy.Filter.lowercase())
    .build()
)
index.register_tokenizer("lower", analyzer)
searcher = index.searcher()
results = []
ids = set()
for i, row in enumerate(rows):
    tokens = analyzer.analyze(row["title"])
    q = index.parse_query(" ".join('"' + t + '"' for t in tokens), ["title"])
    start = time.perf_counter()
    hits = searcher.search(q, limit=100).hits
    ms = (time.perf_counter() - start) * 1000
    candidates = []
    for score, addr in hits:
        doc = searcher.doc(addr).to_dict()
        key = doc["id"][0]
        ids.add(key)
        candidates.append({"id": key, "score": score})
    results.append({"sample_id": i, "retrieval_ms": ms, "candidates": candidates})
db = duckdb.connect()
db.execute("SET threads=12")
db.execute("CREATE TABLE wanted(id VARCHAR)")
db.executemany("INSERT INTO wanted VALUES (?)", [(k,) for k in ids])
meta = db.execute(
    f"SELECT w.key,w.title,list(DISTINCT a.name) FILTER(WHERE a.name IS NOT NULL) FROM read_parquet('{old}/works.parquet') w JOIN wanted ON w.key=wanted.id LEFT JOIN UNNEST(w.authors) au(id) ON true LEFT JOIN read_parquet('{old}/authors.parquet') a ON a.key=au.id GROUP BY w.key,w.title"
).fetchall()
docs = {k: {"id": k, "title": t, "authors": a or []} for k, t, a in meta}
(out / "candidates.json").write_text(json.dumps(results))
(out / "documents.json").write_text(json.dumps(docs))
print(
    {
        "queries": len(results),
        "unique_candidates": len(docs),
        "retrieval_seconds": sum(r["retrieval_ms"] for r in results) / 1000,
    },
    flush=True,
)
