"""Freeze complete comments, original-span top-50 results and catalog metadata.

Run once on melchior. All later stages resume from this artifact, not /tmp or a
mutable live corpus. Multiple workers retrieve unique title queries concurrently.
"""

import argparse
import concurrent.futures
import json
import random
import sqlite3
import time
from pathlib import Path

import duckdb
import polars as pl
import tantivy
from common import MODEL, PROMPT, REVISION, RUN, connect, digest, status


def main():
    RUN.mkdir(parents=True, exist_ok=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-metadata", action="store_true")
    parser.add_argument("--slice", type=Path, required=True)
    parser.add_argument("--proposals", type=Path, required=True)
    args = parser.parse_args()
    old = Path("data/probes/books-resolver-smoke-v1")
    exists = (RUN / "run.sqlite").exists()
    assert exists == args.resume_metadata, "Existing inputs require --resume-metadata"
    db = connect()
    if args.resume_metadata:
        rows = [
            json.loads(p)
            for (p,) in db.execute("SELECT payload FROM refs ORDER BY ordinal")
        ]
        assert db.execute("SELECT count(*) FROM documents").fetchone()[0] == 0
        ids = {c["id"] for row in rows for c in row["candidates"]}
    else:
        source = sqlite3.connect(
            (args.slice / "index.sqlite").resolve().as_uri() + "?mode=ro", uri=True
        )
        db.executescript("""
        CREATE TABLE refs(id TEXT PRIMARY KEY, ordinal INTEGER UNIQUE, payload TEXT NOT NULL);
        CREATE TABLE documents(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE rankings(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE selections(id TEXT, model TEXT, payload TEXT NOT NULL, PRIMARY KEY(id,model));
        CREATE TABLE failures(id TEXT, stage TEXT, payload TEXT NOT NULL, PRIMARY KEY(id,stage));
        CREATE TABLE status(stage TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)
        rows = []
        for line in args.proposals.open():
            record = json.loads(line)
            spans = [s for s in record["spans"] if s["score"] >= 0.17]
            if not spans:
                continue
            context = source.execute(
                "SELECT text FROM comments WHERE comment_id=?", (record["comment_id"],)
            ).fetchone()[0]
            seen = set()
            for span in spans:
                norm = " ".join(span["title"].lower().split())
                if norm in seen:
                    continue
                seen.add(norm)
                assert context[span["start"] : span["end"]] == span["title"]
                rows.append(
                    span
                    | {
                        "id": f"{record['comment_id']}:{span['start']}:{span['end']}",
                        "comment_id": record["comment_id"],
                        "context": context,
                    }
                )
        random.Random(20260926).shuffle(rows)
        index = tantivy.Index.open(str(old / "title-bm25"))
        analyzer = (
            tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
            .filter(tantivy.Filter.lowercase())
            .build()
        )
        index.register_tokenizer("lower", analyzer)
        searcher = index.searcher()

        def retrieve(title):
            tokens = analyzer.analyze(title)
            if not tokens:
                return title, []
            query = index.parse_query(
                " ".join('"' + t + '"' for t in tokens), ["title"]
            )
            return title, [
                {"id": searcher.doc(addr).to_dict()["id"][0], "bm25": score}
                for score, addr in searcher.search(query, limit=50).hits
            ]

        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            queries = dict(pool.map(retrieve, sorted({r["title"] for r in rows})))
        ids = {c["id"] for result in queries.values() for c in result}
        for n, row in enumerate(rows):
            row["candidates"] = queries[row["title"]]
            db.execute(
                "INSERT INTO refs VALUES (?,?,?)", (row["id"], n, json.dumps(row))
            )
        db.commit()
        print(
            json.dumps(
                {
                    "references": len(rows),
                    "candidate_ids": len(ids),
                    "retrieval_seconds": time.monotonic() - start,
                }
            ),
            flush=True,
        )
    meta = duckdb.connect()
    meta.execute("SET threads=12")
    wanted = pl.DataFrame({"id": sorted(ids)})
    meta.register("wanted", wanted.to_arrow())
    docs = meta.execute(
        f"SELECT w.key,w.title,list(DISTINCT a.name) FILTER(WHERE a.name IS NOT NULL) FROM read_parquet('{old}/works.parquet') w JOIN wanted ON w.key=wanted.id LEFT JOIN UNNEST(w.authors) au(id) ON true LEFT JOIN read_parquet('{old}/authors.parquet') a ON a.key=au.id GROUP BY w.key,w.title"
    ).fetchall()
    assert len(docs) == len(ids)
    db.executemany(
        "INSERT INTO documents VALUES (?,?)",
        [
            (k, json.dumps({"id": k, "title": t, "authors": a or []}))
            for k, t, a in docs
        ],
    )
    db.commit()
    sources = [
        args.proposals,
        old / "build-summary.json",
        old / "works.parquet",
        old / "authors.parquet",
    ]
    manifest = {
        "run_id": RUN.name,
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "references": len(rows),
        "catalog_snapshot": "2026-08-31",
        "retrieval": "Tantivy title BM25 top 50, original span",
        "translation": False,
        "popularity": False,
        "ner_threshold": 0.17,
        "dedup": "comment ID and whitespace-collapsed lowercase title; retain first occurrence",
        "shuffle_seed": 20260926,
        "reranker": MODEL,
        "reranker_revision": REVISION,
        "reranker_dtype": "fp8 with BF16 remaining computation and KV",
        "reranker_backend": "vllm 0.23.0; prefix caching; 16k token budget; 256 sequences",
        "selector_prompt": PROMPT,
        "sources": {str(p): digest(p) for p in sources},
        "script_sha256": {
            str(p): digest(p) for p in Path(__file__).parent.glob("*.py")
        },
    }
    (RUN / "manifest.json").write_text(json.dumps(manifest, indent=2))
    status(db, "prepare", {"state": "complete", "references": len(rows)})
    print("PREPARE COMPLETE", flush=True)


if __name__ == "__main__":
    main()
