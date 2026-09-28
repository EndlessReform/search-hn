"""Freeze a random 2025 throughput slice or the labeled 250 for precision checks."""

import argparse
import json
import sqlite3
import time
from pathlib import Path

import duckdb
from common import MODEL, REVISION, RUN, marked
from transformers import AutoTokenizer

OUT = Path("data/research/books-resolver-throughput-v1")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold", action="store_true")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if args.gold:
        fixture = json.loads(
            Path(
                "packages/search-research/docs/comment-classification/assets/resolver-iteration-v1.json"
            ).read_text()
        )["rows"]
        source = sqlite3.connect(
            "file:data/comment-2025/index.sqlite?mode=ro", uri=True
        )
        candidates = json.loads(
            Path("data/probes/books-resolver-iteration-v1/candidates.json").read_text()
        )
        rows = [
            s
            | {
                "id": str(s["sample_id"]),
                "context": source.execute(
                    "SELECT text FROM comments WHERE comment_id=?", (s["comment_id"],)
                ).fetchone()[0],
                "candidates": candidates[s["sample_id"]]["candidates"][:50],
            }
            for s in fixture
        ]
        docs = json.loads(
            Path("data/probes/books-resolver-iteration-v1/documents.json").read_text()
        )
    else:
        db = sqlite3.connect(f"file:{RUN}/run.sqlite?mode=ro", uri=True)
        rows = [
            json.loads(p)
            for (p,) in db.execute(
                "SELECT payload FROM refs ORDER BY ordinal LIMIT 128"
            )
        ]
        wanted = {c["id"] for r in rows for c in r["candidates"]}
        sql = duckdb.connect()
        sql.execute("SET threads=12")
        sql.execute("CREATE TABLE wanted(id VARCHAR)")
        sql.executemany("INSERT INTO wanted VALUES (?)", [(k,) for k in wanted])
        old = Path("data/probes/books-resolver-smoke-v1")
        records = sql.execute(
            f"SELECT w.key,w.title,list(DISTINCT a.name) FILTER(WHERE a.name IS NOT NULL) FROM read_parquet('{old}/works.parquet') w JOIN wanted ON w.key=wanted.id LEFT JOIN UNNEST(w.authors) au(id) ON true LEFT JOIN read_parquet('{old}/authors.parquet') a ON a.key=au.id GROUP BY w.key,w.title"
        ).fetchall()
        docs = {k: {"title": t, "authors": a or []} for k, t, a in records}
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL, revision=REVISION, local_files_only=True
    )
    start = time.monotonic()
    texts = []
    pairs = []
    groups = []
    for row in rows:
        group = []
        query = (
            "Identify the book referred to by the marked mention.\nMention: "
            + row["title"]
            + "\nComment:\n"
            + marked(row)
        )
        for c in row["candidates"]:
            d = docs[c["id"]]
            document = (
                "Title: "
                + d["title"]
                + "\nAuthor: "
                + ("; ".join(sorted(d["authors"])) or "unknown")
            )
            texts.append(
                tokenizer.apply_chat_template(
                    [
                        {"role": "query", "content": query},
                        {"role": "document", "content": document},
                    ],
                    tokenize=False,
                    add_generation_prompt=True,
                )
            )
            group.append(len(pairs))
            pairs.append({"ref": row["id"], "work": c["id"]})
        groups.append(group)
    tokens = tokenizer(texts, add_special_tokens=False, truncation=False)["input_ids"]
    lengths = sorted(map(len, tokens))
    assert max(lengths) <= 32768
    name = "gold" if args.gold else "corpus128"
    result = {
        "rows": rows,
        "pairs": pairs,
        "groups": groups,
        "tokens": tokens,
        "tokenization_seconds": time.monotonic() - start,
    }
    (OUT / f"{name}.json").write_text(json.dumps(result))
    print(
        json.dumps(
            {
                "name": name,
                "references": len(rows),
                "pairs": len(pairs),
                "tokens": sum(lengths),
                "min": min(lengths),
                "median": lengths[len(lengths) // 2],
                "p95": lengths[int(len(lengths) * 0.95)],
                "max": max(lengths),
                "tokenization_seconds": result["tokenization_seconds"],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
