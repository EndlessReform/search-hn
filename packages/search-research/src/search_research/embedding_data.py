"""Freeze a title+URL corpus and count exact embedding inputs before spending."""

import hashlib
import json
import os
from pathlib import Path

import polars as pl
import tiktoken
from search_agent.data_access import HNStorySearchRepository
from sqlalchemy import text

from search_research.dataset import read_jsonl

MODEL = "text-embedding-3-large"
PRICE_PER_MILLION = 0.13
SQL = """SELECT id, title, url, score, day FROM items
WHERE type='story' AND day >= DATE '2024-09-04' AND day < DATE '2026-09-05'
AND score >= 10 AND NOT coalesce(dead,false) AND NOT coalesce(deleted,false)
ORDER BY id"""


def prepare(root: Path, eval_path: Path):
    """Only public HN title+URL leaves the laptop; no bodies/comments or secrets.

    Snapshot reuse makes restart independent of changes to the live mirror.
    Token IDs are recomputed from these exact strings for each API batch.
    """
    root.mkdir(parents=True, exist_ok=True)
    encoding = tiktoken.encoding_for_model(MODEL)
    path = root / "corpus.parquet"
    if not path.exists():
        repo = HNStorySearchRepository.from_database_url(
            os.environ["HN_QUERY_DATABASE_URL"]
        )
        try:
            with repo._engine.connect() as conn:
                conn.execute(text("SET statement_timeout='45s'"))
                rows = [dict(r) for r in conn.execute(text(SQL)).mappings()]
            frame = pl.DataFrame(rows).with_columns(
                (
                    pl.col("title").fill_null("") + "\n" + pl.col("url").fill_null("")
                ).alias("input")
            )
            frame = frame.with_columns(
                pl.Series(
                    "tokens",
                    [
                        len(encoding.encode(s, disallowed_special=()))
                        for s in frame["input"]
                    ],
                )
            )
            frame.write_parquet(path.with_suffix(".partial"))
            path.with_suffix(".partial").replace(path)
        finally:
            repo.dispose()
    corpus = pl.read_parquet(path)
    questions = pl.DataFrame(
        [
            {
                "case": f"{r['id']}-{style}",
                "target_id": r["id"],
                "style": style,
                "cohort": r["cohort"],
                "input": r["questions"][style + "_question"],
            }
            for r in read_jsonl(eval_path)
            for style in ("entity", "paraphrase")
        ]
    ).with_columns(
        pl.Series(
            "tokens",
            [
                len(
                    encoding.encode(
                        r["questions"][s + "_question"], disallowed_special=()
                    )
                )
                for r in read_jsonl(eval_path)
                for s in ("entity", "paraphrase")
            ],
        )
    )
    assert set(questions["target_id"]) <= set(corpus["id"]), (
        "Evaluation targets absent from filtered corpus"
    )
    assert corpus["tokens"].min() > 0 and corpus["tokens"].max() <= 8191
    assert questions["tokens"].min() > 0 and questions["tokens"].max() <= 8191
    tokens = corpus["tokens"].sum() + questions["tokens"].sum()
    manifest = {
        "model": MODEL,
        "dimensions": 3072,
        "sql": SQL,
        "corpus_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "eval_sha256": hashlib.sha256(eval_path.read_bytes()).hexdigest(),
        "stories": corpus.height,
        "questions": questions.height,
        "input_tokens": tokens,
        "price_per_million": PRICE_PER_MILLION,
        "estimated_usd": tokens * PRICE_PER_MILLION / 1e6,
        "ceiling_usd": 2.50,
    }
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        assert json.loads(manifest_path.read_text()) == manifest, (
            "Frozen inputs changed"
        )
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2))
    questions.write_parquet(root / "questions.parquet")
    print(json.dumps(manifest), flush=True)
    assert manifest["estimated_usd"] * 1.15 < 2.50, (
        "Cost exceeds authorization including 15% reserve; ask user"
    )
    return corpus, questions
