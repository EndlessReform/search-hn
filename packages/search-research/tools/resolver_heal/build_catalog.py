"""Build a single Tantivy retrieval catalog from the frozen Parquet snapshot.

DuckDB is an offline input reader here, not a retrieval backend. Native joins
resolve author IDs in bounded hash partitions; only one thousand completed work
records at a time cross into Python for indexing. No name-query/work cross
product is ever constructed. Each Tantivy document stores the title, work ID,
and separate author names/token lists, preserving per-author overlap semantics.
"""

import argparse
import json
import resource
import time
from pathlib import Path

import duckdb
import tantivy


def build(source: Path, output: Path, partitions=32):
    """Publish a complete index without replacing an existing catalog.

    Partitioning bounds metadata aggregation memory; it does require repeated
    Parquet scans. The title schema/tokenizer matches the previous title-only
    index. Null titles are excluded, just as in that index. build.json is written
    only after commits and merges finish and the searchable count is verified.
    """
    assert partitions > 0
    assert not output.exists(), f"Refusing to replace existing catalog: {output}"
    temporary = output.with_name(output.name + ".building")
    temporary.mkdir(parents=True)
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("id", stored=True, tokenizer_name="raw")
    schema_builder.add_text_field(
        "title", stored=True, tokenizer_name="lower", index_option="freq"
    )
    schema_builder.add_bytes_field(
        "author_data", stored=True, indexed=False, fast=False
    )
    schema = schema_builder.build()
    index = tantivy.Index(schema, path=str(temporary))
    analyzer = (
        tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
        .filter(tantivy.Filter.lowercase())
        .build()
    )
    index.register_tokenizer("lower", analyzer)
    writer = index.writer(512 * 1024**2, 8)
    started = time.monotonic()
    count = 0
    for partition in range(partitions):
        with duckdb.connect(
            config={
                "memory_limit": "6GB",
                "threads": "4",
                "temp_directory": str(temporary / "spill"),
                "preserve_insertion_order": "false",
            }
        ) as db:
            db.execute(
                "CREATE TEMP TABLE works AS SELECT key,title,authors FROM read_parquet(?) "
                "WHERE title IS NOT NULL AND hash(key)%?=?",
                [str(source / "works.parquet"), partitions, partition],
            )
            db.execute(
                "CREATE TEMP TABLE wa AS SELECT key,unnest(authors) aid FROM works"
            )
            db.execute(
                "CREATE TEMP TABLE metadata AS SELECT wa.key,"
                "to_json(list(DISTINCT struct_pack(name:=a.name,tokens:="
                "regexp_extract_all(lower(a.name),'[\\p{L}\\p{N}]+')))) payload "
                "FROM wa JOIN read_parquet(?) a ON a.key=wa.aid "
                "WHERE a.name IS NOT NULL GROUP BY wa.key",
                [str(source / "authors.parquet")],
            )
            db.execute(
                "SELECT w.key,w.title,coalesce(m.payload,'[]') FROM works w LEFT JOIN metadata m ON m.key=w.key"
            )
            while batch := db.fetchmany(1000):
                for key, title, payload in batch:
                    document = tantivy.Document.from_dict(
                        {"id": key, "title": title}, schema
                    )
                    document.add_bytes("author_data", payload.encode())
                    writer.add_document(document)
                    count += 1
        print(
            json.dumps(
                {
                    "partition": partition,
                    "documents": count,
                    "seconds": time.monotonic() - started,
                }
            ),
            flush=True,
        )
    writer.commit()
    writer.wait_merging_threads()
    index.reload()
    with duckdb.connect() as db:
        expected = db.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE title IS NOT NULL",
            [str(source / "works.parquet")],
        ).fetchone()[0]
    assert index.searcher().num_docs == count == expected
    manifest = {
        "format": 1,
        "documents": count,
        "partitions": partitions,
        "seconds": time.monotonic() - started,
        "max_rss_platform_units": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "sources": {
            name: {
                "path": str(source / name),
                "bytes": (source / name).stat().st_size,
                "mtime_ns": (source / name).stat().st_mtime_ns,
            }
            for name in ("works.parquet", "authors.parquet")
        },
    }
    (temporary / "build.json").write_text(json.dumps(manifest, indent=2))
    temporary.rename(output)
    print(json.dumps(manifest), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", type=Path, default=Path("data/probes/books-resolver-smoke-v1")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("data/probes/books-catalog-tantivy-v1")
    )
    parser.add_argument("--partitions", type=int, default=32)
    args = parser.parse_args()
    build(args.source, args.output, args.partitions)
