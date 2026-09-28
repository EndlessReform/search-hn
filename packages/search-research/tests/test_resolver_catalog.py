"""Check consolidated retrieval against exhaustive scoring on a small catalog."""

import importlib.util
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("tantivy")
TOOLS = Path(__file__).parents[1] / "tools/resolver_heal"


def load(name):
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_author_scoring_keeps_contributors_separate():
    score = load("catalog").author_bonus
    authors = [{"tokens": ["don", "other"]}, {"tokens": ["another", "norman"]}]
    assert score(authors, {"don", "norman"}, []) == 2.5
    assert score(authors, set(), [{"don"}]) == 0
    assert score(authors, None, [{"don", "norman"}]) == 0
    assert score(authors, None, [{"don", "other"}, {"another", "norman"}]) == 5
    assert score([], {"don"}, []) == 0


def test_index_build_and_single_search_match_exhaustive(tmp_path):
    build = load("build_catalog").build
    module = load("catalog")
    db = duckdb.connect()
    db.execute("CREATE TABLE authors(key VARCHAR,name VARCHAR)")
    db.executemany(
        "INSERT INTO authors VALUES (?,?)",
        [("a", "Don Norman"), ("b", "Donald A. Norman"), ("c", "Unrelated Author")],
    )
    db.execute(
        "COPY authors TO ? (FORMAT PARQUET)", [str(tmp_path / "authors.parquet")]
    )
    db.execute("CREATE TABLE works(key VARCHAR,title VARCHAR,authors VARCHAR[])")
    rows = [
        (
            f"/works/OL{i}W",
            "Target " + "extra " * (i % 12),
            [["a"], ["b"], ["c"], [], ["a", "b"]][i % 5],
        )
        for i in range(120)
    ]
    rows.append(("/works/null", None, ["a"]))
    db.executemany("INSERT INTO works VALUES (?,?,?)", rows)
    db.execute("COPY works TO ? (FORMAT PARQUET)", [str(tmp_path / "works.parquet")])
    output = tmp_path / "index"
    build(tmp_path, output, partitions=2)
    counts_path = tmp_path / "counts.parquet"
    db.execute(
        "COPY (SELECT NULL::VARCHAR work_id, 0::BIGINT readinglog_count WHERE false) TO ? (FORMAT PARQUET)",
        [str(counts_path)],
    )
    catalog = module.Catalog(output, title_pool=1000, counts_path=counts_path)
    assert catalog.searcher.num_docs == 120
    actual, metrics = catalog.search("Target", "Don Norman", [])
    assert metrics["search_calls"] == 1
    assert metrics["hits_returned"] == 120
    assert metrics["global_score_bound_satisfied"]
    query = catalog.index.parse_query('"target"', ["title"])
    expected = []
    bonus = {0: 5.0, 1: 2.5, 2: 0.0, 3: 0.0, 4: 5.0}
    for raw, address in catalog.searcher.search(query, limit=1000).hits:
        key = catalog.searcher.doc(address).to_dict()["id"][0]
        score = module.float32(
            raw + bonus[int(key.removeprefix("/works/OL").removesuffix("W")) % 5]
        )
        expected.append((score, key))
    assert [(d["retrieval_score"], d["id"]) for d in actual] == sorted(
        expected,
        key=lambda v: (-v[0], int(v[1].removeprefix("/works/OL").removesuffix("W"))),
    )[:50]
    assert all(d["author_match"] == (d["author_match_score"] > 0) for d in actual)
    assert len(actual) == 50
    assert catalog.search("Nonexistent", None, [])[0] == []
    narrow = module.Catalog(output, title_pool=50, counts_path=counts_path)
    limited, measurements = narrow.search("Target", "Don Norman", [])
    assert measurements["search_calls"] == 1
    assert measurements["hits_returned"] == 50
    assert len(limited) == 50
    with pytest.raises(AssertionError, match="Refusing to replace"):
        build(tmp_path, output)


def test_author_overlap_matches_legacy_formula():
    score = load("catalog").author_bonus
    authors = {
        "partial": [{"tokens": ["donald", "a", "norman"]}],
        "exact": [{"tokens": ["don", "norman"]}],
        "contributors": [
            {"tokens": ["don", "other"]},
            {"tokens": ["another", "norman"]},
        ],
    }
    assert {
        key: score(value, {"don", "norman"}, []) for key, value in authors.items()
    } == {"partial": 2.5, "exact": 5.0, "contributors": 2.5}
    assert all(score(value, set(), []) == 0 for value in authors.values())
    assert all(score(value, {"missing"}, []) == 0 for value in authors.values())


def test_initial_cases_require_complete_ner(tmp_path, monkeypatch):
    import json

    monkeypatch.syspath_prepend(str(TOOLS))
    retrieve = load("retrieve")
    refs = [{"id": "1:0:6", "comment_id": 1, "title": "Target"}]
    (tmp_path / "references.json").write_text(json.dumps(refs))
    (tmp_path / "names.jsonl").write_text('{"comment_id": 2, "spans": []}\n')
    with pytest.raises(AssertionError, match="NER is incomplete"):
        retrieve.initial_cases(tmp_path)
    spans = [{"text": "Don Norman"}]
    (tmp_path / "names.jsonl").write_text(
        json.dumps({"comment_id": 1, "spans": spans}) + "\n"
    )
    assert retrieve.initial_cases(tmp_path) == [
        {
            "id": "1:0:6",
            "reference": refs[0],
            "query_title": "Target",
            "query_author": None,
            "person_spans": spans,
        }
    ]


@pytest.mark.parametrize("stage", ["first", "round1"])
def test_retrieval_cli_preserves_schema(tmp_path, monkeypatch, stage):
    import json
    import sys

    monkeypatch.syspath_prepend(str(TOOLS))
    retrieve = load("retrieve")
    monkeypatch.setattr(retrieve, "ROOT", tmp_path)
    reference = {"id": "1:0:6", "comment_id": 1, "title": "Target"}
    case = {
        "id": "child",
        "reference": reference,
        "query_title": "Target",
        "query_author": "Don Norman",
        "person_spans": [],
        "root_id": reference["id"],
        "parent_id": reference["id"],
    }
    (tmp_path / "references.json").write_text(json.dumps([reference]))
    (tmp_path / "names.jsonl").write_text('{"comment_id": 1, "spans": []}\n')
    (tmp_path / "round1-queries.json").write_text(json.dumps({"cases": [case]}))
    (tmp_path / "build.json").write_text("{}")
    candidate = {
        "id": "/works/A",
        "title": "Target",
        "authors": ["Don Norman"],
        "bm25": 1.0,
        "retrieval_score": 6.0,
        "author_match": True,
        "author_match_score": 5.0,
    }

    class FixtureCatalog:
        def __init__(self, path, pool, *, counts_path):
            assert path == tmp_path and pool == 10_000

        def search(self, title, author, spans):
            assert title == "Target" and spans == []
            assert author == (None if stage == "first" else "Don Norman")
            return [candidate], {"search_calls": 1}

    monkeypatch.setattr(retrieve, "Catalog", FixtureCatalog)
    mode = ["--stage", "first"] if stage == "first" else ["--round", "1"]
    monkeypatch.setattr(
        sys,
        "argv",
        ["retrieve.py", *mode, "--catalog", str(tmp_path), "--workers", "1"],
    )
    retrieve.main()
    result = json.loads((tmp_path / f"{stage}-cases.json").read_text())["cases"][0]
    assert result["reference"] == reference
    assert result["candidates"] == [candidate]
    assert result["author_bonus"] == 5.0
    assert result["retrieved"] == [
        {k: v for k, v in candidate.items() if k not in ("title", "authors")}
    ]
    if stage != "first":
        assert all(result[key] == value for key, value in case.items())
    metrics = json.loads((tmp_path / f"{stage}-retrieval-metrics.json").read_text())
    assert metrics["queries"] == [{"id": result["id"], "search_calls": 1}]


def test_tied_scores_prefer_popularity_then_numeric_id(tmp_path):
    """Exercise heap eviction as well as sorting across a 50-work cutoff."""
    module = load("catalog")
    with duckdb.connect() as db:
        db.execute(
            "COPY (SELECT 'a' AS key, 'An Author' AS name) TO ? (FORMAT PARQUET)",
            [str(tmp_path / "authors.parquet")],
        )
        db.execute(
            "COPY (SELECT '/works/OL' || i || 'W' AS key, 'Target' AS title, ['a'] AS authors FROM range(1, 81) t(i)) TO ? (FORMAT PARQUET)",
            [str(tmp_path / "works.parquet")],
        )
        counts_path = tmp_path / "counts.parquet"
        db.execute(
            "COPY (SELECT '/works/OL' || i || 'W' AS work_id, 10 AS readinglog_count FROM range(60, 81) t(i)) TO ? (FORMAT PARQUET)",
            [str(counts_path)],
        )
    load("build_catalog").build(tmp_path, tmp_path / "index", partitions=2)
    catalog = module.Catalog(tmp_path / "index", counts_path=counts_path)
    hits, _ = catalog.search("Target", None, [])
    assert len(hits) == 50
    assert len({d["retrieval_score"] for d in hits}) == 1
    assert [d["id"] for d in hits] == [
        f"/works/OL{i}W" for i in [*range(60, 81), *range(1, 30)]
    ]


def test_parallel_search_matches_serial(tmp_path, monkeypatch):
    """Real spawned workers preserve candidate values, query order and failures."""
    import importlib
    from concurrent.futures.process import BrokenProcessPool

    monkeypatch.syspath_prepend(str(TOOLS))
    retrieve = importlib.import_module("retrieve")
    with duckdb.connect() as db:
        db.execute(
            "COPY (SELECT 'a' AS key, 'An Author' AS name) TO ? (FORMAT PARQUET)",
            [str(tmp_path / "authors.parquet")],
        )
        db.execute(
            "COPY (SELECT '/works/OL' || i || 'W' AS key, 'Target' AS title, ['a'] AS authors FROM range(1, 81) t(i)) TO ? (FORMAT PARQUET)",
            [str(tmp_path / "works.parquet")],
        )
        db.execute(
            "COPY (SELECT '/works/OL70W' AS work_id, 10 AS readinglog_count) TO ? (FORMAT PARQUET)",
            [str(tmp_path / "counts.parquet")],
        )
    load("build_catalog").build(tmp_path, tmp_path / "index", partitions=2)
    cases = [
        {"query_title": title, "query_author": author, "person_spans": spans}
        for title, author, spans in [
            ("Target", None, []),
            ("Missing", None, []),
            ("Target", "An Author", []),
            ("Target", None, [{"text": "An Author"}]),
        ]
    ]

    def run(workers):
        return list(
            retrieve.search_cases(
                cases, tmp_path / "index", 10000, workers, tmp_path / "counts.parquet"
            )
        )

    serial, parallel = run(1), run(2)
    assert [r[0] for r in serial] == [r[0] for r in parallel]
    for (_, left), (_, right) in zip(serial, parallel, strict=True):
        assert {k: v for k, v in left.items() if not k.endswith("seconds")} == {
            k: v for k, v in right.items() if not k.endswith("seconds")
        }
    assert list(retrieve.search_cases([], tmp_path / "missing", 10000, 16)) == []
    with pytest.raises(BrokenProcessPool):
        list(
            retrieve.search_cases(
                cases, tmp_path / "missing", 10000, 2, tmp_path / "counts.parquet"
            )
        )
    with pytest.raises(AssertionError, match="Empty title query"):
        list(
            retrieve.search_cases(
                [{"query_title": "", "query_author": None, "person_spans": []}],
                tmp_path / "index",
                10000,
                2,
                tmp_path / "counts.parquet",
            )
        )
