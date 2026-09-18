"""Adversarial metric cases: provenance, consumption, batching and false citations."""

import json
import math

from search_research.report import evaluate, report


def events(consumed=True, completed_request=True):
    call = {
        "type": "function_call",
        "call_id": "c1",
        "name": "fetch_stories",
        "arguments": '{"query":["x","y"]}',
    }
    output = {
        "type": "function_call_output",
        "call_id": "c1",
        "output": json.dumps(
            {
                "queries": [
                    {"query": "x", "results": [{"id": 1, "title": "other"}]},
                    {
                        "query": "y",
                        "results": [
                            {"id": 1, "title": "other"},
                            {"id": 42, "title": "target"},
                        ],
                    },
                ]
            }
        ),
    }
    rows = [
        {
            "event": "start",
            "model": "test",
            "metadata": {"target_id": 42},
            "prompt": "find 42",
        },
        {"event": "tool_output", "output": output["output"]},
    ]
    if consumed:
        rows.append({"event": "model_input", "request": 2, "items": [call, output]})
    if completed_request:
        rows.append({"event": "model_output", "request": 2, "items": []})
    rows.append({"event": "complete", "final": "answer【story:42】", "elapsed": 1})
    return rows


def test_only_consumed_successful_request_counts():
    assert not evaluate(events(consumed=False))["exposed"]
    assert not evaluate(events(completed_request=False))["exposed"]
    assert evaluate(events())["exposed"]


def test_batch_dedup_rank_and_ndcg():
    row = evaluate(events())
    assert row["pooled_rank"] == 2
    assert row["query_count"] == 2
    assert row["query_pass@1"] == 0
    assert row["query_pass@3"] == 1
    assert row["ndcg@3"] == 1 / math.log2(3)
    assert row["first_query_ndcg@3"] == 0


def test_citation_does_not_imply_retrieval():
    row = evaluate(events(consumed=False))
    assert row["cited"] and not row["exposed"]


def test_repeated_context_not_double_counted():
    rows = events()
    rows.insert(-1, {**rows[2], "request": 3})
    rows.insert(-1, {"event": "model_output", "request": 3, "items": []})
    assert evaluate(rows)["query_count"] == 2


def test_comment_citation_must_belong_to_consumed_target():
    rows = events(consumed=False)
    call = {
        "type": "function_call",
        "call_id": "comments",
        "name": "fetch_top_comments",
        "arguments": '{"story_id":42}',
    }
    output = {
        "type": "function_call_output",
        "call_id": "comments",
        "output": json.dumps(
            {
                "stories": [
                    {"story_id": 42, "comments": [{"id": 101}]},
                    {"story_id": 99, "comments": [{"id": 102}]},
                ]
            }
        ),
    }
    rows.insert(-1, {"event": "model_input", "request": 2, "items": [call, output]})
    rows[-1]["final"] = "answer【comment:101】"
    result = evaluate(rows)
    assert result["exposed"] and result["cited_target_comment"]
    assert result["cited_evidence"] and not result["cited"]
    rows[-1]["final"] = "answer【comment:102】"
    assert not evaluate(rows)["cited_target_comment"]


def test_partial_reports_exclude_unfinished_denominators(tmp_path):
    (tmp_path / "eval.jsonl").write_text(
        json.dumps(
            {
                "id": 42,
                "title": "target",
                "url": "https://example.com",
                "date": "2026-08-01",
            }
        )
        + "\n"
    )
    directory = tmp_path / "trajectories" / "test"
    directory.mkdir(parents=True)
    for style in ("entity", "paraphrase"):
        rows = events()
        rows[0]["metadata"].update(case=f"42-{style}", style=style, cohort="recent")
        if style == "paraphrase":
            rows = rows[:1]  # Started, but not yet a miss or a terminal outcome.
        (directory / f"{style}.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in rows)
        )
    report(tmp_path)
    summary = json.loads((tmp_path / "summary.json").read_text())[0]
    progress = json.loads((tmp_path / "progress.json").read_text())[0]
    assert summary["n"] == 1 and summary["exposed"] == 1.0
    assert summary["variant_pass@2"] is None
    assert progress["in_progress"] == 1 and progress["terminal"] == 1
