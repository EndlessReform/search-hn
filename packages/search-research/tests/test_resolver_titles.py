"""Title proposal window merging, source offsets, and reference identity."""

import importlib.util
import json
from pathlib import Path

import pytest


def load():
    path = Path(__file__).parents[1] / "tools/resolver_heal/titles.py"
    spec = importlib.util.spec_from_file_location("resolver_titles", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("scheduled", [False, True])
def test_length_sorted_windows_merge_offsets_and_keep_highest_score(
    monkeypatch, scheduled
):
    module = load()
    text = "😀 Dune and Dune"

    def windows(model, source, labels):
        assert labels == ["book title"]
        return [(0, source), (11, "Dune")] if source else []

    monkeypatch.setattr(module, "text_windows", windows)
    if scheduled:
        from search_research.ner_batching import Window

        # GPU order reverses character order; reference identity must not follow it.
        monkeypatch.setattr(
            module,
            "prepare_windows",
            lambda *args: [Window(1, 0, text, 10), Window(1, 11, "Dune", 20)],
        )

    class Model:
        def inference(self, texts, labels, **kwargs):
            assert texts == ([text, "Dune"] if scheduled else ["Dune", text])
            assert kwargs == {"batch_size": 16, "threshold": 0.17, "flat_ner": False}
            result = [
                [{"start": 0, "end": 4, "text": "Dune", "score": 0.4}],
                [
                    {"start": 2, "end": 6, "text": "Dune", "score": 0.6},
                    {"start": 11, "end": 15, "text": "Dune", "score": 0.9},
                ],
            ]
            return result[::-1] if scheduled else result

    records = module.extract_block(
        Model(),
        [(1, text), (2, "")],
        16,
        0.17,
        [(1536, 16)] if scheduled else None,
    )
    assert records == [
        {
            "comment_id": 1,
            "spans": [
                {"start": 11, "end": 15, "title": "Dune", "score": 0.9},
                {"start": 2, "end": 6, "title": "Dune", "score": 0.6},
            ],
        },
        {"comment_id": 2, "spans": []},
    ]
    # "First" is the original runner's proposal order, not the earliest offset.
    refs = module.references(records[0], text, 0.17)
    assert len(refs) == 1
    assert refs[0]["id"] == "1:11:15"
    assert refs[0]["context"] == text


def test_dedup_uses_lowercase_not_casefold_and_inclusive_threshold():
    module = load()
    spans = [
        {"title": " DUNE\n Messiah ", "score": 0.16},
        {"title": "Dune Messiah", "score": 0.17},
        {"title": "DUNE  MESSIAH", "score": 0.99},
        {"title": "Straße", "score": 0.5},
        {"title": "STRASSE", "score": 0.5},
    ]
    assert list(module.unique_spans(spans, 0.17)) == [spans[1], spans[3], spans[4]]


def test_bad_model_offsets_fail(monkeypatch):
    module = load()
    monkeypatch.setattr(module, "text_windows", lambda *args: [(0, "Dune")])

    class Model:
        def inference(self, *args, **kwargs):
            return [[{"start": 0, "end": 3, "text": "Dune", "score": 0.8}]]

    with pytest.raises(AssertionError, match="Title differs from source"):
        module.extract_block(Model(), [(1, "Dune")], 16, 0.17)


def test_filter_ids_reject_duplicates(tmp_path):
    module = load()
    path = tmp_path / "passes.jsonl"
    path.write_text('{"comment_id": 1}\n{"comment_id": 1}\n')
    with pytest.raises(AssertionError, match="Duplicate filter"):
        module.passing_ids(path)


def test_comparison_reports_threshold_flip_and_score_drift(tmp_path):
    module = load()
    old = {
        "comment_id": 1,
        "spans": [
            {"start": 0, "end": 4, "title": "Dune", "score": 0.1701},
            {"start": 5, "end": 12, "title": "Hyperion", "score": 0.8},
        ],
    }
    new = {"comment_id": 1, "spans": [old["spans"][1] | {"score": 0.81}]}
    baseline, actual = tmp_path / "baseline.jsonl", tmp_path / "actual.jsonl"
    baseline.write_text(json.dumps(old) + "\n")
    actual.write_text(json.dumps(new) + "\n")
    result = module.compare_proposals(actual, baseline, 0.17)
    assert result["span_changes"] == [
        {"comment_id": 1, "change": "removed", **old["spans"][0]}
    ]
    assert result["baseline_references"] == 2
    assert result["actual_references"] == 1
    assert result["reference_changed_comments"] == [1]
    assert result["max_score_delta"] == pytest.approx(0.01)
    assert result["changed_scores"] == 1
