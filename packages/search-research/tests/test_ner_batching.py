"""Check offline scheduling cannot change window identity or overlap semantics."""

import pytest
from search_research import ner_batching as batching


def test_token_schedule_restores_order_and_never_truncates():
    windows = [
        batching.Window(1, 0, "long", 900),
        batching.Window(2, 0, "short", 20),
        batching.Window(1, 5, "middle", 200),
    ]
    calls = []

    class Model:
        def inference(self, texts, labels, **kwargs):
            calls.append((texts, kwargs["batch_size"]))
            return [[{"text": text}] for text in texts]

    predictions = batching.predict_windows(
        Model(), windows, ["person"], 0.3, True, [(128, 64), (256, 32), (1536, 8)]
    )
    assert calls == [(["short"], 64), (["middle"], 32), (["long"], 8)]
    assert [row[0]["text"] for row in predictions] == ["long", "short", "middle"]
    with pytest.raises(AssertionError, match="exceeds"):
        batching.predict_windows(Model(), windows, ["person"], 0.3, True, [(512, 16)])


def test_person_overlap_keeps_last_source_window_after_reordering(monkeypatch):
    windows = [batching.Window(1, 0, "xx Ada", 100), batching.Window(1, 3, "Ada", 10)]
    monkeypatch.setattr(batching, "prepare_windows", lambda *args: windows)

    class Model:
        def inference(self, texts, labels, **kwargs):
            assert texts == ["Ada", "xx Ada"]
            return [
                [{"text": "Ada", "start": 0, "end": 3, "score": 0.4}],
                [{"text": "Ada", "start": 3, "end": 6, "score": 0.9}],
            ]

    assert batching.person_records(Model(), [(1, "xx Ada"), (2, "")], [(1536, 16)]) == [
        (1, [{"text": "Ada", "start": 3, "end": 6, "score": 0.4}]),
        (2, []),
    ]
