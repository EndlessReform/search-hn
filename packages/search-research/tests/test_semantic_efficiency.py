"""First evidence must be consumed in a successful model request."""

import json

from search_research.semantic_efficiency import measure


def test_first_exposure_and_stopping():
    call = {
        "type": "function_call",
        "call_id": "s",
        "name": "fetch_stories",
        "arguments": '{"query":"anchor"}',
    }
    result = {
        "type": "function_call_output",
        "call_id": "s",
        "output": json.dumps({"results": [{"id": 42}]}),
    }
    events = [
        {
            "event": "start",
            "model": "test",
            "metadata": {"target_id": 42, "case": "x", "style": "entity"},
            "prompt": "find",
        },
        {"event": "model_input", "request": 1, "items": []},
        {"event": "model_output", "request": 1, "items": [call]},
        {"event": "model_input", "request": 2, "items": [call, result]},
        {"event": "model_output", "request": 2, "items": []},
        {"event": "model_input", "request": 3, "items": [call, result]},
        {"event": "model_output", "request": 3, "items": []},
        {"event": "complete", "final": "answer", "elapsed": 1},
    ]
    row = measure(events)
    assert row["turns"] == 3
    assert row["first_exposure_turn"] == 2
    assert row["turns_after_exposure"] == 1
    assert row["lists_consumed_at_exposure"] == 1
    assert row["exposure_by_turn_2"]
    # The same returned result without any successful consuming response is a miss.
    row = measure(events[:4] + [events[-1]])
    assert row["first_exposure_turn"] is None
    assert row["completed_without_target"]
