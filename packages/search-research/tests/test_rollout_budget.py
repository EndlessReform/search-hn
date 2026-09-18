"""No-network spending and restart contracts: unknown charges are never forgotten."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from search_agent.journal import Journal
from search_research.rollout_budget import (
    BilledModel,
    Budget,
    BudgetPaused,
    MeteredHooks,
)


def test_outstanding_reservations_survive_resume(tmp_path):
    path = tmp_path / "budget.jsonl"
    budget = Budget(path, 0.02)
    id = budget.reserve(10000, "pplx/case")
    second = budget.reserve(10000, "qwen/case")
    with pytest.raises(BudgetPaused):
        budget.reserve(10000, "nemotron/case")
    budget.set(id, 0.001, "reported", "pplx/case")
    unknown = budget.charges[second]["usd"]
    budget.journal.close()
    resumed = Budget(path, 0.03)
    assert resumed.total == pytest.approx(0.001 + unknown)
    assert resumed.summary()["unknown_reserved_usd"] == unknown
    assert resumed.reserve(10000, "nemotron/case")
    resumed.journal.close()


def test_paused_dispatch_does_not_release_pending_charge(tmp_path):
    budget = Budget(tmp_path / "budget.jsonl")
    budget.reserve(10000, "pplx/case")
    total = budget.total
    budget.pause("OpenRouter HTTP 402")
    with pytest.raises(BudgetPaused, match="402"):
        budget.reserve(1, "qwen/case")
    assert budget.total == total
    budget.journal.close()


def test_streamed_actual_cost_replaces_reservation(tmp_path):
    budget = Budget(tmp_path / "budget.jsonl")
    journal = Journal(tmp_path / "trace.jsonl")
    hooks = MeteredHooks(journal, budget, "pplx/case")
    hooks.id = budget.reserve(10000, hooks.case)
    usage = SimpleNamespace(model_dump=lambda: {"cost": 0.00123})
    event = SimpleNamespace(
        type="response.completed",
        response=SimpleNamespace(id="response-1", usage=usage),
    )

    class Model:
        async def stream_response(self):
            yield event

    async def consume():
        return [e async for e in BilledModel(Model(), hooks).stream_response()]

    assert asyncio.run(consume()) == [event]
    assert budget.total == 0.00123
    assert budget.summary()["unknown_reserved_usd"] == 0
    journal.close()
    budget.journal.close()


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_one_failed_conversation_does_not_cancel_sibling(
    tmp_path, monkeypatch, cleanup_fails
):
    from search_research import sovereign_rollouts as runner

    source = {
        "id": 1,
        "cohort": "recent",
        "questions": {"entity_question": "synthetic query"},
    }
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "eval.jsonl").write_text((json.dumps(source) + "\n") * 98)
    monkeypatch.setattr(runner, "TRACES", inputs)
    monkeypatch.setattr(runner, "prepare", lambda root: None)
    monkeypatch.setattr(
        runner,
        "manifest",
        lambda root: {
            "base_url": "http://unused",
            "eval_sha256": "dataset",
            "source_sha256": "code",
        },
    )
    monkeypatch.setattr(
        runner,
        "cases",
        lambda dataset, smoke: [(source, "entity", "pplx"), (source, "entity", "qwen")],
    )
    monkeypatch.setenv("OPENROUTER_API_KEY", "not-a-real-key")
    monkeypatch.setattr(runner, "LocalQueries", lambda arm: arm)
    closed = []

    class Repository:
        def __init__(self, *args, **kwargs):
            self.stats = {}
            self.arm = kwargs["embedding_provider"]

    class Runtime:
        def __init__(self, **kwargs):
            self.repository = kwargs["repository"]
            self.client = SimpleNamespace(max_retries=0)

        async def close(self):
            closed.append(self.repository.arm)
            if cleanup_fails:
                raise RuntimeError("Synthetic cleanup failure")

    async def headless(runtime, prompt, path, **kwargs):
        journal = Journal(path)
        journal.write("start")
        if runtime.repository.arm == "pplx":
            journal.write("error", error_type="ConnectionError")
            journal.close()
            raise ConnectionError("Synthetic transport failure")
        await asyncio.sleep(0.01)
        journal.write("complete")
        journal.close()

    monkeypatch.setattr(runner, "SemanticStoryRepository", Repository)
    monkeypatch.setattr(runner, "SearchRuntime", Runtime)
    monkeypatch.setattr(runner, "run_headless", headless)
    root = tmp_path / "run"
    asyncio.run(
        runner.run(SimpleNamespace(root=root, concurrency=2, budget_usd=8, smoke=False))
    )
    outcomes = {
        r["key"]: r["status"]
        for r in runner.read_jsonl(root / "outcomes.jsonl")
        if r["event"] == "outcome"
    }
    assert outcomes == {
        "pplx/1-entity": "infrastructure_error",
        "qwen/1-entity": "complete",
    }
    assert sorted(closed) == ["pplx", "qwen"]
    assert len(list((root / "pplx" / "infrastructure-attempts").glob("*.jsonl"))) == 1
    # A terminal answer must stay terminal even when closing its client failed.
    asyncio.run(
        runner.run(SimpleNamespace(root=root, concurrency=2, budget_usd=8, smoke=False))
    )
    assert closed.count("qwen") == 1
