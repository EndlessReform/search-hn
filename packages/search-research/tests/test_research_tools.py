"""Failure-path regressions for the standalone research tools; no GPU or network."""

import asyncio
import importlib
import io
import json
from pathlib import Path

import httpx
import polars as pl
import pytest
from search_research.sovereign_e2e_report import validate_observations, validate_traces
from search_research.sovereign_repository import ARMS


@pytest.fixture
def tools_path(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "tools"))


def test_analysis_scripts_are_import_safe(tools_path, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["pytest", "--unrelated-option"])
    for name in ("pplx_vm_latency_hybrid", "pplx_ef1000_accuracy"):
        module = importlib.import_module(name)
        assert callable(module.run)
    assert list(tmp_path.iterdir()) == []


def test_quantization_rejects_vectors_that_round_to_zero(tools_path):
    from pplx_vllm_gate import encode

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, json={"data": [{"index": 0, "embedding": [1e-9] * 1024}]}
        )
    )
    with (
        httpx.Client(base_url="http://unused", transport=transport) as client,
        pytest.raises(AssertionError, match="zero embedding"),
    ):
        encode(client, ["synthetic"])


def test_probe_budget_stop_without_dispatch_is_not_an_index_error(tools_path, tmp_path):
    from openrouter_capacity import Probe

    # No HTTP client is needed: the budget must stop before call() is reached.
    probe = Probe.__new__(Probe)
    probe.root = tmp_path
    probe.pool = [{"tokens": 100}]
    probe.cursor = 0
    probe.charge = 0
    probe.dollar_cap = 0.000001
    probe.request_cap = 1
    probe.log = io.StringIO()
    probe.summaries = []
    result = asyncio.run(probe.stage(1, 10, 1))
    assert result["stop_reason"] == "dollar_cap"
    assert result["requests"] == 0


def test_final_comparison_rejects_missing_or_duplicate_cases(tmp_path):
    rows = []
    for arm in ARMS:
        directory = tmp_path / arm
        directory.mkdir()
        (directory / "eval.jsonl").write_text(json.dumps({"id": 1}) + "\n")
        rows.extend(
            {"arm": arm, "case": f"1-{style}"} for style in ("entity", "paraphrase")
        )
    frame = pl.DataFrame(rows)
    validate_observations(frame, tmp_path)
    with pytest.raises(AssertionError, match="Incomplete/duplicate"):
        validate_observations(frame.head(len(rows) - 1), tmp_path)
    duplicate = pl.concat([frame.head(len(rows) - 1), frame.slice(len(rows) - 2, 1)])
    with pytest.raises(AssertionError, match="Case coverage differs"):
        validate_observations(duplicate, tmp_path)


def test_final_report_rejects_unfinished_and_duplicate_traces(tmp_path):
    path = tmp_path / "trace.jsonl"
    start = {"event": "start", "metadata": {"case": "1-entity"}}
    path.write_text(json.dumps(start) + "\n")
    with pytest.raises(AssertionError, match="Nonterminal"):
        validate_traces([path])
    with path.open("a") as stream:
        stream.write(json.dumps({"event": "complete"}) + "\n")
    validate_traces([path])
    with pytest.raises(AssertionError, match="Duplicate terminal trace"):
        validate_traces([path, path])
