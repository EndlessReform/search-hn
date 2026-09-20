"""Sampling identity, split stability, durable attempts and explicit invalidation."""

import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from search_research.comment_annotations import AnnotationStore
from search_research.comment_explorer import CommentExplorer
from search_research.comment_explorer_web import create_app
from search_research.comment_rollout_store import RolloutStore
from search_research.comment_sampling import PoolSettings, Sampler, SamplingRule
from test_comment_explorer import corpus as corpus_fixture

corpus = corpus_fixture


def setup_pool(corpus):
    explorer = CommentExplorer(corpus, "http://unused", threads=1)
    annotations = AnnotationStore(corpus / "annotations.sqlite", explorer.corpus_id)
    sid = annotations.create("Rollout test")
    annotations.member(sid, 10, True)
    ledger = RolloutStore(annotations)
    sampler = Sampler(explorer, ledger)
    pool = sampler.create(sid, PoolSettings())
    return explorer, annotations, ledger, sampler, pool


def test_sampling_overlap_replay_continue_and_archive(corpus):
    _explorer, annotations, ledger, sampler, pool = setup_pool(corpus)
    sampler.add_rule(pool, SamplingRule(kind="rank", count=1))
    sampler.add_rule(pool, SamplingRule(kind="rank", count=1))
    rule = ledger.summary(pool["set_id"])["rules"][0]
    assert len(ledger.summary(pool["set_id"])["rules"]) == 1
    first = sampler.sample(pool, rule["id"])
    assert first["added"] == 1
    with ledger.connect() as db:
        original = [dict(r) for r in db.execute("SELECT * FROM rollout_picks")]
    assert sampler.sample(pool, rule["id"])["replayed"]
    assert sampler.sample(pool, rule["id"], more=True)["added"] == 1
    sampler.add_rule(pool, SamplingRule(kind="similarity", start_score=1, count=10))
    second = ledger.summary(pool["set_id"])["rules"][1]
    overlap = sampler.sample(pool, second["id"])
    assert overlap["added"] == 0 and overlap["overlap"] == 2
    sampler.add_rule(pool, SamplingRule(kind="random", count=10))
    third = ledger.summary(pool["set_id"])["rules"][2]
    assert sampler.sample(pool, third["id"])["overlap"] == 2
    with ledger.connect() as db:
        assert db.execute("SELECT count(*) FROM rollout_picks").fetchone()[0] == 2
        assert (
            db.execute(
                "SELECT split FROM rollout_picks WHERE comment_id=?",
                (original[0]["comment_id"],),
            ).fetchone()[0]
            == original[0]["split"]
        )
        assert db.execute("SELECT count(*) FROM rollout_sources").fetchone()[0] == 6
    ledger.invalidate(pool["set_id"])
    assert ledger.pool(pool["set_id"]) is None
    replacement = sampler.create(pool["set_id"], PoolSettings())
    assert replacement["id"] != pool["id"]
    assert annotations.get(pool["set_id"])["comment_ids"] == [10]


def test_rank_interval_and_similarity_start(corpus):
    _, _, ledger, sampler, pool = setup_pool(corpus)
    sampler.add_rule(
        pool, SamplingRule(kind="rank", start_rank=2, end_rank=2, count=10)
    )
    rule = ledger.summary(pool["set_id"])["rules"][0]
    assert sampler.sample(pool, rule["id"])["added"] == 1
    assert sampler.sample(pool, rule["id"], more=True)["added"] == 1
    with pytest.raises(ValueError, match="End rank"):
        sampler.add_rule(pool, SamplingRule(start_rank=5, end_rank=3))
    ledger.invalidate(pool["set_id"])
    pool = sampler.create(pool["set_id"], PoolSettings())
    sampler.add_rule(pool, SamplingRule(kind="similarity", start_score=0.1, count=10))
    rule = ledger.summary(pool["set_id"])["rules"][0]
    sampler.sample(pool, rule["id"])
    with ledger.connect() as db:
        assert all(
            r[0] <= 0.1
            for r in db.execute(
                "SELECT score FROM rollout_picks WHERE pool_id=?", (pool["id"],)
            )
        )


def test_worker_api_accept_retry_export_and_interrupt(corpus, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-only")
    calls = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.chat = SimpleNamespace(completions=self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def create(self, **kwargs):
            calls.append(kwargs)
            result = {"is_positive": True, "taxonomy": "book"}
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        finish_reason="stop",
                        message=SimpleNamespace(
                            refusal=None, content=json.dumps(result)
                        ),
                    )
                ],
                model_dump=lambda **kwargs: {
                    "id": "fake",
                    "model": "test",
                    "usage": {"cost": 0.001},
                    "choices": [],
                },
            )

    monkeypatch.setattr(
        "search_research.comment_rollout_worker.AsyncOpenAI", FakeClient
    )
    explorer = CommentExplorer(corpus, "http://unused", threads=1)
    app = create_app(explorer)
    client = TestClient(app)
    sid = client.post("/api/sets", json={"name": "Books"}).json()["id"]
    base = f"/api/sets/{sid}"
    client.put(base + "/members/10", json={})
    client.put(
        base + "/classifier",
        json={
            "description": "Book recommendations",
            "taxonomy": [
                {"name": "book", "description": "A book", "is_positive": True}
            ],
        },
    )
    url = base + "/rollouts"
    assert client.post(url + "/pool", json={}).status_code == 200
    state = client.get(url).json()
    for rule in state["rules"]:
        assert (
            client.post(url + f"/rules/{rule['id']}/sample", json={}).status_code == 200
        )
    # Sampling order, not similarity, controls a partial source batch.
    with app.state.rollout_worker.ledger.connect() as db:
        db.execute(
            "UPDATE rollout_picks SET picked_at='2000-01-01' WHERE comment_id=20"
        )
    assert client.post(url + "/run", json={"limit": 50}).status_code == 200
    app.state.rollout_worker.future.result(timeout=5)
    state = client.get(url).json()
    assert state["counts"] == {"accepted": 2}
    assert len(calls) == 2
    with app.state.rollout_worker.ledger.connect() as db:
        assert (
            db.execute(
                "SELECT comment_id FROM classifier_attempts ORDER BY id LIMIT 1"
            ).fetchone()[0]
            == 20
        )
    assert state["label_counts"] == {"positive": 2}
    assert sum(state["label_splits"]["positive"].values()) == 2
    assert state["label_splits"]["negative"] == {"train": 0, "test": 0}
    assert client.post(url + "/run", json={"limit": 50}).status_code == 400
    exported = [
        json.loads(line) for line in client.get(url + "/export").text.splitlines()
    ]
    assert len(exported) == 2 and exported[0]["provenance"]["prompt"]
    cid = exported[0]["comment_id"]
    client.post(url + f"/picks/{cid}", json={"action": "reject"})
    assert len(client.get(url + "/export").text.splitlines()) == 1
    client.post(url + f"/picks/{cid}", json={"action": "retry"})
    client.post(url + "/run", json={"limit": 50})
    app.state.rollout_worker.future.result(timeout=5)
    assert len(calls) == 3
    assert client.get(url + "/predictions?label=positive").json()["total"] == 2
    assert (
        client.post(url + "/invalidate", json={"confirmation": "no"}).status_code == 400
    )
    assert (
        client.post(
            url + "/invalidate", json={"confirmation": "INVALIDATE ALL"}
        ).status_code
        == 200
    )
    assert client.get(base).json()["comment_ids"] == [10]
    assert (
        client.get(base + "/classifier").json()["draft"]["description"]
        == "Book recommendations"
    )
    app.state.rollout_worker.executor.shutdown()


def test_recovery_preserves_unknown_attempts(corpus):
    _, _, ledger, _, pool = setup_pool(corpus)
    with ledger.connect() as db:
        run = db.execute(
            """INSERT INTO classifier_runs(pool_id,created_at,status,model,snapshot_json,requested)
            VALUES (?,'now','running','test','{}',1)""",
            (pool["id"],),
        ).lastrowid
        attempt = db.execute(
            "INSERT INTO classifier_attempts(run_id,comment_id,status) VALUES (?,20,'running')",
            (run,),
        ).lastrowid
        db.execute(
            """INSERT INTO rollout_picks(pool_id,comment_id,rank,score,split,picked_at,status,latest_attempt)
            VALUES (?,20,2,0.5,'train','now','running',?)""",
            (pool["id"], attempt),
        )
    reopened = RolloutStore(ledger.annotations)
    assert reopened.summary(pool["set_id"])["counts"] == {"interrupted": 1}
    assert reopened.summary(pool["set_id"])["runs"][0]["status"] == "interrupted"


def test_edit_and_delete_rules_preserve_picks(corpus):
    _, _, ledger, sampler, pool = setup_pool(corpus)
    sampler.add_rule(pool, SamplingRule(start_rank=1, end_rank=2, count=2))
    rule = ledger.summary(pool["set_id"])["rules"][0]
    assert sampler.sample(pool, rule["id"])["added"] == 1
    sampler.edit_rule(pool, rule["id"], SamplingRule(start_rank=1, end_rank=3, count=2))
    assert sampler.sample(pool, rule["id"])["added"] == 1
    assert sampler.sample(pool, rule["id"])["replayed"]
    sampler.edit_rule(pool, rule["id"])
    assert ledger.summary(pool["set_id"])["rules"] == []
    with ledger.connect() as db:
        assert db.execute("SELECT count(*) FROM rollout_picks").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM rollout_sources").fetchone()[0] == 2
        assert (
            db.execute("SELECT count(*) FROM rollout_rule_changes").fetchone()[0] == 2
        )
    with pytest.raises(KeyError):
        sampler.sample(pool, rule["id"])
