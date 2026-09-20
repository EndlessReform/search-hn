"""Stable sampling over one frozen positive-mean ranking."""

import hashlib
import json
from typing import Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from search_research.comment_centroids import CommentCentroids
from search_research.comment_rollout_store import now


class PoolSettings(BaseModel):
    seed: int = Field(default=20260919, ge=0, le=2**32 - 1)
    test_fraction: float = Field(default=300 / 1300, ge=0, le=1)


class SamplingRule(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    kind: Literal["rank", "similarity", "random"] = "rank"
    start_rank: int = Field(default=1, ge=1)
    end_rank: int | None = Field(default=None, ge=1)
    start_score: float = Field(default=0.3, ge=-1, le=1)
    count: int = Field(default=1000, ge=1)


class Sampler:
    def __init__(self, explorer, ledger):
        self.explorer, self.ledger = explorer, ledger
        self.centroids = CommentCentroids(explorer, ledger.annotations)

    def create(self, set_id, settings):
        existing = self.ledger.pool(set_id)
        if existing:
            return existing
        labels = self.ledger.annotations.get(set_id)
        query, diagnostics = self.centroids.query(
            labels["comment_ids"], "mean", 0, 0, 100
        )
        anchor = {
            "corpus_id": self.explorer.corpus_id,
            "positive_ids": labels["comment_ids"],
            "excluded_ids": labels["comment_ids"]
            + [n["comment_id"] for n in labels["negatives"]],
            "query": query.tolist(),
            "diagnostics": diagnostics,
        }
        with self.ledger.connect() as db:
            db.execute(
                """INSERT INTO rollout_pools(set_id,created_at,anchor_json,seed,test_fraction)
                VALUES (?,?,?,?,?)""",
                (
                    set_id,
                    now(),
                    json.dumps(anchor),
                    settings.seed,
                    settings.test_fraction,
                ),
            )
        return self.ledger.pool(set_id)

    def add_rule(self, pool, rule):
        if rule.end_rank is not None and rule.end_rank < rule.start_rank:
            raise ValueError("End rank must be at least start rank")
        # Ignore irrelevant controls when identifying identical rules.
        spec = {"kind": rule.kind, "count": rule.count}
        if rule.kind == "rank":
            spec.update(start_rank=rule.start_rank, end_rank=rule.end_rank)
        elif rule.kind == "similarity":
            spec["start_score"] = rule.start_score
        with self.ledger.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO rollout_rules(pool_id,spec_json) VALUES (?,?)",
                (pool["id"], json.dumps(spec, sort_keys=True)),
            )
            db.execute(
                "DELETE FROM rollout_deleted_rules WHERE rule_id IN "
                "(SELECT id FROM rollout_rules WHERE pool_id=? AND spec_json=?)",
                (pool["id"], json.dumps(spec, sort_keys=True)),
            )

    def edit_rule(self, pool, rule_id, rule=None):
        """Edit or retire a rule without deleting paid predictions or sampled IDs."""
        with self.ledger.connect() as db:
            old = db.execute(
                "SELECT * FROM rollout_rules WHERE id=? AND pool_id=? AND id NOT IN (SELECT rule_id FROM rollout_deleted_rules)",
                (rule_id, pool["id"]),
            ).fetchone()
            if old is None:
                raise KeyError("Sampling rule does not exist")
            if rule is None:
                db.execute("INSERT INTO rollout_deleted_rules VALUES (?)", (rule_id,))
                action = "delete"
            else:
                if rule.kind != json.loads(old["spec_json"])["kind"]:
                    raise ValueError("Add a new rule to change its source type")
                if rule.end_rank is not None and rule.end_rank < rule.start_rank:
                    raise ValueError("End rank must be at least start rank")
                spec = {"kind": rule.kind, "count": rule.count}
                if rule.kind == "rank":
                    spec.update(start_rank=rule.start_rank, end_rank=rule.end_rank)
                    if rule.end_rank is not None:
                        spec["count"] = rule.end_rank - rule.start_rank + 1
                elif rule.kind == "similarity":
                    spec["start_score"] = rule.start_score
                payload = json.dumps(spec, sort_keys=True)
                duplicate = db.execute(
                    "SELECT id FROM rollout_rules WHERE pool_id=? AND spec_json=? AND id!=?",
                    (pool["id"], payload, rule_id),
                ).fetchone()
                if duplicate:
                    raise ValueError("An identical rule already exists")
                if payload == old["spec_json"]:
                    return
                db.execute(
                    "UPDATE rollout_rules SET spec_json=?,cursor=0,sampled=0 WHERE id=?",
                    (payload, rule_id),
                )
                action = "edit"
            db.execute(
                "INSERT INTO rollout_rule_changes(rule_id,old_spec,action,at) VALUES (?,?,?,?)",
                (rule_id, old["spec_json"], action, now()),
            )

    def sample(self, pool, rule_id, more=False):
        """Replay is a no-op. Continue advances the saved source cursor explicitly."""
        with self.ledger.connect() as db:
            rule = db.execute(
                "SELECT * FROM rollout_rules WHERE id=? AND pool_id=? AND id NOT IN (SELECT rule_id FROM rollout_deleted_rules)",
                (rule_id, pool["id"]),
            ).fetchone()
            if rule is None:
                raise KeyError("Sampling rule does not exist")
            if rule["sampled"] and not more:
                return {"added": 0, "overlap": 0, "replayed": True}
            spec = json.loads(rule["spec_json"])
            own_count = db.execute(
                "SELECT count(*) FROM rollout_sources WHERE pool_id=? AND rule_id=?",
                (pool["id"], rule_id),
            ).fetchone()[0]
            already = {
                r[0]
                for r in db.execute(
                    "SELECT comment_id FROM rollout_picks WHERE pool_id=?",
                    (pool["id"],),
                )
            }
        anchor = json.loads(pool["anchor_json"])
        (rows, scores, _, _), _ = self.explorer.rank_vector(
            np.asarray(anchor["query"], dtype=np.float32), ("rollout-pool", pool["id"])
        )
        ids = self.explorer.comment_ids[rows]
        order = np.arange(len(ids))
        if spec["kind"] == "random":
            order = np.random.default_rng(pool["seed"] + rule_id).permutation(order)
        elif spec["kind"] == "rank":
            start = spec["start_rank"] - 1
            end = spec["end_rank"] if spec["end_rank"] is not None else len(ids)
            # Explicit Continue moves an interval downward by its width.
            if more and spec["end_rank"] is not None:
                width = end - start
                start += rule["cursor"]
                end = start + width
            order = order[start:end]
        else:
            order = order[scores <= spec["start_score"]]
        excluded = set(anchor["excluded_ids"])
        # Also exclude labels/examples added since freezing the anchor.
        current = self.ledger.annotations.get(pool["set_id"])
        excluded.update(current["comment_ids"])
        excluded.update(n["comment_id"] for n in current["negatives"])
        cursor = (
            0
            if spec["kind"] == "rank" and spec.get("end_rank") is not None
            else rule["cursor"]
        )
        added = overlap = 0
        sources = []
        records = []
        target = (
            spec["count"]
            if more or spec["kind"] == "rank"
            else max(0, spec["count"] - own_count)
        )
        while cursor < len(order) and added < target:
            pos = int(order[cursor])
            cursor += 1
            cid = int(ids[pos])
            if cid in excluded:
                continue
            sources.append((pool["id"], cid, rule_id))
            if cid in already:
                overlap += 1
                continue
            split_value = (
                int.from_bytes(
                    hashlib.sha256(f"{pool['seed']}:{cid}".encode()).digest()[:8], "big"
                )
                / 2**64
            )
            records.append(
                (
                    pool["id"],
                    cid,
                    pos + 1,
                    float(scores[pos]),
                    "test" if split_value < pool["test_fraction"] else "train",
                    now(),
                )
            )
            already.add(cid)
            added += 1
        with self.ledger.connect() as db:
            db.executemany(
                """INSERT OR IGNORE INTO rollout_picks
                (pool_id,comment_id,rank,score,split,picked_at) VALUES (?,?,?,?,?,?)""",
                records,
            )
            db.executemany(
                "INSERT OR IGNORE INTO rollout_sources VALUES (?,?,?)", sources
            )
            stored_cursor = (
                rule["cursor"] + cursor
                if spec["kind"] == "rank" and spec.get("end_rank") is not None
                else cursor
            )
            db.execute(
                "UPDATE rollout_rules SET cursor=?,sampled=1 WHERE id=?",
                (stored_cursor, rule_id),
            )
        return {
            "added": added,
            "overlap": overlap,
            "replayed": False,
            "exhausted": cursor >= len(order),
        }
