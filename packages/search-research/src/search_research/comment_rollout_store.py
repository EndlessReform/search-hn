"""SQLite ledger for sampled pools and append-only classifier attempts."""

import json
from datetime import UTC, datetime


def now():
    return datetime.now(UTC).isoformat()


class RolloutStore:
    def __init__(self, annotations):
        self.annotations = annotations
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS rollout_pools(
                    id INTEGER PRIMARY KEY, set_id INTEGER NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
                    anchor_json TEXT NOT NULL, seed INTEGER NOT NULL,
                    test_fraction REAL NOT NULL);
                CREATE UNIQUE INDEX IF NOT EXISTS active_rollout_pool
                    ON rollout_pools(set_id) WHERE active=1;
                CREATE TABLE IF NOT EXISTS rollout_rules(
                    id INTEGER PRIMARY KEY, pool_id INTEGER NOT NULL,
                    spec_json TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0,
                    sampled INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(pool_id,spec_json));
                CREATE TABLE IF NOT EXISTS rollout_rule_changes(
                    id INTEGER PRIMARY KEY, rule_id INTEGER NOT NULL,
                    old_spec TEXT NOT NULL, action TEXT NOT NULL, at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS rollout_deleted_rules(rule_id INTEGER PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS rollout_picks(
                    pool_id INTEGER NOT NULL, comment_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL, score REAL NOT NULL, split TEXT NOT NULL,
                    picked_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                    latest_attempt INTEGER, PRIMARY KEY(pool_id,comment_id));
                CREATE TABLE IF NOT EXISTS rollout_sources(
                    pool_id INTEGER NOT NULL, comment_id INTEGER NOT NULL,
                    rule_id INTEGER NOT NULL, PRIMARY KEY(pool_id,comment_id,rule_id));
                CREATE TABLE IF NOT EXISTS classifier_runs(
                    id INTEGER PRIMARY KEY, pool_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
                    model TEXT NOT NULL, snapshot_json TEXT NOT NULL,
                    requested INTEGER NOT NULL, error TEXT);
                CREATE TABLE IF NOT EXISTS classifier_attempts(
                    id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL,
                    comment_id INTEGER NOT NULL, started_at TEXT, finished_at TEXT,
                    status TEXT NOT NULL, response_json TEXT, label_json TEXT, error TEXT,
                    UNIQUE(run_id,comment_id));
                CREATE TABLE IF NOT EXISTS rollout_reviews(
                    id INTEGER PRIMARY KEY, pool_id INTEGER NOT NULL,
                    comment_id INTEGER NOT NULL, action TEXT NOT NULL, at TEXT NOT NULL);
            """)
            # A killed request may already have incurred a charge. Preserve it;
            # only an explicit retry can dispatch that comment again.
            db.execute("""UPDATE rollout_picks SET status='interrupted' WHERE
                latest_attempt IN (SELECT id FROM classifier_attempts
                WHERE status IN ('queued','running'))""")
            db.execute("""UPDATE classifier_attempts SET status='interrupted'
                WHERE status IN ('queued','running')""")
            db.execute(
                """UPDATE classifier_runs SET status='interrupted',finished_at=?
                WHERE status='running'""",
                (now(),),
            )

    def connect(self):
        return self.annotations.connect()

    def pool(self, set_id):
        self.annotations.get(set_id)
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM rollout_pools WHERE set_id=? AND active=1", (set_id,)
            ).fetchone()
            return dict(row) if row else None

    def summary(self, set_id):
        pool = self.pool(set_id)
        if not pool:
            return {"pool": None, "rules": [], "runs": [], "counts": {}}
        with self.connect() as db:
            rules = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM rollout_rules WHERE pool_id=? AND id NOT IN (SELECT rule_id FROM rollout_deleted_rules) ORDER BY id",
                    (pool["id"],),
                )
            ]
            for rule in rules:
                rule["spec"] = json.loads(rule.pop("spec_json"))
                if (
                    rule["spec"]["kind"] == "rank"
                    and rule["spec"].get("end_rank") is not None
                ):
                    rule["spec"]["end_rank"] = max(
                        rule["spec"]["end_rank"],
                        rule["spec"]["start_rank"] + rule["cursor"] - 1,
                    )
                rule["picked"] = db.execute(
                    "SELECT count(*) FROM rollout_sources WHERE rule_id=?",
                    (rule["id"],),
                ).fetchone()[0]
            runs = [
                dict(r)
                for r in db.execute(
                    """SELECT id,model,status,created_at,finished_at,requested,error
                FROM classifier_runs WHERE pool_id=? ORDER BY id DESC""",
                    (pool["id"],),
                )
            ]
            for run in runs:
                run["counts"] = dict(
                    db.execute(
                        "SELECT status,count(*) FROM classifier_attempts WHERE run_id=? GROUP BY status",
                        (run["id"],),
                    ).fetchall()
                )
                run["reported_cost"] = db.execute(
                    """
                    SELECT sum(json_extract(response_json,'$.usage.cost'))
                    FROM classifier_attempts WHERE run_id=?""",
                    (run["id"],),
                ).fetchone()[0]
            counts = dict(
                db.execute(
                    "SELECT status,count(*) FROM rollout_picks WHERE pool_id=? GROUP BY status",
                    (pool["id"],),
                ).fetchall()
            )
        with self.connect() as db:
            # Count current accepted picks, not historical attempts or rejected labels.
            label_splits = {label: {"train": 0, "test": 0} for label in ("positive", "negative")}
            for label, split, count in db.execute(
                """
                SELECT CASE WHEN json_extract(a.label_json,'$.is_positive') THEN 'positive'
                       ELSE 'negative' END,p.split,count(*)
                FROM rollout_picks p JOIN classifier_attempts a ON a.id=p.latest_attempt
                WHERE p.pool_id=? AND p.status='accepted' GROUP BY 1,2""",
                (pool["id"],),
            ):
                label_splits[label][split] = count
            label_counts = {label: sum(splits.values()) for label, splits in label_splits.items() if sum(splits.values())}
        pool["anchor"] = json.loads(pool.pop("anchor_json"))
        return {
            "pool": pool,
            "rules": rules,
            "runs": runs,
            "counts": counts,
            "label_counts": label_counts,
            "label_splits": label_splits,
        }

    def invalidate(self, set_id):
        pool = self.pool(set_id)
        if pool:
            with self.connect() as db:
                if db.execute(
                    "SELECT 1 FROM classifier_runs WHERE pool_id=? AND status='running'",
                    (pool["id"],),
                ).fetchone():
                    raise ValueError(
                        "Stop or finish the running batch before invalidating"
                    )
                db.execute(
                    "UPDATE rollout_pools SET active=0 WHERE id=?", (pool["id"],)
                )
