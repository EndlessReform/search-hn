"""Write status and an optional consistent portable snapshot of the live run.

Use SQLite's backup API, never copy a live WAL database directly. The checkpoint
can be copied to another machine while workers keep committing to run.sqlite.
"""

import argparse
import json
import sqlite3
import time

from common import RUN, connect, digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", action="store_true")
    args = parser.parse_args()
    db = connect()
    counts = {
        name: db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        for name in ["refs", "documents", "rankings", "failures"]
    }
    arms = []
    for model, n, abstain, cost, inputs, outputs in db.execute("""
        SELECT model,count(*),sum(json_extract(payload,'$.selection.work_id') IS NULL),
          sum(json_extract(payload,'$.response.usage.cost')),
          sum(json_extract(payload,'$.response.usage.prompt_tokens')),
          sum(json_extract(payload,'$.response.usage.completion_tokens'))
        FROM selections GROUP BY model"""):
        arms.append(
            {
                "model": model,
                "completed": n,
                "abstain": abstain,
                "reported_cost": cost,
                "input_tokens": inputs,
                "output_tokens": outputs,
            }
        )
    # Include every recorded paid attempt, without double-counting successful
    # responses copied into selections. Pre-recovery successes have no attempts row.
    has_attempts = db.execute(
        "SELECT 1 FROM sqlite_master WHERE name='attempts'"
    ).fetchone()
    usage_source = "SELECT model,payload FROM selections"
    if has_attempts:
        usage_source = """SELECT model,payload FROM attempts UNION ALL
        SELECT s.model,s.payload FROM selections s WHERE NOT EXISTS
          (SELECT 1 FROM attempts a WHERE a.id=s.id AND a.model=s.model)"""
    spend = [
        dict(
            zip(
                [
                    "model",
                    "recorded_attempts",
                    "reported_cost",
                    "input_tokens",
                    "output_tokens",
                ],
                row,
            )
        )
        for row in db.execute(f"""
        SELECT model,count(*),sum(json_extract(payload,'$.response.usage.cost')),
          sum(json_extract(payload,'$.response.usage.prompt_tokens')),
          sum(json_extract(payload,'$.response.usage.completion_tokens'))
        FROM ({usage_source}) GROUP BY model""")
    ]
    # Native responses report tokens rather than dollars. Retain an explicitly
    # named off-peak estimate; this run is on Saturday, when that rate applies.
    native = db.execute(f"""SELECT
        sum(json_extract(payload,'$.response.usage.prompt_cache_hit_tokens')),
        sum(json_extract(payload,'$.response.usage.prompt_cache_miss_tokens')),
        sum(json_extract(payload,'$.response.usage.completion_tokens'))
        FROM ({usage_source}) WHERE model='deepseek-native'""").fetchone()
    native_cost = None
    if native[0] is not None:
        native_cost = (native[0] * 0.003 + native[1] * 0.15 + native[2] * 0.6) / 1e6
    result = {
        "created_unix": time.time(),
        "counts": counts,
        "arms": arms,
        "all_recorded_attempt_usage": spend,
        "native_offpeak_cost_estimate": native_cost,
        "status": {
            s: json.loads(p) for s, p in db.execute("SELECT stage,payload FROM status")
        },
    }
    if args.backup:
        temporary = RUN / "checkpoint.tmp.sqlite"
        target = sqlite3.connect(temporary)
        db.backup(target, pages=4096)
        assert target.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        target.close()
        temporary.replace(RUN / "checkpoint.sqlite")
        result["checkpoint_sha256"] = digest(RUN / "checkpoint.sqlite")
    (RUN / "progress.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
