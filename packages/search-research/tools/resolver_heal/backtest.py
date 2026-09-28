"""Replay rank-one shortcut gates against saved Luna outputs, without API calls.

Agreement measures imitation of Luna, not correctness. Every original reference
remains in the denominator, including repairs, split references and abstentions.
"""

import json
import os
import sqlite3
from pathlib import Path


def main():
    root = Path(os.environ["RESOLVER_RUN_ROOT"])
    db = sqlite3.connect(f"file:{root}/checkpoint.sqlite?mode=ro", uri=True)
    rows = []
    for ident, rp, sp in db.execute(
        "SELECT r.id,r.payload,s.payload FROM rankings r JOIN selections s USING(id) WHERE s.model='luna'"
    ):
        rank, result = json.loads(rp), json.loads(sp)
        pairs = sorted(zip(rank["scores"], rank["ids"], strict=True), reverse=True)
        if not pairs:
            continue
        score, best = pairs[0]
        gap = score - pairs[1][0] if len(pairs) > 1 else 0
        rows.append((ident, score, gap, best, result["selection"]["work_ids"]))
    total = db.execute("SELECT count(*) FROM refs").fetchone()[0]
    results = []
    for minimum in (None, 10):
        for gap in (1, 2, 3, 5):
            accepted = [
                r for r in rows if r[2] >= gap and (minimum is None or r[1] >= minimum)
            ]
            matching = sum(r[4] == [r[3]] for r in accepted)
            results.append(
                {
                    "minimum_score": minimum,
                    "minimum_gap": gap,
                    "accepted": len(accepted),
                    "total": total,
                    "same_final_work_ids": matching,
                    "different_final_work_ids": len(accepted) - matching,
                    "luna_abstained": sum(not r[4] for r in accepted),
                }
            )
    (root / "gate-backtest.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
