"""Auditable retrieval metrics and an offline, filterable trajectory explorer.

Only function outputs present in an actual model_input count as exposure. IDs in
the question, a fabricated citation, or an unconsumed final tool output do not.
NDCG uses one known relevant anchor: alternate valid stories remain unjudged.
"""

import json
import math
import re
from itertools import combinations
from pathlib import Path

import polars as pl

from search_research.dataset import read_jsonl


def evaluate(events):
    """Extract ordered, consumed search lists and answer citations from a journal."""
    start = events[0]
    target = start["metadata"]["target_id"]
    calls, consumed, queries, pooled = {}, set(), [], []
    comment_stories = set()
    target_comment_ids = set()
    attempts = []
    for event in events:
        if event["event"] == "model_output":
            attempts.extend(
                {"request": event["request"], **item}
                for item in event["items"]
                if item.get("type") == "function_call"
            )
    model_requests = 0
    successful_requests = {e["request"] for e in events if e["event"] == "model_output"}
    for event in events:
        if (
            event["event"] != "model_input"
            or event["request"] not in successful_requests
        ):
            continue
        model_requests += 1
        for item in event["items"]:
            if item.get("type") == "function_call":
                calls[item["call_id"]] = item
        for item in event["items"]:
            if (
                item.get("type") != "function_call_output"
                or item["call_id"] in consumed
            ):
                continue
            call_id = item["call_id"]
            consumed.add(call_id)
            call = calls.get(call_id)
            if call is None or call["name"] not in (
                "fetch_stories",
                "fetch_top_stories_for_date",
                "fetch_top_comments",
            ):
                continue
            try:
                output = json.loads(item["output"])
                args = json.loads(call["arguments"])
            except (json.JSONDecodeError, TypeError):
                continue  # Tool errors are model-visible text, not retrieval lists.
            if call["name"] == "fetch_top_comments":
                for story in output.get("stories", [output]):
                    comment_stories.add(story["story_id"])
                    if story["story_id"] == target:
                        target_comment_ids.update(c["id"] for c in story["comments"])
                continue
            batches = output.get("queries", [output])
            for batch in batches:
                results = batch.get("results", batch.get("stories", []))
                ids = [r["id"] for r in results]
                rank = ids.index(target) + 1 if target in ids else None
                queries.append(
                    {
                        "query": batch.get("query"),
                        "tool": call["name"],
                        "arguments": args,
                        "results": results,
                        "target_rank": rank,
                        "request": event["request"],
                    }
                )
                for story_id in ids:
                    if story_id not in pooled:
                        pooled.append(story_id)
    final = next((e["final"] for e in reversed(events) if e["event"] == "complete"), "")
    terminal = events[-1]
    row = {
        "model": start["model"],
        **start["metadata"],
        "prompt": start["prompt"],
        "status": terminal["event"]
        if terminal["event"] in ("complete", "error")
        else "in_progress",
        "elapsed": terminal.get("elapsed"),
        "queries": queries,
        "attempts": attempts,
        "final": final,
        "model_requests": model_requests,
        "exposed": target in pooled or target in comment_stories,
        "comments_exposed": target in comment_stories,
        "cited_target_comment": bool(
            target_comment_ids.intersection(
                int(value) for value in re.findall(r"【comment:(\d+)】", final)
            )
        ),
        "query_count": len(queries),
        "cited": bool(re.search(r"【story:" + str(target) + r"】", final)),
        "pooled_rank": pooled.index(target) + 1 if target in pooled else None,
        "error": terminal.get("error"),
    }
    for field in ("input_tokens", "output_tokens", "total_tokens"):
        row[field] = sum(
            e.get("usage", {}).get(field, 0)
            for e in events
            if e["event"] == "model_output"
        )
    row["cited_evidence"] = row["cited"] or row["cited_target_comment"]
    for k in (1, 3, 5, 8, 10, 20):
        rank = row["pooled_rank"]
        row[f"recall@{k}"] = float(rank is not None and rank <= k)
        row[f"ndcg@{k}"] = (
            1 / math.log2(rank + 1) if rank is not None and rank <= k else 0.0
        )
        first = queries[0]["target_rank"] if queries else None
        row[f"first_query_recall@{k}"] = float(first is not None and first <= k)
        row[f"first_query_ndcg@{k}"] = (
            1 / math.log2(first + 1) if first is not None and first <= k else 0.0
        )
        row[f"query_pass@{k}"] = float(any(q["target_rank"] for q in queries[:k]))
    return row


def report(root: Path):
    targets = {r["id"]: r for r in read_jsonl(root / "eval.jsonl")}
    latest = {}
    for path in sorted((root / "trajectories").glob("*/*.jsonl")):
        events = read_jsonl(path)
        if not events:
            continue
        row = evaluate(events)
        target = targets[row["target_id"]]
        row.update(
            target_title=target["title"],
            target_url=target["url"],
            target_date=target["date"],
        )
        row["trajectory"] = str(path.relative_to(root))
        latest[(row["model"], row["case"])] = row
    rows = list(latest.values())
    assert rows, "No trajectories found"
    # Raw nested trajectories stay JSON; tabular measures are easy to compare in Polars/DuckDB.
    frame = pl.DataFrame(
        [
            {k: v for k, v in r.items() if k not in ("queries", "attempts")}
            for r in rows
        ],
        infer_schema_length=None,
    )
    frame.write_parquet(root / "metrics.parquet")
    progress = (
        frame.group_by("model")
        .agg(
            pl.len().alias("started"),
            pl.col("status").is_in(["complete", "error"]).sum().alias("terminal"),
            (pl.col("status") == "complete").sum().alias("completed"),
            (pl.col("status") == "error").sum().alias("errors"),
            (pl.col("status") == "in_progress").sum().alias("in_progress"),
        )
        .with_columns(pl.lit(len(targets) * 2).alias("expected_per_model"))
    )
    (root / "progress.json").write_text(json.dumps(progress.to_dicts(), indent=2))
    # Never count a still-running trajectory as a miss in provisional aggregates.
    # Its observed context remains available in the raw per-trajectory table.
    frame = frame.filter(pl.col("status").is_in(["complete", "error"]))
    attempted = [
        {
            "model": r["model"],
            "case": r["case"],
            "target_id": r["target_id"],
            "request": a["request"],
            "tool": a["name"],
            "arguments": a["arguments"],
        }
        for r in rows
        for a in r["attempts"]
    ]
    if attempted:
        pl.DataFrame(attempted, infer_schema_length=None).write_parquet(
            root / "attempted_calls.parquet"
        )
    query_rows = []
    for row in rows:
        for i, query in enumerate(row["queries"], 1):
            raw = query["query"]
            value = raw if isinstance(raw, str) else ""
            query_rows.append(
                {
                    "model": row["model"],
                    "case": row["case"],
                    "target_id": row["target_id"],
                    "query_number": i,
                    "query": value,
                    "tool": query["tool"],
                    "arguments": json.dumps(query["arguments"]),
                    "target_rank": query["target_rank"],
                    "returned": len(query["results"]),
                    "result_ids": [r["id"] for r in query["results"]],
                    "words": len(value.split()),
                    "has_quotes": '"' in value,
                    "has_or": bool(re.search(r"\bOR\b", value)),
                    "has_prefix": ":*" in value,
                    "encoded_array": value.lstrip().startswith("["),
                    "date_filter": any(
                        query["arguments"].get(k)
                        for k in ("min_date", "max_date", "target_date")
                    ),
                }
            )
    if query_rows:
        qframe = pl.DataFrame(query_rows, infer_schema_length=None)
        qframe.write_parquet(root / "queries.parquet")
        qframe.group_by("model").agg(
            pl.len().alias("queries"),
            *[
                pl.col(c).mean()
                for c in (
                    "words",
                    "has_quotes",
                    "has_or",
                    "has_prefix",
                    "encoded_array",
                    "date_filter",
                )
            ],
            (pl.col("returned") == 0).mean().alias("empty_fraction"),
        ).write_csv(root / "query_syntax.csv")
    metrics = [c for c in frame.columns if "@" in c] + [
        "exposed",
        "cited",
        "cited_target_comment",
        "cited_evidence",
        "query_count",
        "elapsed",
    ]
    summary = (
        frame.group_by("model")
        .agg(
            pl.len().alias("n"),
            (pl.col("status") == "complete").sum().alias("completed"),
            *[pl.col(c).mean().alias(c) for c in metrics],
        )
        .sort("model")
    )
    # Two fixed prompt variants, not iid samples: pass@1 averages variants, pass@2
    # asks whether either succeeds. Never claim the unbiased code-sampling estimator.
    variants = frame.group_by("model", "target_id").agg(
        pl.col("exposed").mean().alias("variant_pass@1"),
        pl.col("exposed").max().cast(pl.Float64).alias("variant_pass@2"),
        pl.len().alias("variant_count"),
    )
    variants = (
        variants.filter(pl.col("variant_count") == 2)
        .group_by("model")
        .agg(pl.col("variant_pass@1").mean(), pl.col("variant_pass@2").mean())
    )
    summary = summary.join(variants, on="model", how="left")
    summary.write_csv(root / "summary.csv")
    # During a serial model run, unmatched sample sizes/cohorts are not a bakeoff.
    # Publish a separate terminal intersection, which initially is just the pilot.
    model_count = progress.height
    common = (
        frame.group_by("case")
        .agg(pl.col("model").n_unique().alias("models"))
        .filter(pl.col("models") == model_count)
        .select("case")
    )
    frame.join(common, on="case", how="inner").group_by("model").agg(
        pl.len().alias("n"),
        *[pl.col(c).mean().alias(c) for c in metrics],
    ).write_csv(root / "matched_summary.csv")
    paired = []
    for left, right in combinations(sorted(frame["model"].unique().to_list()), 2):
        subset = frame.filter(pl.col("model").is_in([left, right]))
        shared = (
            subset.group_by("case")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") == 2)
            .select("case")
        )
        paired.append(
            subset.join(shared, on="case")
            .group_by("model")
            .agg(pl.len().alias("n"), *[pl.col(c).mean().alias(c) for c in metrics])
            .with_columns(pl.lit(left + " / " + right).alias("comparison"))
        )
    if paired:
        pl.concat(paired).write_csv(root / "pairwise_summary.csv")
    frame.group_by("model", "cohort", "style").agg(
        pl.len().alias("n"),
        pl.col("exposed").mean(),
        pl.col("cited").mean(),
        pl.col("ndcg@8").mean(),
        pl.col("query_count").mean(),
    ).write_csv(root / "strata.csv")
    print(
        summary.select(
            "model",
            "n",
            "completed",
            "exposed",
            "cited",
            "variant_pass@1",
            "variant_pass@2",
            "recall@8",
            "ndcg@8",
            "query_count",
        )
    )
    (root / "summary.json").write_text(json.dumps(summary.to_dicts(), indent=2))
    (root / "explorer.html").write_text(
        render_html(rows, summary.to_dicts()), encoding="utf-8"
    )
    return rows


def render_html(rows, summary):
    """Self-contained searchable explorer; all source strings rendered with textContent."""
    payload = json.dumps(
        {"rows": rows, "summary": summary}, ensure_ascii=False
    ).replace("<", "\\u003c")
    return (
        """<!doctype html><meta charset="utf-8"><title>HN retrieval trajectories</title>
<style>body{font:15px system-ui;background:#f7f7f2;color:#17232b;margin:32px;max-width:1400px}
input,select{padding:10px;margin:8px}table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:10px;border-bottom:1px solid #ccc}details{padding:12px;background:white;margin:12px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere} .hit{border-left:5px solid #16816a}.miss{border-left:5px solid #c27437}</style>
<h1>HN retrieval: prompts → queries → model context</h1>
<p>Success requires the anchor in a tool result consumed by a subsequent model request.
NDCG judges only the known anchor. Variant pass@2 means either of two fixed question styles.</p>
<p>Snapshot, not a live feed. Summary rates include finished runs only; subsets may differ by model.</p>
<div id="summary"></div><input id="filter" placeholder="Search prompts, queries, IDs" size="50">
<select id="model"><option value="">All models</option></select>
<select id="outcome"><option value="">All outcomes</option><option value="hit">Recovered</option><option value="miss">Missed</option><option value="pending">In progress</option></select>
<p id="count"></p><div id="cases"></div><script>
const data="""
        + payload
        + """;
const elem=(tag,text)=>{let e=document.createElement(tag);e.textContent=text;return e};
const table=elem('table','');let tr=elem('tr','');['Model','Runs','Completed','Context hit','Citation','Variant pass@2','NDCG@8'].forEach(x=>tr.append(elem('th',x)));table.append(tr);
data.summary.forEach(s=>{let r=elem('tr','');[s.model,s.n,s.completed,s.exposed,s.cited,s['variant_pass@2'],s['ndcg@8']].forEach(v=>r.append(elem('td',typeof v==='number'?Number(v.toFixed(3)):v)));table.append(r)});document.querySelector('#summary').append(table);
[...new Set(data.rows.map(r=>r.model))].forEach(m=>{let o=elem('option',m);o.value=m;document.querySelector('#model').append(o)});
const outcome=r=>r.status==='in_progress'?'pending':(r.exposed?'hit':'miss');
function render(){const q=document.querySelector('#filter').value.toLowerCase(),m=document.querySelector('#model').value,o=document.querySelector('#outcome').value;
const rows=data.rows.filter(r=>(!m||r.model===m)&&(!o||o===outcome(r))&&JSON.stringify(r).toLowerCase().includes(q));
document.querySelector('#count').textContent=rows.length+' trajectories';const container=document.querySelector('#cases');container.replaceChildren();
rows.forEach(r=>{let d=elem('details','');d.className=outcome(r);d.append(elem('summary',r.model+' · '+r.target_id+' · '+r.style+' · '+outcome(r).toUpperCase()+' · '+r.prompt));d.append(elem('h3','Target: '+r.target_title));d.append(elem('p','Status: '+r.status+'; context rank: '+r.pooled_rank+'; citation: '+r.cited));let raw=elem('details','');raw.append(elem('summary','All attempted tool calls (including errors)'));raw.append(elem('pre',JSON.stringify(r.attempts,null,2)));d.append(raw);r.queries.forEach((q,i)=>{d.append(elem('h4','Query '+(i+1)+': '+(q.query??'[date/filter only]')+' — target rank '+q.target_rank));d.append(elem('pre',JSON.stringify(q.arguments,null,2)));q.results.forEach((s,j)=>d.append(elem('p',(j+1)+'. '+s.id+' '+s.title)))});d.append(elem('pre',r.final||r.error||'Incomplete'));container.append(d)})}
['filter','model','outcome'].forEach(id=>document.getElementById(id).addEventListener('input',render));render();</script>"""
    )
