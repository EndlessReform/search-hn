"""Offline query-level comparison widget, embedded in the research report."""

import json

import polars as pl

from search_research.engine_data import OUT, TRACES


def render():
    """Show literal queries and target ranks without loading giant raw traces."""
    source = pl.read_parquet(OUT / "replay.parquet")
    keys = ["model", "case", "query_number", "query"]
    rows = source.with_columns(
        (pl.col("engine") + "_" + pl.col("method")).alias("retriever")
    ).pivot(on="retriever", index=keys, values="rank")
    titles = pl.read_parquet(TRACES / "metrics.parquet").select(
        "model", "case", "target_title"
    )
    rows = rows.join(titles, on=["model", "case"]).sort("model", "case", "query_number")
    payload = json.dumps(rows.to_dicts()).replace("<", "\\u003c")
    return (
        """<h2>Inspect the intermediate searches</h2>
<p>Filter by model, story title/ID, or literal query. Numbers are target ranks; a dash means absent from the top 100 candidates (top 200 for hybrid). Showing at most 100 matching rows.</p>
<input id="query-filter" placeholder="e.g. Dark Hours, qwen, ISS" style="width:500px;max-width:90%;padding:10px"><p id="query-count"></p><div style="overflow:auto"><table id="query-table"></table></div>
<script type="application/json" id="query-data">"""
        + payload
        + """</script>
<script>
const queryRows=JSON.parse(document.getElementById('query-data').textContent);
const queryColumns=['model','target_title','query_number','query','pg_lexical','duckdb_lexical','pg_dense','duckdb_dense','pg_hybrid','duckdb_hybrid'];
function showQueries(){
 const needle=document.getElementById('query-filter').value.toLowerCase();
 const matches=queryRows.filter(r=>[r.model,r.case,r.target_title,r.query].join(' ').toLowerCase().includes(needle));
 document.getElementById('query-count').textContent=matches.length+' matching searches';
 const table=document.getElementById('query-table'); table.replaceChildren();
 const header=document.createElement('tr');
 for(const c of queryColumns){const th=document.createElement('th');th.textContent=c;header.appendChild(th)}
 table.appendChild(header);
 for(const r of matches.slice(0,100)){const tr=document.createElement('tr');for(const c of queryColumns){const td=document.createElement('td');td.textContent=r[c]??'—';tr.appendChild(td)}table.appendChild(tr)}
}
document.getElementById('query-filter').addEventListener('input',showQueries);showQueries();
</script>"""
    )
