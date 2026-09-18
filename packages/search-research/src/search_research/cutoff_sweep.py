"""Replay consumed searches against live PG, measuring exact target rank.

Run with uv run --package search-research python -m search_research.cutoff_sweep.
First probe the target by primary key: nonmatching targets need no corpus scan.
For eligible targets count rows preceding them in the production score/day/id
ordering. This is a fixed-query counterfactual, not a new agent trajectory.
"""

import json
from datetime import date
from pathlib import Path

import polars as pl
from search_agent.data_access import create_db_engine
from search_agent.tools.utils import normalize_domains
from sqlalchemy import text

ROOT = Path('data/fts-baseline-20260904/plain-results')


def predicate(row):
    """Mirror production search predicates, preserving malformed query strings."""
    args = json.loads(row['arguments'])
    clauses = ["i.type = 'story'"]
    params = {}
    query = row['query']
    if row['tool'] == 'fetch_top_stories_for_date':
        params['day'] = date.fromisoformat(args['target_date'])
        clauses.append('i.day = :day')
    elif query and query.strip():
        params['query'] = query.strip()
        clauses.append("i.search_tsv @@ plainto_tsquery('simple', :query)")
    lexical = ' AND '.join(clauses)
    if row['tool'] == 'fetch_stories':
        for key, column, op in [('min_score','score','>='), ('min_date','day','>='), ('max_date','day','<=')]:
            if args.get(key) is not None:
                params[key] = date.fromisoformat(args[key]) if 'date' in key else args[key]
                clauses.append(f'i.{column} {op} :{key}')
        for key in ['include_domains', 'exclude_domains']:
            domains = normalize_domains(args.get(key))
            if domains:
                params[key] = domains
                negation = 'NOT ' if key == 'exclude_domains' else ''
                clauses.append(f"{negation}(regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = ANY(CAST(:{key} AS text[])))")
    return lexical, ' AND '.join(clauses), params


def main():
    """Persist each result immediately; run one read-only DB request at a time."""
    rows = pl.read_parquet(ROOT / 'queries.parquet').to_dicts()
    engine = create_db_engine('postgresql://readonly_hn_agent@searchhn-pg:5432/searchhn_test')
    cache = {}
    out = ROOT / 'cutoff-sweep.jsonl'
    with engine.connect() as conn, out.open('w') as journal:
        conn.execute(text("SET statement_timeout = '55s'"))
        for idx, row in enumerate(rows):
            lexical, where, params = predicate(row)
            key = (row['target_id'], where, json.dumps(params, default=str, sort_keys=True))
            if key not in cache:
                probe = conn.execute(text(f'SELECT ({lexical}) AS lexical, ({where}) AS eligible FROM items i WHERE id=:target'), {**params, 'target': row['target_id']}).mappings().one()
                rank = None
                if probe['eligible']:
                    # A window over matching rows gives the exact NULLS LAST
                    # ordering without inventing sentinels for nullable fields.
                    sql = f'''SELECT rank FROM (SELECT i.id, row_number() OVER
                        (ORDER BY i.score DESC NULLS LAST, i.day DESC NULLS LAST, i.id DESC) rank
                        FROM items i WHERE {where}) ranked WHERE id=:target'''
                    rank = conn.execute(text(sql), {**params, 'target': row['target_id']}).scalar_one()
                cache[key] = {'lexical_match': bool(probe['lexical']), 'eligible': bool(probe['eligible']), 'live_rank': rank}
            record = {**row, **cache[key]}
            journal.write(json.dumps(record) + '\n')
            journal.flush()
            if idx % 100 == 0:
                print(f'{idx}/{len(rows)} lists; {len(cache)} distinct target/search pairs', flush=True)
    pl.read_ndjson(out).write_parquet(ROOT / 'cutoff-sweep.parquet')


if __name__ == '__main__':
    main()
