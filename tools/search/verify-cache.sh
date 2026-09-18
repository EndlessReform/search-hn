#!/usr/bin/env bash
# Profile full hybrid SQL separately from embedding HTTP time; mutate no DB data.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export PGHOST="${PGHOST:-searchhn-pg}"
export PGDATABASE="${PGDATABASE:-searchhn_test}"
export PGUSER="${PGUSER:-readonly_hn_agent}"
embedding_base="${EMBEDDING_BASE_URL:-https://magi06-inference.tail7a3eb.ts.net/embeddings/v1}"
scratch_dir=$(mktemp -d)
trap 'rm -rf "$scratch_dir"' EXIT

# Each EXPLAIN executes the same query. PostgreSQL 17 exposes I/O times when
# track_io_timing is enabled; null in the report means unavailable, not zero.
{
  cat <<'SQL'
BEGIN READ ONLY;
SET LOCAL statement_timeout='30s';
SET LOCAL hnsw.ef_search=1000;
SELECT obj_description('public.story_search'::regclass,'pg_class') = :'query_recipe' AS recipe_matches \gset
\if :recipe_matches
\else
\echo 'ERROR: embedding recipe mismatch'
\quit 1
\endif
EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)
SQL
  cat "$script_dir/hybrid-query.sql"
  printf '\nCOMMIT;\n'
} > "$scratch_dir/explain.sql"

for query in \
  'How do database indexes work?' \
  'Running large language models locally on consumer hardware' \
  'How to build a programming language compiler' \
  'Solar panels and home battery energy storage'; do
  printf '\nQuery: %s\n' "$query"
  jq -n --arg query "$query" \
    '{model:"pplx-embed-v1-0.6b",input:[$query],encoding_format:"float"}' > "$scratch_dir/request.json"
  curl --fail-with-body --max-time 40 -sS \
    "${embedding_base%/}/embeddings" \
    -H 'Content-Type: application/json' -H 'X-Embedding-Workload: interactive' \
    --data-binary "@$scratch_dir/request.json" -o "$scratch_dir/response.json" \
    -w 'Embedding HTTP: %{time_total}s\n'
  jq -e '.model == "pplx-embed-v1-0.6b" and (.data|length)==1 and
    .data[0].index==0 and (.data[0].embedding|length)==1024 and
    all(.data[0].embedding[]; type=="number" and .==floor and .>=-127 and .<=127)' \
    "$scratch_dir/response.json" > /dev/null
  vector=$(jq -c '.data[0].embedding' "$scratch_dir/response.json")
  recipe=$(jq -er '.embedding_recipe' "$scratch_dir/response.json")
  printf '%s\n' 'pass | total ms | read wait ms | HNSW ms | HNSW read wait ms | blocks read'
  for pass in 1 2 3; do
    psql -X -qAt -v ON_ERROR_STOP=1 -v query_vector="$vector" \
      -v query_text="$query" -v query_recipe="$recipe" \
      -f "$scratch_dir/explain.sql" > "$scratch_dir/plan.json"
    jq -r --arg pass "$pass" '.[0] as $p |
      ($p.Plan | recurse(.Plans[]?) | select(."Index Name"=="story_search_embedding_hnsw")) as $h |
      [$pass,$p."Execution Time",$p.Plan."Shared I/O Read Time",
       $h."Actual Total Time",$h."Shared I/O Read Time",$p.Plan."Shared Read Blocks"] |
      map(tostring) | join(" | ")' "$scratch_dir/plan.json"
  done
done
