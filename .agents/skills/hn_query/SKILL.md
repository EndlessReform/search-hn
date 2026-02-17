---
name: hn-query
description: Query mirrored Hacker News data with a minimal JSONL CLI for fast entity lookups and exploratory checks. Use this for (1) submission search over stories with full-text/title-url matching and score/date gates, and (2) peeking top-level comments for a known story ID. Prefer this when you need quick read-only retrieval and jq-friendly output.
---

# HN Query

Use this skill to run lightweight submission lookups over the mirrored Postgres DB.

## Run

```bash
uv run .agents/skills/hn_query/scripts/make_query.py "openai"
```

Use filters and sort as needed:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py "openai" \
  --min-score 50 \
  --min-date 2024-01-01 \
  --max-date 2025-12-31 \
  --sort score \
  --limit 20 \
  --skip 0
```

Count matches without fetching rows:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py count "openai" \
  --min-date 2024-01-01 \
  --domain openai.com
```

Top submissions from one domain (no query string):

```bash
uv run .agents/skills/hn_query/scripts/make_query.py top-domain github.com \
  --sort score \
  --min-score 50 \
  --min-date 2024-01-01
```

Use a domain blacklist in search/count:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py search "ai" \
  --exclude-domain twitter.com \
  --exclude-domain x.com
```

Top-level comments for a known story ID:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py comments 26699106 --comments 3 --skip 0
```

## Output Contract

- Emit JSONL to stdout (one JSON object per line).
- `search` rows include `id`, `title`, `url`, `date`, `score`.
- Include `text` only when non-empty.
- `count` rows include `count`.
- `comments` rows include `author`, `comment`.
- Emit operational logs and no-result notices to stderr.
- `comments` mode also emits a stderr summary with top-level comments remaining.

## Scope And Limitations

- Search mode is submissions-only: SQL filter is `items.type = 'story'`; comments are excluded from full-text search.
- `top-domain` mode is submissions-only and does not run full-text query matching.
- Search document is title+URL oriented: `search_tsv` is generated from `title` and `url` (not comment text).
- Sort options are `--sort score` (default), `--sort rank`, and `--sort date` (descending date).
- `--sort rank` uses PostgreSQL `ts_rank_cd` over `plainto_tsquery('simple', ...)`:
  - Treat this as a rough relevance signal, not semantic ranking.
  - Prefer caution when using rank alone for quality judgments.
- Query syntax is plain text (`plainto_tsquery`), not advanced boolean `tsquery`.
  - Punctuation/operators are normalized; phrase-like intent may not behave as expected.
- Date filters use `items.day` (`YYYY-MM-DD`) and only affect rows where `day` is present.
- `--domain` and `--exclude-domain` are normalized to strip leading `www.` and lower-case.
  - You can pass plain domains or full URLs; comparison is done against normalized `items.domain`.
- `comments` mode returns only top-level comments from `kids` where `kids.item = <story_id>`.
- `comments` ordering is `display_order` (then comment id), because comments have no score.
- `comments` mode does not fetch nested replies; run again with a comment ID if you need that thread's direct children.

## Common JSONL + jq Patterns

Top 5 URLs:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py "openai" --limit 100 \
  | jq -r '.url' \
  | head -n 5
```

Only high-score rows with compact fields:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py "vector db" --min-score 100 \
  | jq -c '{title, score, date, url}'
```

Get top-level comment authors for a story:

```bash
uv run .agents/skills/hn_query/scripts/make_query.py comments 26699106 --comments 20 \
  | jq -r '.author'
```
