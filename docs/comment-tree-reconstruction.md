# Comment Tree Reconstruction: Schema Analysis & Proposals

## Current Schema (as of migration 000004)

The `items` table mirrors HN's flat item model:

```sql
items (
    id         BIGINT PRIMARY KEY,
    type       TEXT,           -- 'story', 'comment', 'job', 'poll', 'pollopt'
    parent     BIGINT,         -- immediate parent (story or comment)
    -- ... other columns omitted for brevity
)
```

The `kids` table captures the ordered child list from each HN API response:

```sql
kids (
    item           BIGINT NOT NULL,   -- parent item id
    kid            BIGINT NOT NULL,   -- child item id
    display_order  BIGINT,            -- 0-indexed position in parent's kids array
    PRIMARY KEY (item, kid)
)
```

Existing indexes (from migration 000004):

- `idx_items_type` on `items(type)`
- `idx_items_parent` on `items(parent)`

---

## Core Problems

### 1. No `story_id` column — "find the root story" requires recursive traversal

A comment only knows its immediate `parent`. To find the root story, you must
walk the `parent` chain upward until you hit a row where `type = 'story'` (or
`parent IS NULL`). On Postgres this means a recursive CTE:

```sql
WITH RECURSIVE chain AS (
    SELECT id, parent, type FROM items WHERE id = :comment_id
    UNION ALL
    SELECT i.id, i.parent, i.type FROM items i JOIN chain c ON i.id = c.parent
)
SELECT id FROM chain WHERE type = 'story';
```

This is O(depth) random reads per comment, with typical depths of 5-20 and
worst-case >50. It's fine for one-off lookups but painful at scale or in
analytics queries.

### 2. No way to get "all comments under a story" without recursion

Query (a) — "reconstruct the whole comment tree for a story" — faces the same
problem in reverse. You need a recursive CTE walking *downward* from the story
through `kids` or `parent`:

```sql
WITH RECURSIVE tree AS (
    SELECT kid AS id, display_order, 0 AS depth
    FROM kids WHERE item = :story_id
    UNION ALL
    SELECT k.kid, k.display_order, t.depth + 1
    FROM kids k JOIN tree t ON k.item = t.id
)
SELECT t.*, i.* FROM tree t JOIN items i ON t.id = i.id
ORDER BY /* ??? */;
```

This works, but:
- It's expensive (one index lookup per level per branch).
- The `ORDER BY` for "display order" is non-trivial (see problem 3).
- Postgres recursive CTEs are `UNION ALL` set operations — the planner can't
  push predicates into them well, and the intermediate working table lives in
  memory.

### 3. Display-order reconstruction is the hardest part

HN's display order is a depth-first traversal of the tree, where children at
each level are ordered by the parent's `kids` array (stored as
`kids.display_order`). Reconstructing this in SQL means you need a
*depth-first ordering key* — something like a materialized path or a global
sort key.

With only `parent` + `kids.display_order`, the recursive CTE must build the
path on the fly:

```sql
WITH RECURSIVE tree AS (
    SELECT kid AS id, display_order, ARRAY[display_order] AS path
    FROM kids WHERE item = :story_id
    UNION ALL
    SELECT k.kid, k.display_order, t.path || k.display_order
    FROM kids k JOIN tree t ON k.item = t.id
)
SELECT t.*, i.* FROM tree t JOIN items i ON t.id = i.id
ORDER BY t.path;
```

The `path` array grows per-row and the sort is O(n log n) on variable-length
arrays. It works for moderate trees (hundreds of comments) but gets expensive
for mega-threads (thousands).

### 4. Top-level comments require a join or subquery

Query (b) — "all top-level comments for a story" — is not terrible today:

```sql
SELECT * FROM items WHERE parent = :story_id AND type = 'comment';
```

This uses `idx_items_parent`. But if you also want them in display order, you
need to join `kids`:

```sql
SELECT i.* FROM items i
JOIN kids k ON k.kid = i.id AND k.item = :story_id
WHERE i.type = 'comment'
ORDER BY k.display_order;
```

This is fine. The `kids` PK `(item, kid)` covers the join. This is the one
query that's already reasonably cheap.

### 5. The `kids` table duplicates `parent` but in the other direction

Every parent-child edge is stored *twice*: once in `items.parent` (child →
parent) and once in `kids` (parent → child, with order). This is fine for
correctness and mirrors the HN API, but it means:
- Inserts write two tables per item.
- The two can theoretically disagree (an item's `parent` might not match any
  `kids` row, or vice versa). In practice your upsert logic keeps them
  consistent, but there's no FK constraint enforcing it.

This isn't really a "problem" — it's a reasonable trade-off for a mirror
database. But it's worth noting that `kids` is the *only* source of
display-order information, while `items.parent` is the only source of the
upward pointer. Both are needed.

---

## Proposals

### Tier 1: Low-effort, high-leverage (additive, no schema breaks)

#### 1A. Add a `story_id` generated/denormalized column to `items`

**What:** A `BIGINT` column on `items` that stores the root story ID for every
comment. For stories themselves, `story_id = id`. For non-comment types, NULL.

**Why:** This is the single highest-leverage change. It turns queries (a), (b),
and (c) from recursive to flat:

```sql
-- (a) All comments under a story (still need tree ordering, but no recursion to *find* them)
SELECT * FROM items WHERE story_id = :story_id;

-- (b) Top-level comments
SELECT * FROM items WHERE story_id = :story_id AND parent = :story_id;

-- (c) From comment, find story
SELECT story_id FROM items WHERE id = :comment_id;
```

**Implementation options (pick one):**

*Option A — Postgres generated column (no ingest changes):*

Unfortunately, this can't be a `GENERATED ALWAYS AS` column because it depends
on other rows (the parent chain), not just the current row's columns. Generated
columns in Postgres can only reference the row's own data.

*Option B — Populate at ingest time:*

When a comment is ingested, look up its parent's `story_id`. If the parent is a
story, set `story_id = parent`. If the parent is a comment, copy the parent's
`story_id`. This is O(1) per ingest (one extra read of the parent row).

Edge case: if the parent hasn't been ingested yet (out-of-order catchup),
`story_id` stays NULL and gets backfilled on a second pass or via a periodic
maintenance query:

```sql
-- Backfill: iterative, run until 0 rows updated
UPDATE items c
SET story_id = p.story_id
FROM items p
WHERE c.parent = p.id
  AND c.story_id IS NULL
  AND p.story_id IS NOT NULL;
```

*Option C — Materialized via trigger:*

A `BEFORE INSERT OR UPDATE` trigger that resolves the parent chain. More
complex, but guarantees consistency without application changes.

**Storage cost:** 8 bytes per row (nullable BIGINT). With an index, roughly
16-24 bytes/row total overhead. For ~50M items, that's ~0.8-1.2 GB.

**Recommended index:**

```sql
CREATE INDEX idx_items_story_id ON items(story_id) WHERE story_id IS NOT NULL;
```

This partial index skips stories/jobs/polls (which don't need to be looked up
via `story_id`) and keeps the index compact.

**Blast radius:** Additive column, nullable, no existing queries or ingest code
break. The Diesel `Item` struct simply gains an `Option<i64>` field.

#### 1B. Add a composite index on `kids(item, display_order, kid)`

**What:** A covering index that lets "get ordered children of item X" be an
index-only scan:

```sql
CREATE INDEX idx_kids_item_order ON kids(item, display_order) INCLUDE (kid);
```

**Why:** The existing PK `(item, kid)` doesn't help when ordering by
`display_order`. This index makes query (b) and per-level child fetching in
recursive tree-building fast.

**Storage cost:** Roughly the same size as the existing PK index.

**Blast radius:** Pure additive index, zero code changes needed.

---

### Tier 2: Medium effort, good payoff

#### 2A. Materialized path column (`path`) for display-order sorting

**What:** A `TEXT` or `BIGINT[]` column on `items` storing the path from root
to the comment, e.g. `{0, 2, 1}` meaning "third child of first child of
story".

**Why:** With a path column, display-order reconstruction becomes a trivial
sort:

```sql
SELECT * FROM items
WHERE story_id = :story_id
ORDER BY path;
```

No recursive CTE, no on-the-fly path building.

**Implementation:** Populated at ingest time (parent's path || own
display_order) or via backfill. Same out-of-order concerns as `story_id`.

**Trade-offs:**
- Requires knowing the parent's path at ingest time (same chicken-and-egg as
  story_id, but harder to backfill since display_order comes from `kids`).
- Path changes if HN re-orders comments (rare but possible). The `kids` upsert
  would need to cascade path updates — this gets complex.
- Variable-length column; array comparisons in Postgres are lexicographic on
  elements, which is correct for display order.

**Verdict:** High value but meaningful ingest complexity. Consider only if
display-order queries are a frequent need. If you go with 1A (`story_id`), you
can reconstruct display order client-side by fetching all comments + kids rows
and building the tree in memory, which is often fast enough.

#### 2B. Depth column

**What:** `depth INTEGER` on `items`, indicating nesting level (0 for stories,
1 for top-level comments, etc.).

**Why:** Useful for rendering indent levels without traversing the tree. Also
allows efficient "find all top-level comments" as `WHERE story_id = X AND
depth = 1` without needing to know the story's ID for the `parent` filter.

**Implementation:** Trivially derived: `parent_depth + 1`. Same
ingest-time-or-backfill pattern as `story_id`.

**Storage cost:** 4 bytes/row.

**Blast radius:** Additive, nullable.

---

### Tier 3: High effort, specialized value

#### 3A. Nested set / interval encoding

Classic approach for "fetch entire subtree in one query with ordering." Two
columns (`lft`, `rgt`) encode a pre-order/post-order traversal. Any subtree is
a range scan: `WHERE lft BETWEEN parent.lft AND parent.rgt`.

**Trade-offs:** Extremely efficient reads, but writes are catastrophic — inserting
a node requires updating `lft`/`rgt` for half the tree. Not viable for a
live-updating mirror.

#### 3B. Closure table

A separate `comment_ancestors(ancestor_id, descendant_id, depth)` table
storing every ancestor-descendant pair. Enables arbitrary subtree queries
without recursion.

**Trade-offs:** O(depth) rows per comment inserted (so ~5-20 new rows per
comment). For 50M items with average depth 8, that's ~400M rows. Significant
storage and ingest overhead. Probably overkill unless you need arbitrary
subtree queries frequently.

---

## Recommendation

**Do 1A (`story_id`) and 1B (kids ordering index).** Together they solve all
three target queries:

| Query | Before | After |
|-------|--------|-------|
| (a) Full comment tree in display order | Recursive CTE, expensive | `WHERE story_id = X` + client-side tree build from `kids`, or recursive CTE that's now shallow (only ordering, not discovery) |
| (b) Top-level comments | `WHERE parent = X` (already okay) | `WHERE story_id = X AND parent = X` (semantically clearer, same perf) |
| (c) Comment → story | Recursive CTE | `SELECT story_id WHERE id = X` (single index lookup) |

For display-order rendering: once you have all comments for a story (via
`story_id`), fetch the `kids` rows for those comments and build the tree
client-side. This is trivially fast — even a 2000-comment thread is <1ms of
in-memory tree construction. Don't over-engineer the SQL sort.

Add `depth` (2B) only if you find you need it for rendering or filtering —
it's cheap but not strictly necessary.

Avoid tier 3 approaches. They're designed for systems where reads vastly
outnumber writes and the tree is relatively static. An HN mirror has continuous
writes and the tree structure changes as comments arrive.

---

## Migration Sketch (for 1A + 1B)

```sql
-- 1A: Add story_id column
ALTER TABLE items ADD COLUMN story_id BIGINT;

-- Set story_id for stories themselves
UPDATE items SET story_id = id WHERE type = 'story';

-- Iterative backfill for comments (run until 0 rows affected)
-- Each pass resolves one more level of depth.
DO $$
DECLARE
    affected INTEGER;
BEGIN
    LOOP
        UPDATE items c
        SET story_id = COALESCE(
            (SELECT p.story_id FROM items p WHERE p.id = c.parent),
            c.parent  -- parent is the story itself
        )
        FROM items p
        WHERE c.parent = p.id
          AND c.story_id IS NULL
          AND c.type = 'comment'
          AND (p.story_id IS NOT NULL OR p.type = 'story');

        GET DIAGNOSTICS affected = ROW_COUNT;
        RAISE NOTICE 'Backfilled % rows', affected;
        EXIT WHEN affected = 0;
    END LOOP;
END $$;

-- Partial index (excludes NULLs — non-comment types)
CREATE INDEX idx_items_story_id ON items(story_id) WHERE story_id IS NOT NULL;

-- 1B: Covering index for ordered child lookup
CREATE INDEX idx_kids_item_order ON kids(item, display_order) INCLUDE (kid);
```

Ingest-side change (in `convert_item` or the persister): when inserting a
comment, do a single `SELECT story_id FROM items WHERE id = $parent` and set
the new comment's `story_id`. If the parent isn't ingested yet, leave
`story_id` NULL — a periodic backfill job or post-segment-completion sweep
resolves stragglers.

---

## Codex Notes

### (a) Agreement level

I agree with the core recommendation: adding a denormalized root pointer
(`story_id`) plus an ordered-children index on `kids` is the highest-leverage
change for your target queries.

### (b) Key alternatives / details to add

1. Consider `root_id` (+ optional `root_type`) instead of `story_id`.
   - Current wording assumes every comment tree roots at a story, but HN also
     has poll threads.
   - If poll-thread queries matter, `root_id` is the more future-proof name.

2. Clarify index predicate semantics.
   - If you keep `story_id = id` for stories, then
     `WHERE story_id IS NOT NULL` includes stories.
   - If the index is meant for comment lookups only, prefer:
     `WHERE type = 'comment' AND story_id IS NOT NULL`.

3. Update both ingest paths.
   - This repo currently has both the newer ingest worker and a legacy worker.
   - Both need consistent population rules for `story_id`.

4. Add data-quality guardrails.
   - Track unresolved comment rows (`type='comment' AND story_id IS NULL`).
   - Validate child/parent consistency between `items.parent` and `kids`.
   - Add a mismatch report for non-comment rows that accidentally get
     `story_id` populated.

### (c) Feasibility across ~47M existing rows

Yes, feasible, but the migration sketch should be made production-safe:

1. Avoid one giant transaction for iterative backfill.
   - A single `DO $$ ... LOOP ... $$` keeps all passes in one transaction.
   - On tens of millions of rows, this risks heavy WAL growth, long lock
     hold times, and painful rollback behavior.

2. Prefer chunked backfill with commits between batches.
   - Run batched updates from an application/ops job (or repeated SQL calls),
     committing each batch.
   - Continue until unresolved count reaches zero (or known orphan baseline).

3. Build indexes concurrently.
   - Use `CREATE INDEX CONCURRENTLY` for hot production tables to avoid
     blocking writers.

4. Roll out in phases.
   - Phase 1: add nullable column.
   - Phase 2: backfill in chunks with progress telemetry.
   - Phase 3: add concurrent index.
   - Phase 4: update ingest to populate on write.
   - Phase 5: keep periodic repair job for out-of-order arrivals.

### Suggested SQL adjustments

```sql
-- Add nullable column first.
ALTER TABLE items ADD COLUMN story_id BIGINT;

-- Seed stories.
UPDATE items
SET story_id = id
WHERE type = 'story' AND story_id IS NULL;

-- Create index online (production).
CREATE INDEX CONCURRENTLY idx_items_story_id_comment
ON items(story_id)
WHERE type = 'comment' AND story_id IS NOT NULL;

CREATE INDEX CONCURRENTLY idx_kids_item_order
ON kids(item, display_order) INCLUDE (kid);
```

```sql
-- Example batched backfill pass (run repeatedly from a job runner).
WITH batch AS (
    SELECT c.id, COALESCE(p.story_id, c.parent) AS resolved_story_id
    FROM items c
    JOIN items p ON p.id = c.parent
    WHERE c.type = 'comment'
      AND c.story_id IS NULL
      AND (p.story_id IS NOT NULL OR p.type = 'story')
    LIMIT 50000
)
UPDATE items c
SET story_id = b.resolved_story_id
FROM batch b
WHERE c.id = b.id;
```
