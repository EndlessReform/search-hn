-- Supports the hn_app homepage candidate query:
-- recent stories only, bounded by `id >= cutoff`, fetching just `id/time/score`.
--
-- `id DESC` matches the query's scan order, while INCLUDE columns allow Postgres to
-- satisfy the narrow candidate fetch from the index when visibility maps permit.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_items_homepage_story_candidate
ON items (id DESC)
INCLUDE (time, score)
WHERE type = 'story'
  AND COALESCE(deleted, FALSE) = FALSE
  AND COALESCE(dead, FALSE) = FALSE
  AND title IS NOT NULL
  AND time IS NOT NULL;
