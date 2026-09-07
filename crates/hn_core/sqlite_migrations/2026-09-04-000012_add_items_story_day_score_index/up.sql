-- SQLite sorts NULL below non-NULL values, so DESC naturally matches the
-- PostgreSQL query's explicit `score DESC NULLS LAST` ordering.
CREATE INDEX IF NOT EXISTS idx_items_story_day_score
ON items (day, score DESC, id DESC)
WHERE type = 'story';
