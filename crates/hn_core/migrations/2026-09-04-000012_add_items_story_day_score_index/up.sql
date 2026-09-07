-- Supports the search-agent's top-stories-for-date query. The partial index
-- narrows entries to stories, begins with the equality-filtered UTC day, and
-- stores the remaining keys in the query's exact ranking order so LIMIT can
-- stop after the requested rows without scanning every historical story.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_items_story_day_score
ON items (day, score DESC NULLS LAST, id DESC)
WHERE type = 'story';
