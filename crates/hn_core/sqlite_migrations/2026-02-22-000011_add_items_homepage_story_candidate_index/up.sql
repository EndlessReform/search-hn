CREATE INDEX IF NOT EXISTS idx_items_homepage_story_candidate
ON items (id DESC, time, score)
WHERE type = 'story'
  AND COALESCE(deleted, 0) = 0
  AND COALESCE(dead, 0) = 0
  AND title IS NOT NULL
  AND time IS NOT NULL;
