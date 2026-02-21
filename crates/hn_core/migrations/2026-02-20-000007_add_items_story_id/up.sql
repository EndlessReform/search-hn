ALTER TABLE items
ADD COLUMN story_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_items_story_id_comment
ON items(story_id)
WHERE type = 'comment' AND story_id IS NOT NULL;
