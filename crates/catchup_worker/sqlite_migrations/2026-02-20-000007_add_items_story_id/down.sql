DROP INDEX IF EXISTS idx_items_story_id_comment;

ALTER TABLE items
DROP COLUMN story_id;
