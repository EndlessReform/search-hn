ALTER TABLE items
DROP COLUMN day;

ALTER TABLE items
DROP COLUMN domain;

DROP INDEX IF EXISTS idx_items_parent;
DROP INDEX IF EXISTS idx_items_type;
