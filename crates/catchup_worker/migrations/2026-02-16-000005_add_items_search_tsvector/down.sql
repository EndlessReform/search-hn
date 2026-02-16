DROP INDEX CONCURRENTLY IF EXISTS idx_items_search_tsv;

ALTER TABLE items
DROP COLUMN search_tsv;
