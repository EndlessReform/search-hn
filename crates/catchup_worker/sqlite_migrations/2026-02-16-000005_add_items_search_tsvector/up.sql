-- SQLite parity migration for DB-backed tests.
--
-- SQLite has no `tsvector`, so we keep a generated text search document and index it.
ALTER TABLE items
ADD COLUMN search_tsv TEXT GENERATED ALWAYS AS (
    trim(COALESCE(title, '') || ' ' || COALESCE(url, ''))
) STORED;

CREATE INDEX IF NOT EXISTS idx_items_search_tsv
ON items(search_tsv)
WHERE type = 'story';
