-- SQLite parity migration for DB-backed tests.
--
-- SQLite has no `tsvector`, so we keep a generated text search document.
ALTER TABLE items
ADD COLUMN search_tsv TEXT GENERATED ALWAYS AS (
    trim(COALESCE(title, '') || ' ' || COALESCE(url, ''))
) STORED;
