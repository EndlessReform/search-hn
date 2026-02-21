CREATE INDEX IF NOT EXISTS idx_items_search_tsv
ON items(search_tsv)
WHERE type = 'story';
