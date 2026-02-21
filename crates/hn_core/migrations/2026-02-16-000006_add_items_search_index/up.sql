-- Build the FTS index separately so we can use CONCURRENTLY outside a transaction.
CREATE INDEX CONCURRENTLY idx_items_search_tsv
ON items USING GIN (search_tsv)
WHERE type = 'story';
