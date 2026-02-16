-- Add a generated full-text document over title + url for 80/20 entity lookup.
--
-- Design notes:
-- - We intentionally use the `simple` config so URL/domain-ish tokens are preserved better
--   than language-stemming dictionaries.
-- - We index stories only (`type = 'story'`) to keep index size and write amplification low
--   for the 80/20 lookup target.
ALTER TABLE items
ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('simple', COALESCE(url, '')), 'B')
) STORED;

CREATE INDEX CONCURRENTLY idx_items_search_tsv
ON items USING GIN (search_tsv)
WHERE type = 'story';
