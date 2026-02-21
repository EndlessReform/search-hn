-- Add a generated full-text document over title + url for 80/20 entity lookup.
--
-- Design notes:
-- - We intentionally use the `simple` config so URL/domain-ish tokens are preserved better
--   than language-stemming dictionaries.
ALTER TABLE items
ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('simple', COALESCE(url, '')), 'B')
) STORED;
