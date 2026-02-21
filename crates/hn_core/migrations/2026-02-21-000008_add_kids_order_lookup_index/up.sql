-- Keeps ordered child expansion cheap and predictable for comment-tree assembly.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kids_item_order
ON kids(item, display_order) INCLUDE (kid);
