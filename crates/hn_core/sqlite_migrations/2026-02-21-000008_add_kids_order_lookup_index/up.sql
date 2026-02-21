-- SQLite parity index for ordered parent->children lookups in DB-backed tests.
CREATE INDEX IF NOT EXISTS idx_kids_item_order
ON kids(item, display_order, kid);
