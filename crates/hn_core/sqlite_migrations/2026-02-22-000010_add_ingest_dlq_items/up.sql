CREATE TABLE ingest_dlq_items (
    dlq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK (source IN ('catchup', 'realtime')),
    run_id TEXT NOT NULL,
    segment_id BIGINT,
    item_id BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('retry_wait', 'terminal_missing', 'dead_letter')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    failure_class TEXT,
    last_error TEXT,
    diagnostics_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX ingest_dlq_items_state_idx
    ON ingest_dlq_items (source, state, updated_at);

CREATE INDEX ingest_dlq_items_item_idx
    ON ingest_dlq_items (item_id, updated_at);
