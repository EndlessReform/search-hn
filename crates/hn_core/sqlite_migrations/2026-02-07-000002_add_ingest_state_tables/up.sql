CREATE TABLE ingest_segments (
    segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    scan_cursor_id INTEGER,
    unresolved_count INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT,
    CHECK (start_id <= end_id),
    CHECK (attempts >= 0),
    CHECK (unresolved_count >= 0),
    CHECK (status IN ('pending', 'in_progress', 'done', 'retry_wait', 'dead_letter'))
);

CREATE INDEX ingest_segments_status_idx
    ON ingest_segments (status, started_at);

CREATE INDEX ingest_segments_heartbeat_idx
    ON ingest_segments (status, heartbeat_at);

CREATE INDEX ingest_segments_range_idx
    ON ingest_segments (start_id, end_id);

CREATE TABLE ingest_exceptions (
    segment_id INTEGER NOT NULL REFERENCES ingest_segments(segment_id) ON DELETE CASCADE,
    item_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (segment_id, item_id),
    CHECK (attempts >= 0),
    CHECK (state IN ('pending', 'retry_wait', 'terminal_missing', 'dead_letter'))
);

CREATE INDEX ingest_exceptions_state_idx
    ON ingest_exceptions (state, next_retry_at);

CREATE INDEX ingest_exceptions_item_idx
    ON ingest_exceptions (item_id);
