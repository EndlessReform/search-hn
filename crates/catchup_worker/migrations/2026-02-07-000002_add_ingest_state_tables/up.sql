CREATE TABLE ingest_segments (
    segment_id BIGSERIAL PRIMARY KEY,
    start_id BIGINT NOT NULL,
    end_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    scan_cursor_id BIGINT,
    unresolved_count INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
    segment_id BIGINT NOT NULL REFERENCES ingest_segments(segment_id) ON DELETE CASCADE,
    item_id BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (segment_id, item_id),
    CHECK (attempts >= 0),
    CHECK (state IN ('pending', 'retry_wait', 'terminal_missing', 'dead_letter'))
);

CREATE INDEX ingest_exceptions_state_idx
    ON ingest_exceptions (state, next_retry_at);

CREATE INDEX ingest_exceptions_item_idx
    ON ingest_exceptions (item_id);
