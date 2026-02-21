CREATE TABLE updater_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_sse_event_at TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
