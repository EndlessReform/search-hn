CREATE TABLE updater_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    last_sse_event_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
