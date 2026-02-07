CREATE TABLE kids (
    item INTEGER NOT NULL,
    kid INTEGER NOT NULL,
    display_order INTEGER,
    PRIMARY KEY(item, kid)
);

CREATE TABLE items (
    id INTEGER NOT NULL PRIMARY KEY,
    deleted INTEGER,
    type TEXT,
    by TEXT,
    time INTEGER,
    text TEXT,
    dead INTEGER,
    parent INTEGER,
    poll INTEGER,
    url TEXT,
    score INTEGER,
    title TEXT,
    -- Postgres stores this as BIGINT[]; SQLite test schema stores JSON text.
    parts TEXT,
    descendants INTEGER
);

CREATE TABLE users (
    id TEXT NOT NULL PRIMARY KEY,
    created INTEGER,
    karma INTEGER,
    about TEXT,
    submitted TEXT
);
