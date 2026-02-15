-- SQLite parity migration for local DB-backed tests.
--
-- The production Postgres migration adds generated `domain`/`day` columns.
-- Here we keep behavior close enough for tests while staying SQLite-compatible.
CREATE INDEX IF NOT EXISTS idx_items_type ON items(type);
CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent);

ALTER TABLE items
ADD COLUMN domain TEXT GENERATED ALWAYS AS (
    CASE
        WHEN url IS NULL OR trim(url) = '' THEN NULL
        ELSE NULLIF(
            lower(
                CASE
                    WHEN instr(
                        CASE
                            WHEN instr(url, '://') > 0 THEN substr(url, instr(url, '://') + 3)
                            ELSE url
                        END,
                        '/'
                    ) > 0 THEN substr(
                        CASE
                            WHEN instr(url, '://') > 0 THEN substr(url, instr(url, '://') + 3)
                            ELSE url
                        END,
                        1,
                        instr(
                            CASE
                                WHEN instr(url, '://') > 0 THEN substr(url, instr(url, '://') + 3)
                                ELSE url
                            END,
                            '/'
                        ) - 1
                    )
                    ELSE CASE
                        WHEN instr(url, '://') > 0 THEN substr(url, instr(url, '://') + 3)
                        ELSE url
                    END
                END
            ),
            ''
        )
    END
) STORED;

ALTER TABLE items
ADD COLUMN day TEXT GENERATED ALWAYS AS (
    CASE
        WHEN time IS NULL THEN NULL
        ELSE date(time, 'unixepoch')
    END
) STORED;
