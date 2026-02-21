-- Add indexes used by common item filters and parent-child traversals.
CREATE INDEX IF NOT EXISTS idx_items_type ON items(type);
CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent);

-- Add generated helper columns for downstream analytics and filtering.
--
-- Notes:
-- - `domain` is intentionally best-effort host extraction. It lowercases output and returns
--   NULL when extraction fails. This avoids ingest failures on malformed or unusual URLs.
-- - `day` normalizes unix-second `time` values into UTC calendar dates.
ALTER TABLE items
ADD COLUMN domain TEXT GENERATED ALWAYS AS (
    CASE
        WHEN url IS NULL OR btrim(url) = '' THEN NULL
        ELSE NULLIF(
            lower(
                substring(
                    url
                    FROM '^(?:[a-zA-Z][a-zA-Z0-9+.-]*://)?(?:[^/?#@]+@)?([^/:?#]+)'
                )
            ),
            ''
        )
    END
) STORED,
ADD COLUMN day DATE GENERATED ALWAYS AS (
    CASE
        WHEN time IS NULL THEN NULL
        ELSE (to_timestamp(time) AT TIME ZONE 'UTC')::date
    END
) STORED;
