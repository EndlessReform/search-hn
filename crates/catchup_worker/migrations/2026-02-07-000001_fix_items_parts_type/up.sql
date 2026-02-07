ALTER TABLE items
ALTER COLUMN parts TYPE BIGINT[]
USING (
    CASE
        WHEN parts IS NULL OR btrim(parts) = '' THEN NULL
        WHEN parts ~ '^\s*\[.*\]\s*$' THEN ARRAY(
            SELECT value::BIGINT
            FROM jsonb_array_elements_text(parts::jsonb) AS value
        )
        WHEN parts ~ '^\s*\{.*\}\s*$' THEN parts::BIGINT[]
        ELSE string_to_array(parts, ',')::BIGINT[]
    END
);
