CREATE OR REPLACE FUNCTION parse_items_parts_to_bigint_array(parts_text TEXT)
RETURNS BIGINT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN parts_text IS NULL OR btrim(parts_text) = '' THEN NULL::BIGINT[]
        WHEN parts_text ~ '^\s*\[.*\]\s*$' THEN ARRAY(
            SELECT value::BIGINT
            FROM jsonb_array_elements_text(parts_text::jsonb) AS value
        )
        WHEN parts_text ~ '^\s*\{.*\}\s*$' THEN parts_text::BIGINT[]
        ELSE string_to_array(parts_text, ',')::BIGINT[]
    END
$$;

ALTER TABLE items
ALTER COLUMN parts TYPE BIGINT[]
USING parse_items_parts_to_bigint_array(parts);

DROP FUNCTION parse_items_parts_to_bigint_array(TEXT);
