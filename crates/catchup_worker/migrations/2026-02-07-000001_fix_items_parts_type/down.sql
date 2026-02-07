ALTER TABLE items
ALTER COLUMN parts TYPE TEXT
USING (
    CASE
        WHEN parts IS NULL THEN NULL
        ELSE parts::TEXT
    END
);
