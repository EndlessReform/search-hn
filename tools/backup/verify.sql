-- Run only against the freshly restored database. Counts describe the dump snapshot.
SELECT 'items' AS table_name, count(*) AS rows FROM items
UNION ALL SELECT 'kids', count(*) FROM kids
UNION ALL SELECT 'users', count(*) FROM users
UNION ALL SELECT 'ingest_segments', count(*) FROM ingest_segments
UNION ALL SELECT 'ingest_exceptions', count(*) FROM ingest_exceptions
UNION ALL SELECT 'ingest_dlq_items', count(*) FROM ingest_dlq_items
UNION ALL SELECT 'updater_state', count(*) FROM updater_state
UNION ALL SELECT '__diesel_schema_migrations', count(*) FROM __diesel_schema_migrations;
SELECT version FROM __diesel_schema_migrations ORDER BY version;
SELECT indexrelid::regclass AS invalid_index FROM pg_index
WHERE indrelid IN (SELECT oid FROM pg_class WHERE relnamespace='public'::regnamespace)
AND (NOT indisvalid OR NOT indisready);
SELECT conrelid::regclass AS table_name, conname, contype, convalidated
FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY 1, 2;
SELECT to_regclass('public.story_search') AS search_table_before_migration;
SELECT id FROM items
WHERE type = 'story' AND search_tsv @@ plainto_tsquery('simple', 'postgresql')
LIMIT 5;
