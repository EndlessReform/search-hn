-- Run after PG17/main restarts, once shared_buffers can hold the working set.
-- The pg_prewarm extension is installed by migration 20260907000013.
-- Loading pages is repeatable and does not pin them against future eviction.
\set ON_ERROR_STOP on
\timing on
SELECT public.pg_prewarm('public.story_search'::regclass, 'buffer');
SELECT indexrelid::regclass AS index_name,
       public.pg_prewarm(indexrelid, 'buffer') AS blocks_loaded
FROM pg_index
WHERE indrelid = 'public.story_search'::regclass;

-- halfvec values can live out of line. Warming the main relation does not
-- recursively warm TOAST, which index scans may read to return/recheck vectors.
SELECT reltoastrelid::regclass AS toast_table,
       public.pg_prewarm(reltoastrelid, 'buffer') AS blocks_loaded
FROM pg_class
WHERE oid = 'public.story_search'::regclass AND reltoastrelid <> 0;
SELECT indexrelid::regclass AS toast_index,
       public.pg_prewarm(indexrelid, 'buffer') AS blocks_loaded
FROM pg_index
WHERE indrelid = (
    SELECT reltoastrelid FROM pg_class
    WHERE oid = 'public.story_search'::regclass
);
