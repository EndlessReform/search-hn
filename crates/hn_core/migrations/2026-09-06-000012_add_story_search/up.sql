-- Additive rollout: no population scan and no changes to the existing FTS path.
-- Extensions must be packaged and pg_textsearch preloaded before this migration.
SET LOCAL lock_timeout = '5s';
CREATE EXTENSION IF NOT EXISTS vector VERSION '0.8.2';
CREATE EXTENSION IF NOT EXISTS pg_textsearch VERSION '1.4.0';
DO $$ BEGIN
    IF (SELECT extversion FROM pg_extension WHERE extname='vector') <> '0.8.2'
        OR (SELECT extversion FROM pg_extension WHERE extname='pg_textsearch') <> '1.4.0'
    THEN
        RAISE EXCEPTION 'search requires the verified vector 0.8.2 and pg_textsearch 1.4.0 extensions';
    END IF;
END $$;

CREATE TABLE story_search (
    story_id bigint PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
    title text NOT NULL,
    url text NOT NULL,
    embedding halfvec(1024),
    retry_after timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE story_search IS
    'pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1';
COMMENT ON COLUMN story_search.embedding IS
    'Final unnormalized signed int8 coordinates represented exactly in halfvec; cosine distance. NULL is pending work.';
CREATE INDEX story_search_title_bm25 ON story_search USING bm25(title)
    WITH (text_config='english', k1=1.2, b=0.75);
CREATE INDEX story_search_embedding_hnsw ON story_search
    USING hnsw(embedding halfvec_cosine_ops) WITH (m=16, ef_construction=128);
CREATE INDEX story_search_pending ON story_search(retry_after, story_id)
    WHERE embedding IS NULL;

-- One admission rule, also used by conditional saves. Missing HN flags mean false.
-- Positive Unix seconds and a generated calendar day constitute a valid timestamp.
CREATE FUNCTION story_search_eligible(source items) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT coalesce(source.type = 'story' AND source.score >= 25
        AND source.title ~ '[^[:space:]]' AND source.time > 0
        AND source.day IS NOT NULL
        AND NOT coalesce(source.dead, false)
        AND NOT coalesce(source.deleted, false), false)
$$;

-- Callers hold the source row lock: the trigger already does; backfill explicitly
-- locks before reading. Never synchronize a stale snapshot obtained before a lock.
CREATE FUNCTION sync_story_search(source items) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF story_search_eligible(source) THEN
        INSERT INTO story_search(story_id, title, url)
        VALUES (source.id, source.title, coalesce(source.url, ''))
        ON CONFLICT (story_id) DO UPDATE
        SET title = EXCLUDED.title, url = EXCLUDED.url,
            embedding = NULL, retry_after = now()
        WHERE (story_search.title, story_search.url)
            IS DISTINCT FROM (EXCLUDED.title, EXCLUDED.url);
    ELSE
        DELETE FROM story_search WHERE story_id = source.id;
    END IF;
END
$$;

CREATE FUNCTION story_search_source_changed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.type = 'story' THEN PERFORM sync_story_search(NEW); END IF;
    ELSIF (OLD.type = 'story' OR NEW.type = 'story') AND
        (OLD.type, OLD.score, OLD.title, OLD.url, OLD.time, OLD.dead, OLD.deleted)
        IS DISTINCT FROM
        (NEW.type, NEW.score, NEW.title, NEW.url, NEW.time, NEW.dead, NEW.deleted)
    THEN
        -- OLD.type recognizes sparse tombstones whose NEW.type is NULL.
        PERFORM sync_story_search(NEW);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER story_search_source_changed
AFTER INSERT OR UPDATE OF type, score, title, url, time, dead, deleted ON items
FOR EACH ROW EXECUTE FUNCTION story_search_source_changed();
