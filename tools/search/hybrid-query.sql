WITH dense_candidates AS MATERIALIZED (
 SELECT story_id, embedding <=> :'query_vector'::halfvec(1024) AS distance
 FROM public.story_search WHERE embedding IS NOT NULL
 ORDER BY embedding <=> :'query_vector'::halfvec(1024) LIMIT 100
), lexical_candidates AS MATERIALIZED (
 SELECT story_id, title <@> to_bm25query(:'query_text','story_search_title_bm25') AS distance
 FROM public.story_search
 ORDER BY title <@> to_bm25query(:'query_text','story_search_title_bm25') LIMIT 100
), dense AS (
 SELECT story_id, row_number() OVER (ORDER BY distance,story_id) AS rank FROM dense_candidates
), lexical AS (
 SELECT story_id, row_number() OVER (ORDER BY distance,story_id) AS rank FROM lexical_candidates WHERE distance < 0
), fused AS (
 SELECT coalesce(d.story_id,l.story_id) AS story_id,d.rank AS dense_rank,l.rank AS bm25_rank,
 coalesce(1.0/(60+d.rank),0) + coalesce(0.125/(60+l.rank),0) AS rrf
 FROM dense d FULL JOIN lexical l USING(story_id)
)
SELECT f.story_id,s.title,dense_rank,bm25_rank,round(rrf,6) AS rrf
FROM fused f JOIN public.story_search s USING(story_id)
ORDER BY rrf DESC,f.story_id LIMIT 10;
