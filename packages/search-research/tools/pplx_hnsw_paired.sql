-- Paired target gains/losses against PostgreSQL exact search, separately for
-- each fusion weight. Approximation can occasionally improve a target rank;
-- gains do not imply that HNSW finds better nearest neighbors than exact search.
COPY (
 SELECT a.ef_search, a.method,
   count(*) FILTER (WHERE a."recall@8" > e."recall@8") AS gained8,
   count(*) FILTER (WHERE a."recall@8" < e."recall@8") AS lost8,
   count(*) FILTER (WHERE a."recall@20" > e."recall@20") AS gained20,
   count(*) FILTER (WHERE a."recall@20" < e."recall@20") AS lost20,
   avg(a."ndcg@20" - e."ndcg@20") AS ndcg20_delta
 FROM read_parquet('data/pplx-vllm-gate-20260905/bf16-full/hnsw-accuracy/ranks.parquet') a
 JOIN read_parquet('data/pplx-vllm-gate-20260905/bf16-full/hnsw-accuracy/ranks.parquet') e
   ON a."case"=e."case" AND a.method=e.method AND e.ef_search=0
 WHERE a.ef_search>0
 GROUP BY a.ef_search,a.method ORDER BY a.method,a.ef_search
) TO 'data/pplx-vllm-gate-20260905/bf16-full/hnsw-accuracy/paired.csv' (HEADER,FORMAT CSV);
