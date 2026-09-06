-- Timings are VM-loopback query execution/fetch only, one request at a time.
CREATE TEMP TABLE samples AS SELECT * FROM read_json_auto('data/pplx-vm-latency-20260906/samples.jsonl');
COPY (SELECT mode,count(*) n,median(ms) median_ms,quantile_cont(ms,.95) p95_ms,
 avg(ms) mean_ms,min(ms) min_ms,max(ms) max_ms FROM samples GROUP BY mode ORDER BY mode)
 TO 'data/pplx-vm-latency-20260906/summary.csv' (HEADER,FORMAT CSV);
COPY (SELECT mode,repeat,median(ms) median_ms,quantile_cont(ms,.95) p95_ms FROM samples GROUP BY mode,repeat ORDER BY mode,repeat)
 TO 'data/pplx-vm-latency-20260906/by-repeat.csv' (HEADER,FORMAT CSV);
COPY (SELECT a.mode,avg(len(list_intersect(a.ids,e.ids))/100.0) overlap100,
 count(*) FILTER (WHERE list_position(a.ids,a.target_id)<=20) hits20,
 count(*) FILTER (WHERE list_position(a.ids,a.target_id)<=8) hits8
 FROM samples a JOIN samples e ON a."case"=e."case" AND a.repeat=e.repeat AND e.mode='exact_serial'
 WHERE a.repeat=0 GROUP BY a.mode ORDER BY a.mode)
 TO 'data/pplx-vm-latency-20260906/quality.csv' (HEADER,FORMAT CSV);
