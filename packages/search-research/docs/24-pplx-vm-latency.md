# Pplx exact versus HNSW latency on the inference VM

Measured 2026-09-06 on `maya@magi06-inference`, not the M5 Pro/OrbStack laptop.
CPU reports AMD Ryzen 5 5600X under KVM with 8 exposed vCPUs; virtual CPU topology
must not be interpreted as eight physical cores. VM RAM is approximately 23 GiB.

## Method

A temporary official `pgvector/pgvector:0.8.2-pg17` container runs on VM loopback
port 55433. A dedicated removable volume holds only this experiment. Memory limit
10 GiB, shared memory 3 GiB, shared_buffers 2 GiB, effective_cache_size 8 GiB,
work_mem 128 MiB, maintenance_work_mem 1 GiB, JIT off, random_page_cost 1.1.
No CPU quota; the existing BF16 embedding server remains idle and receives no
benchmark requests. Other VMs or hypervisor contention are not controlled.

The same 64,638 cached BF16 document vectors and 196 queries are transferred,
with float32 normalized pgvector storage (`STORAGE EXTERNAL`). One HNSW index is
rebuilt on the VM with m=16 and ef_construction=128, serial construction. It is
not a byte-identical copy of the laptop's graph, so its accuracy is rescored.

One outstanding query at a time; two randomized passes over all questions and
randomized method order within each question. Each method has 392 measurements.
Sixteen queries per path warm the working set before measurement. The driver
runs on the VM over loopback. Timers include execute/fetch of 100 IDs, excluding
settings changes, embedding, BM25, fusion, SSH, and internet/network transit.
This is neither cold-cache latency nor a concurrent throughput test.

Automatic prepared statements are disabled so cached plans cannot blur the
serial/parallel controls. An initial driver pass was interrupted and discarded
when this control was added; the completed pass reused the built index. EXPLAIN
ANALYZE/BUFFERS runs separately and is not used as the reported latency timer.
All sampled plans show zero shared-buffer reads after warmup.

- Exact serial: max_parallel_workers_per_gather=0.
- Exact planner2: up to two workers allowed, ordinary planner cost thresholds.
- Exact forced2: two workers allowed, min_parallel_table_scan_size,
  parallel_setup_cost and parallel_tuple_cost set to zero for this session.
- HNSW ef_search=100/200/400/800, native indexed distance ordering.

Exact planner2 selected the same serial sequence scan as exact serial. The forced
plan launched two workers plus the leader with Gather Merge. HNSW used a serial
index scan. Exact search's sampled plan performs approximately 280,574 shared
buffer hits versus 12,225 for HNSW 800: these timings reflect the PostgreSQL
storage/TOAST path as well as distance calculation, not a bare matrix kernel.

## Results

Actual runtime: PostgreSQL 17.10, pgvector 0.8.2, image digest
`sha256:feb68f4f15446397d8cac7f4fe48fe4586de83160d1fc48b46283312d1a33966`.
HNSW index size 528,293,888 bytes; total vector table plus indexes 891,199,488 bytes.

| Dense retrieval path | Median ms | p95 ms | Exact top-100 recovery | Hybrid hits@20 /196 |
|---|---:|---:|---:|---:|
| Exact, serial | 159.26 | 163.98 | 100% | 158 |
| Exact, planner permits 2 workers | 159.15 | 164.15 | 100% | 158 |
| Exact, forced 2 workers + leader | 65.57 | 70.50 | 100% | 158 |
| HNSW 100 | 2.82 | 3.40 | 85.79% | 141 |
| HNSW 200 | 4.21 | 5.15 | 93.19% | 148 |
| HNSW 400 | 6.82 | 8.58 | 96.99% | 154 |
| HNSW 800 | 11.60 | 14.42 | 98.87% | 155 |

The timing columns cover dense lookup only. Hybrid quality is computed afterward
from these actual VM candidates and cached title-only BM25 lists, weight .125.
All exact modes have identical top-100 sets. Native dense target hits@20 are
153 exact, then 137/144/150/151 for HNSW 100/200/400/800. Small differences from
the laptop graph's accuracy reinforce that graphs must be evaluated separately.

Repeat medians are stable: serial exact 159.16/159.38 ms, forced parallel
65.53/65.60 ms, HNSW 800 11.72/11.39 ms. HNSW 800 is approximately 13.7x faster
than planner-selected exact and 5.7x faster than forced-parallel exact, at the cost
of three hybrid top-20 hits. HNSW 400 is faster still but loses four. Parallel
exact improves one-request latency by about 2.4x while using extra workers; no
concurrent throughput advantage is inferred from that result.

This establishes a substantial ANN benefit even on the two-year slice for this
PG storage layout and CPU. It does not measure final search endpoint latency,
justify linear extrapolation to the full corpus, or resolve filtered ANN recall.
No production setting is changed by this experiment.

## Reproduction and evidence

Driver: [VM timing script](../tools/pplx_vm_latency.py),
[DuckDB summaries](../tools/pplx_vm_latency_summary.sql), and
[local hybrid accuracy check](../tools/pplx_vm_latency_hybrid.py).
The script uses UV script dependencies and a dedicated temporary UV cache.

Artifacts are under `data/pplx-vm-latency-20260906/`: raw per-query samples,
plans, CPU description, actual image digest/version, summaries and runtime log.

## Cleanup completed

The raw sample file's SHA256 matched between VM and laptop before deletion.
Removed the temporary PG container, its dedicated volume, newly pulled pgvector
image, VM working directory including UV cache, and the duplicate local binary
vector export. Only the existing Pplx BF16 vLLM container remains running on the
VM. Cleanup output is retained in `cleanup.log`. The local result artifacts
remain subject to the overall verified-Garage archive/reap checklist.
