# Initial deployment verification — 2026-09-06

## Client documentation update — 2026-09-06

Proxy version `v0.1.0-20260906.3` is now deployed, digest
`sha256:8de1b70c2400f3319a4f1eb8a61f5ead4b4a7befbca528a852008ed82a47128a`.
It renders the client docs from Caddy's standard forwarding headers; no URL
environment variable or Caddy configuration change was required. Both live guides
returned the correct HTTPS links, including when a client sent bogus forwarding
headers. Readiness and one real 1024-coordinate integer embedding passed. Ten Rust
tests and Clippy passed; the Linux image build also ran the tests.

Only the proxy was recreated. vLLM's container ID and start time were unchanged.
The proxy now has the configured three 20 MB Docker log files; vLLM's rotation
remains pending its next planned recreation. The previous image reference is saved
on the VM in `/opt/homelab-inference/.env.before-docs-20260906`.

The original rollout measurements below describe the previous image.

Tranches 1 and 2 are deployed on `magi06-inference`. No database migration, embedding
backfill or application search cutover was performed.

## Published artifact

```text
magi07-registry.tail7a3eb.ts.net/homelab/embedding-proxy@sha256:b12853b363b744075d51bbdd3010787d60113154dbcc18f1632959d129d2bbc7
```

Version tag: `v0.1.0-20260906.2`. Built locally for linux/amd64 and pushed to Zot;
the VM pulled this digest. The administrator added `registry-consumer` to the
inference node to allow registry DNS/connectivity. No new credential was supplied.

| Measurement | Exact value | Rounded |
|---|---:|---:|
| Stripped Linux executable | 5,769,200 bytes | 5.50 MiB |
| Runtime image, unpacked | 96,111,135 bytes | 91.66 MiB |
| Compressed registry layers, summed | 37,604,747 bytes | 35.86 MiB |
| Proxy RSS before load | 6,156 KiB | 6.01 MiB |
| Proxy RSS after 16 concurrent requests | 10,792 KiB | 10.54 MiB |
| Proxy process high-water mark after that test | 12,068 KiB | 11.79 MiB |

Memory is `/proc/1/status` RSS/HWM, not Docker's working-set estimate; sampling is
one deployment check, not a promised maximum. The load used 16 interactive HTTP
requests of eight modest texts each. Runtime dynamic dependencies are libc, libm,
libgcc_s and the loader; no libpq, CUDA or Python. The Rust builder and compilation
cache were not deployed.

## Checks and limits

- Eight Rust tests pass, including golden coordinates, response shape/order,
  forwarding priority, overload, timeout/failure recovery, and cancelled-handler
  admission release. They also ran inside the Linux image build. Clippy with
  `-D warnings` and Rustdoc passed locally.
- [Native priority check](priority.json): 119 inputs were waiting. The later
  interactive single input completed at 1.696 seconds; the earlier bulk single
  input completed at 6.993 seconds. Both used the same text. The filler batch
  finished at 7.096 seconds. Native vLLM pooling priority works; no second
  scheduler/dispatcher was introduced. This is one bounded queue-order check,
  not a query-latency guarantee.
- [Captured transform check](transform-captured-196.json): all 196 evaluation
  queries, 200,704 coordinates; **zero** differences against NumPy when both
  transforms use the exact same pooled output. Tested with the final Linux image.
- [Deployed HTTPS smoke check](smoke.json): documents, readiness, models and both
  output contracts worked. Unknown workload and excessive tokens returned 400.
  All 16 concurrent requests succeeded.
- [Proxy lifecycle check](proxy-restart.json): with explicit administrator approval,
  stopped only the proxy; proxy returned 502 while raw vLLM returned 200. Starting
  the proxy restored readiness. vLLM's start timestamp stayed unchanged.
- Caddy, Docker and tailscaled are enabled. Both Compose services have
  `restart: unless-stopped`; vLLM is healthy. A whole-VM reboot was not performed.

### Model repeatability is separate from proxy correctness

The first test independently called raw vLLM and then the proxy, causing **two
inferences** for every batch. It found 23,734 different integer coordinates out of
200,704 ([record](transform-196.json)). An eight-query diagnostic repeating raw
vLLM alone also changed 1,606/8,192 quantized coordinates, with pooled maximum
absolute difference 0.0122772. The corresponding proxy comparison had the same
1,606 differences and maximum integer difference 2. This is evidence that repeated
BF16 pooling/batching can vary; it is not evidence that the proxy's math is wrong.
We have not isolated the precise GPU/kernel/batching cause or measured the retrieval
impact of these repeated-inference differences in this deployment slice.

The corrected check captures each pooled response on its way into the proxy, then
applies NumPy to that same response. That produced zero transform differences.
The failed independent-inference comparison is retained rather than overwritten.
Do not use bit-identical repeated inference as a production API promise. The later
combined retrieval evaluation remains tranche 6; it has not been claimed complete.

Synthetic arbitrary float32 inputs at rounding boundaries can also expose tiny
math-library tanh differences, described in the standalone fixture README. The
selected production Linux image and recipe identify the implemented transform.
