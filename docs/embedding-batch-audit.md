# Embedding batch configuration audit — 2026-09-06

**Production is using four documents per request, not the research batch of 64.**
Three separately maintained code locations enforce this: worker settings, shared
HTTP client, and proxy request validation. The deployed TOML does not expose the
choice in its example. Increasing only one or two locations is insufficient.
This is an implementation/configuration divergence with a throughput consequence,
not a model requirement. Documentation of the resulting limits is not evidence
that the operator approved the tradeoff.

This audit changes no executable, deployment, database, or running job. It covers
all batch-related configuration and limits in the worker → shared client → Caddy →
proxy → vLLM path, plus persistence and documentation. It is not a general audit
of unrelated ingestion/search parameters.

## What was checked

- Working tree based on `a44c788`. Existing uncommitted Ansible fixes are unrelated.
- Live worker was identified earlier this session as v0.3.1, `841aa2a8d599`.
  `git diff 841aa2a8d599 HEAD` for the worker embedding module, shared embedding
  client, and proxy is empty: the reviewed implementation matches that worker
  source in these paths.
- Earlier live read of `/opt/search-hn/current/worker.toml` confirmed
  `embedding_batch_size` absent. Batch logs show `saved=4`, `discarded=0`, `delayed=0`.
- Proxy and vLLM configuration here is checked-in source plus recorded deployment
  evidence, not a new inspection of the inference host's running image/arguments.
  Before deployment, verify those against the actual image and Compose configuration.
- No new tests, production requests, or benchmark runs were needed for this source audit.

## A. Configuration surface

### Worker and backfill

Definition: [EmbeddingArgs](../crates/catchup_worker/lib/embeddings/mod.rs), lines 16–73.
These fields live under `[embedding]` in TOML. Only `base_url` is renamed by serde;
other fields retain their full `embedding_` prefix. `batch_size = 64` would be an
unknown field, not the correct spelling.

| TOML key | Current default | Validation / effect |
| --- | --- | --- |
| `enabled` | false in TOML loader | Controls updater loop; explicitly invoked backfill still runs |
| `base_url` | absent | Required to construct client; proxy API base, not raw vLLM |
| `embedding_batch_size` | **4** | **1–8**; same setting for updater and backfill |
| `embedding_poll_seconds` | 5 | Positive; updater sleeps only when no due work, not after every batch |
| `embedding_retry_seconds` | 60 | Positive, <= i32::MAX; durable retry delay after failures |
| `embedding_invalid_retry_seconds` | 3600 | Positive, <= i32::MAX; isolated invalid document delay |
| `embedding_timeout_seconds` | 40 | Positive; worker HTTP deadline |

The default four is duplicated in the clap declaration and `Default` implementation.
TOML uses the latter. Legacy CLI exposes `--embedding-batch-size`; there is no
batch-size environment variable on this field. With `--config`, embedding CLI
flags are explicitly rejected, so appending a batch flag to the existing command
is not an override ([main.rs](../crates/catchup_worker/src/main.rs), lines 583–599).

The operational example [worker.example.toml](../infra/ansible/worker.example.toml)
contains only `enabled` and `base_url` under `[embedding]`. It omits every timing
and batch knob above. Consequently the ordinary documented setup inherits four
without the operator seeing or selecting it.

Backfill has a separate `--source-chunk-size` default 1000, allowed 1–10000, and
start/end ID bounds. That is database seeding pagination, **not GPU batch size**.
Its pool has two connections. It seeds history first, then processes embedding
batches sequentially. It does not pipeline the next HTTP batch with database saves.
See [command.rs](../crates/catchup_worker/lib/embeddings/command.rs), lines 11–86.

### Proxy and model

[Proxy Config](../crates/embedding_proxy/src/config.rs) exposes listen address,
backend origin, upstream timeout (default 30 seconds, allowed 1–300), and interactive
outstanding-request limit (default 16, allowed 1–128). It exposes **no configurable
input-count cap, byte cap, or bulk concurrency**. Those require a proxy rebuild today.

[Compose](../deploy/inference/compose.yaml) supplies backend origin and logging,
leaving proxy timeout and admission at defaults. vLLM is configured with:

- `--max-num-seqs 64`: scheduler sequence capacity, not the proxy HTTP allowance.
- `--max-num-batched-tokens 8192`: scheduler token budget; 64 long documents need
  not execute together in one GPU operation.
- `--max-model-len 2048`: per-document token limit.
- BF16, mean pooling, pinned revision, eager execution and priority scheduling.
- `--gpu-memory-utilization 0.35`, zero KV cache for encoder pooling. These are
  existing model-serving settings, not a four/eight-document cap.

## B. Internal hardcoding and its impact

| Location | Limit / behavior | Consequence of increasing batch size |
| --- | --- | --- |
| Worker `lib/embeddings/mod.rs:27,44,61` | Default 4; validation ceiling 8 | Must change both defaults and validation or explicitly configure supported size |
| Shared client `hn_core/src/embeddings.rs:111` | Max 8 inputs, 8192 bytes each, 32768 bytes total | Rejects before HTTP, independently of proxy |
| Proxy `src/contract.rs:44–55` | Same count/per-input/aggregate limits | Applies equally to interactive and bulk; validator does not receive workload class |
| Proxy `src/lib.rs:65` | 256 KiB serialized JSON request body | Separate from text-byte sum; JSON escaping consumes additional bytes |
| Proxy `src/lib.rs:175` | 2 MiB successful upstream response | Applies to raw 1024-dimensional float JSON before int8 conversion; must be checked against a 64-document response, not blindly removed |
| Proxy `src/lib.rs:158` | 64 KiB upstream error body | Diagnostic limit, not normal batch throughput |
| Proxy `src/lib.rs:49–56` | 3-second connect timeout; configurable 30-second request timeout; one bulk permit | One outstanding bulk call across clients; overload returns 429, no proxy queue |
| Worker `lib/embeddings/worker.rs:26–44` | Splits any `InvalidInput` batch recursively | Can conceal unchanged client/proxy limits by making smaller requests |
| Worker `worker.rs:31–35` / DB `story_search.rs:76` | Awaits one conditional SQL save per vector | 64 inputs still mean up to 64 sequential DB round trips; larger HTTP batches do not automatically remove this bottleneck |

**Specific hidden failure:** increasing worker validation and proxy to 64 while
leaving the shared client unchanged makes the client reject 64 locally. The worker
splits 64 → 32 → 16 → 8 and sends eight requests of eight. Its final log can still
say `saved: 64`, because that counter aggregates across the splits. It does not
prove that one request contained 64 documents.

A count-only change also leaves the 32768-byte cap: at 64 documents that is only
512 bytes per document on average. Sixty-four individually allowed 8192-byte
inputs total 512 KiB before JSON escaping, exceeding the existing 256 KiB body cap.
A change must deliberately align these limits, or clearly retain/document a smaller
aggregate budget and consequent splitting. Neither outcome should be implicit.

The checked-in [Caddyfile](../deploy/inference/Caddyfile) adds no explicit request
body cap; it strips `/embeddings` and forwards to the proxy. It also exposes a raw
vLLM route, but changing to that route is **not** an equivalent fix: it bypasses
priority admission and the proxy's output transform/recipe contract.

### Correctness and operational blast radius

- Count/size/admission limits do not change the schema, model revision, tokenizer,
  vector dimensions, or int8 transform. No database migration or wholesale deletion
  of completed embeddings is required to adjust them.
- Worker and shared client compile into the worker artifact. Proxy is a separate
  container artifact. This means **two deployables**, not three services to rebuild.
  The model container need not restart for proxy-cap changes.
- Changing the common proxy count limit affects **all its clients**, both workload
  classes. A bulk-only increase requires distinguishing workload in validation;
  this is a choice to make explicitly, not silently add in implementation.
- Larger bulk calls can occupy the GPU longer before interactive work runs; there
  is no measurement here establishing four/eight as the right latency compromise.
  Keep concurrency and batch-size decisions separate.
- Per-vector conditional writes check source eligibility, matching title/URL and
  pending state. Successful writes survive backfill interruption; rerunning seeds
  again but does not clobber unchanged completed embeddings.
- A transient failure exits one-off backfill; delayed rows can remain pending.
  Completion requires its final pending count/exit status, not merely an active unit.
- Existing [deployment evidence](../deploy/inference/evidence/README.md), section
  “Model repeatability”, already records BF16 repeated-inference differences. A
  batch-size change is not a promise of bit-identical recomputation. That evidence
  does not establish corruption of saved vectors or require a new storage recipe.

## C. Documentation and provenance

| Surface | What it actually says | Missing connection |
| --- | --- | --- |
| [Research backfill](../packages/search-research/tools/pplx_vllm_backfill.py):44,59 | Writes batch_size 64 and loops in groups of 64 | Calls raw vLLM through localhost:58080, bypassing proxy caps |
| [Research gate](../packages/search-research/tools/pplx_vllm_gate.py):119–143 | Benchmarks 1,8,16,32,64; includes longest 64 | This audit located the procedure, not a newly verified numeric knee result |
| [Production design](../packages/search-research/docs/production-design.md):95–99 | Keep bulk batches modest; native priorities | No explicit four/eight choice or quantified departure from research |
| Production design:198–207 | Keep tested BF16 setup; one batch at a time; expose size to operator | Reads as operator-controlled tuning, without mentioning the hard eight ceiling |
| [Worker example](../infra/ansible/worker.example.toml) | enabled/base_url only | No batch setting or warning that default is four |
| [Search operations](search.md):68,76 | CLI example four; table default four, range 1–8 | Values are documented here, but not surfaced in deployment TOML/runbook |
| [Proxy README](../crates/embedding_proxy/README.md):36 | Both classes max eight | Separately lists vLLM max-num-seqs 64 at line 67 |
| [Served proxy usage](../crates/embedding_proxy/docs/usage.md):82–86 | 1–8, byte limits, 256 KiB body, 30-second timeout | Describes restrictions; does not explain selecting eight against the batch-64 research |
| [Deployment decisions](deployment-decisions.md) | Configuration and rollout decisions | No located explicit decision approving the 64 → 4/8 throughput tradeoff |

Introduction history:

- `9e00ca1` (shared embedding proxy): eight-input and byte restrictions introduced.
- `1b8268e` (worker/backfill): default four, ceiling eight, and shared-client caps.
  Comment explicitly cites keeping background GPU operations short.
- `58fc6c9` (TOML/deployment): serde defaults and the abbreviated TOML example carry
  those settings into deployment without exposing the batch value.

Git author metadata uses the operator's account. It cannot establish which agent
chose a value or that the operator approved it. The evidence establishes where the
choices entered code, not consultation. Finding the restriction in secondary docs
is not a reason to blame the operator for expecting the measured research setting.

## Exact scope of a correction, for decision

1. Decide whether 64 is bulk-only or both classes; do not couple this silently to
   interactive concurrency or scheduler changes.
2. Align worker default/validation, shared client and proxy input count and byte
   budgets. Account for serialized body size and raw response size. These are the
   complete count/size enforcement points located in the audited repository path.
3. Put `embedding_batch_size = 64` visibly in the operational TOML example and
   actual source TOML. Document the remaining defaults and correct field spelling.
4. Update contradictory docs. Make actual HTTP input count visible separately from
   saved-row totals so splitting cannot masquerade as a batch-64 request.
5. Rebuild worker and proxy; deploy proxy before the new worker. Verify a real
   request contains 64 and returns 64 through the proxy, not just that saved=64.
   Existing compile/check infrastructure suffices; no new testing framework or
   benchmark project is required to alter these limits.

Unverified boundaries: live inference-host drift from checked-in Compose/Caddy,
64-request raw response bytes on that exact running model, and resulting concurrent
interactive latency. These are specific checks, not reasons to add infrastructure.
