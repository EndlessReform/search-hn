# Reproducing the retrieval study

The [whitepaper](../whitepaper/search-hn.typ) contains methods, results, and the
selected recipe. This guide covers restoring evidence and running the harness.
Commands run from the repository root with the committed UV lockfile.

## Restore evidence

Garage is canonical: bucket `searchhn-data` at
`http://magi06-storage.tail7a3eb.ts.net:3900` on the private Tailscale network.
Credentials are loaded from `packages/search-research/.env` using
`SEARCHHN_EVAL_DATA_BUCKET`, `SEARCHHN_GARAGE_BASE_URL`, `AWS_ACCESS_KEY_ID`, and
`AWS_SECRET_ACCESS_KEY`; `AWS_DEFAULT_REGION` defaults to `garage`.

| Release | Contents | Logical size |
|---|---|---:|
| `research-20260906-v4` | Frozen evaluations/corpora, rollouts, scores, sweeps, serving evidence, selected Pplx vectors | 871,885,024 bytes / 8,987 files |
| `research-20260906-closeout` | Source snapshot, original paper, pinned Typst template, VM setup files and cleanup receipts | 1,587,840 bytes / 163 files |
| `research-20260906-paper-v2` | Five-page editorial revision: PDF and Typst source | 176,590 bytes / 3 files |
| `research-20260904-v3` | Original FTS/TE3 study, TE3 vectors, question generation/review and miss audit | 2,157,854,526 bytes / 1,680 files |

```sh
uv sync --locked --package search-research
uv run --locked --package search-research python -m search_research.artifacts restore research-20260906-v4
uv run --locked --package search-research python -m search_research.artifacts verify research-20260906-v4
# Source and historical instructions, restored outside the working experiment tree:
uv run --locked --package search-research python -m search_research.artifacts restore research-20260906-closeout --destination /absolute/path/to/closeout
```

Restore verifies downloaded SHA256 bytes and refuses to overwrite differing
files. Interrupted downloads use temporary files before a verified rename.
The companion restores beneath `data/research-closeout-20260906/`. Its source
snapshot records uncommitted code explicitly; a release's Git revision alone
does not identify every historical execution. The numbered lab notes and their
exact commands are also available at Git commit `27f572f`, for example:

```sh
git show 27f572f:packages/search-research/docs/24-pplx-vm-latency.md
```

Use that historical record for older FTS, engine, reranker, and model-comparison
procedures. These notes have been removed from active documentation. The paper
and this guide supersede their interim recommendations and cleanup status.

Release manifest SHA256 values:

- v4: `404830d1204129aff6866cb52d5061de2be9fd8704138d6660675d4b242fb136`
- closeout: `ce39eb92f30830cc4dd836b9a59caef694342b99556721c6c6a9c6316af44f4f`
- paper-v2: `177b04ad0fbb23bc2c0ca0be9227a7173f3b564939f4bc57440d472c04024ca1`
- v3: `407b785146fd703858df0dae33449ffe0c75f667832495011eb7ae585ee699d0`

## Frozen evaluation and run contract

The model bakeoff uses 64,638 eligible stories at score >=25, from 2024-09-04
through 2026-09-04. Documents are title, newline, URL; self-posts are included.
The earlier engine/TE3 study used 105,081 stories at score >=10.
The shared evaluation has 196 synthetic questions over 98 targets: one entity
question and one paraphrase each, with 120 recent and 76 older questions.

| Input | SHA256 |
|---|---|
| `data/luna-semantic-20260904/corpus.parquet` | `5a7b46f7ba78e1a1978b2aca947954168c284a4e0bd205ad310c5a9af6ed2dfd` |
| `data/te3-large-baseline-20260904/questions.parquet` | `737382c55bee6050050bfa712757cab865c45abe38b45477db1ac53aea067e4a` |

The dataset's seed is 20260904. Luna generated questions from frozen story and
comment snippets; separate review checked source support and identifying-info
leakage before retrieval scoring. Restore the final `eval.jsonl` and generation/
review journals rather than regenerating questions for a comparison. This set
has been inspected during tuning; use fresh natural questions for acceptance.

Static scoring uses one labeled target per question, Recall/nDCG at 8 and 20,
and both query-style/cohort splits. Bootstrap by target story, keeping its two
questions together. ANN recovery compares candidate sets with exact neighbors.
Agent exposure means the target entered consumed context; final evidence citation
is a separate measure. Keep budget exhaustions in quality denominators and
preserve infrastructure-failure attempts separately.

Use a fresh output directory for a new treatment. Resume only an unchanged
scientific manifest. Drivers journal individual attempts and skip terminal cases;
never erase a ledger or partial trace to make a run appear clean. Fresh agent
runs depend on model serving and live comment reads and are not deterministic
replays. Cached reports can be regenerated offline on a working copy.

## Selected serving and retrieval recipe

The pinned [vLLM Compose file](../compose.pplx-vllm.yaml) and
[launch script](../tools/launch_pplx_vllm.sh) describe the service on
`maya@magi06-inference`, under `/opt/searchhn-embeddings`. The VM has Docker and
an RTX 3060 with 12 GiB VRAM. Bind inference to VM loopback port 8080; the
benchmark uses an SSH tunnel to local port 58080. Check for an existing container
before launching: Compose describes the same service created by the launch script.

- Stock vLLM 0.28.0, image digest pinned in Compose; model revision
  `2c4d510dd4a732063c31a0f70193e35067b51fd8`.
- Native Qwen3 bidirectional attention, mean pooling, activation disabled, BF16,
  Flash Attention, eager execution. No custom webserver or vLLM fork.
- 64 sequences, 8192 batched tokens, 2048-token input limit, zero KV cache;
  prefix caching and chunked prefill disabled. The 0.35 memory-utilization setting
  is a planning parameter, not a hard VRAM cap. Long batches may be split.
- The client applies float32 tanh, multiply by 127, round, clamp to int8, then
  cosine normalization. Raw pooled output is not the final Pplx representation.
- Retain 1024 dimensions. For a dimension ablation, take the vector prefix and
  L2-normalize documents and queries again.
- PostgreSQL title-only `pg_textsearch` BM25; top 100 candidates per branch;
  RRF constant 60, dense weight 1, lexical weight 0.125. Weight zero excludes
  lexical-only candidates. Exact dense search remains the reference.
- pgvector 0.8.2 HNSW: `m=16`, `ef_construction=128`, provisional `ef_search=1000`.
  Record graph rebuilds: approximate results can vary between builds.

Historical comparison recipes are pinned in
[`tei_embeddings.py`](../src/search_research/tei_embeddings.py) and the TEI
Compose files. Pplx was FP32/mean/no prefix; Qwen FP16/last-token uses the exact
instruction prefix in `QWEN_RECIPE`; Jina retrieval FP16/last-token uses
`Query: ` / `Document: `; Nemotron BF16/bidirectional mean uses
`query: ` / `passage: `. Preserve revision, role formatting, output transform,
and normalization together. Rejected-model arrays were deleted; their scores,
recipes, and reference checks are archived. Recomputing those arrays requires
restoring the appropriate serving environment and pinned weights.

## Harness entry points

`hn-eval` covers the original dataset/FTS workflow. Later studies use Python
modules and scripts; the [README code map](../README.md#code-map) identifies them.
Several retain frozen paths, model arms, scratch tables, and ports. Inspect their
configuration before a new experiment, especially `cli.py`'s historical local
endpoint default. Restoring evidence does not start a database or model server.

```sh
# Regenerate finalist reports from restored traces (offline):
uv run --locked --package search-research python -m search_research.sovereign_e2e_report data/sovereign-e2e-20260905
# Recompute the cached BF16 lexical-weight sweep:
uv run --locked --package search-research python packages/search-research/tools/pplx_hybrid_sweep.py --root data/pplx-vllm-gate-20260905/bf16-full
```

The BF16 compatibility/pilot and backfill tools are `pplx_vllm_gate.py` and
`pplx_vllm_backfill.py`. They require the selected endpoint and frozen inputs.
HNSW accuracy uses `pplx_hnsw_accuracy.py` / `pplx_ef1000_accuracy.py` against a
rebuilt scratch PG database. VM timing uses `pplx_vm_latency.py` (with `--ef1000`
for the matched follow-up), plus the adjacent SQL summary and hybrid-scoring
tools. Archived `data/pplx-vm-latency-20260906/` contains samples, query plans,
hardware details and image/version evidence. The original database fixtures
were removed; use the historical recipe above to recreate them.

The finalist agent run used Pplx FP32, Qwen FP16, and Nemotron BF16 with exact
dense retrieval. `sovereign_repository.py` pins their separate tables/endpoints;
the current single BF16 service cannot stand in for all three historical arms.
Once the original prerequisites are restored, the recorded run/resume command is:

```sh
uv run --locked --package search-research python -m search_research.sovereign_rollouts --concurrency 16 --budget-usd 8
```

This makes paid calls. The $8 value is a cumulative run ceiling, not a current
credit balance. The original protocol used Luna via OpenRouter's OpenAI route,
16 shared conversations, ten turns, and 4096 output tokens per request. It
reserves cost before each streamed request; unknown billing retains the
reservation. Budget/access failures pause dispatch while in-flight calls drain.
Individual failures preserve attempts without cancelling siblings. To resume
after recharging, explicitly raise the total allowance; keep the existing ledger.
TE3's earlier direct-API agent run remains historical context, not a matched arm.

## TEI reproduction workaround

TEI was removed from the VM after selecting vLLM. Retained Compose files pin
`ghcr.io/huggingface/text-embeddings-inference:86-1.9.3` at digest
`a7d82dfef16c3bf1a95e93f5b226f358312512dbb0d585b48c3cf886f9d470a9`.
The operator-installed NVIDIA driver 610.57.04 reported `CUDA UMD Version: 13.3`.
TEI's shell entrypoint expected `CUDA Version`, misparsed that output and selected
`/usr/local/cuda/compat`; the resulting compatibility shim caused `cuInit=803`.

Override the entrypoint with `/usr/local/bin/text-embeddings-router`, as recorded
in the Compose files, keeping the image's normal library path. GPU allocation
and readback then passed. This required neither a TEI fork nor disabling NVIDIA
container compatibility checks. The host needs working driver/compute libraries
and the container toolkit; no host CUDA development toolkit was needed. The
workaround applies to that observed image/driver combination. Full diagnostics
are in Git `27f572f` and the Garage source/setup snapshot.

## Retention and future publication

Cleanup completed on 2026-09-06 after downloading and hashing all 8,922 unique
v4 objects. Removed local experiment trees (3,726,463,861 logical bytes), the
scratch PG volume (~8.79 GB) and image, and VM TEI/rejected-model environments,
weights, and stopped containers. VM free disk increased by 21,885,259,776 bytes.
Selected Pplx weights and the BF16 vLLM service were retained; host driver/toolkit
and unrelated data were untouched. Machine receipts remain under `docs/evidence/`.

Preserve frozen inputs, rollouts including failures, per-case results, recipes,
source snapshots, and selected vectors in Garage. Rebuildable databases and
rejected-model arrays are disposable after their evidence is safely archived.

Publication uses content-addressed blobs and an immutable manifest published last.
Stop writers, review the file inventory, publish a new release, then verify actual
downloaded bytes before reaping working copies. `artifacts publish` enumerates
only the four original experiment roots. The later
[`research_closeout.py`](../tools/research_closeout.py) has an explicit expanded
allowlist and byte verifier; adapt/review that policy for a new study rather than
assuming either publisher automatically discovers every artifact. Prior releases
remain immutable; a new configuration or repaired question set gets a new version.
