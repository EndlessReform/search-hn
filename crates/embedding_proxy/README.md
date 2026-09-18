# Embedding proxy

A standalone Rust HTTP service for shared Pplx embeddings. It validates small text
batches, forwards interactive/bulk priority to vLLM, and returns final integer
coordinates. It has no application, database, GPU or model-weight dependencies.

**Client integration:** read [the self-contained API guide](docs/usage.md). This same
template is served at `/usage.md` by the binary and `/embeddings/usage.md` through
Caddy. `/llms.txt` links agents to it. For each request, the proxy replaces
`{{PUBLIC_BASE_URL}}` in both documents with the origin supplied by Caddy.
The live guide has working URLs and tells client agents not to disrupt this shared
service; no deployment hostname is baked into the image.

## Run and configure

From this repository's `crates/` directory:

```sh
cargo run --locked -p embedding_proxy -- --backend http://127.0.0.1:8080
cargo test --locked -p embedding_proxy
cargo doc --locked -p embedding_proxy --no-deps --open
```

After extraction into its own repository, omit `-p embedding_proxy`. All dependency
versions and features are explicit; no parent files are needed at runtime. Copy the
workspace lockfile when extracting and let Cargo prune unrelated entries.

| Environment variable | Default | Purpose |
|---|---|---|
| `EMBED_LISTEN` | `0.0.0.0:8081` | HTTP bind address; publish to loopback behind TLS |
| `EMBED_BACKEND` | `http://127.0.0.1:8080` | vLLM origin, without `/v1` |
| `EMBED_TIMEOUT_SECONDS` | `30` | Upstream deadline including scheduling wait |
| `EMBED_INTERACTIVE_LIMIT` | `16` | Concurrent interactive HTTP calls |
| `RUST_LOG` | set `info` | Standard tracing filter; logs counts/timing/errors, not input text |

Bulk admission is one outstanding call. Both classes allow at most eight inputs per
call. When full, return 429 immediately; clients keep unfinished work and back off.
There is no local job queue, automatic retry or persisted state. `/healthz` is local
liveness; `/readyz` checks the engine's `/health`. Model listing describes the pinned
contract, not current engine readiness.

Caddy already sends `X-Forwarded-Proto` and `X-Forwarded-Host`, replacing values
supplied by clients ([Caddy's header defaults](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#defaults)).
The proxy uses these to generate working URLs automatically;
there is no public-URL environment variable or image rebuild when the hostname
changes. Keep the proxy behind Caddy as in the deployment Compose file. When
inspecting the proxy directly over local HTTP, docs use the request's `Host` header;
the API paths in the guide describe the external Caddy layout.

## Required vLLM recipe

Use stock `vllm/vllm-openai` 0.28.0, pinned image:

```text
vllm/vllm-openai@sha256:61fc8a896b0a4fbbbdc063bc4b0dbc25ce98e02b5050c24aeb7830ac02039b14
```

The output-affecting and scheduling arguments are:

```text
perplexity-ai/pplx-embed-v1-0.6b
--revision 2c4d510dd4a732063c31a0f70193e35067b51fd8
--trust-remote-code --model-impl vllm --runner pooling --convert embed
--hf-overrides '{"architectures":["Qwen3Model"],"is_causal":false}'
--pooler-config '{"pooling_type":"MEAN","use_activation":false}'
--dtype bfloat16 --attention-backend FLASH_ATTN --max-model-len 2048
--max-num-seqs 64 --max-num-batched-tokens 8192 --kv-cache-memory-bytes 0
--gpu-memory-utilization 0.35 --no-enable-prefix-caching
--no-enable-chunked-prefill --enforce-eager --scheduling-policy priority
--host 0.0.0.0 --port 8000 --served-model-name pplx-embed-v1-0.6b
```

Provide persistent HF/model caches and GPU access only to vLLM. These settings are
operator-owned: the proxy cannot inspect which weights a same-named backend loaded.
Its recipe identifier assumes this deployment contract. Any output-affecting change
requires updating the recipe string and coordinating client indexes.

`src/contract.rs` owns validation and the float32 tanh/round-ties-even transform;
`src/lib.rs` owns admission and one-attempt forwarding. Rustdoc explains the details.
`fixtures/` contains standalone regression inputs, independent of research archives.

## Build and publish separately

Docker builds a stripped executable in a Rust builder stage, then copies it into a
Debian slim runtime with CA certificates. The deployed image has no compiler,
CUDA, Python, model weights, or PostgreSQL libraries. It runs as UID/GID 65532 with
no writable application state. The executable can also run directly on compatible
Linux without Docker.

From the crate directory, with Docker Buildx, Cargo and jq available:

```sh
./publish.sh magi07-registry.tail7a3eb.ts.net/homelab/embedding-proxy YOUR_VERSION_TAG
```

The helper stages this crate only, prunes the existing lockfile without upgrading
dependencies, builds `linux/amd64`, pushes and prints `repository@sha256:...`.
It uses existing Docker authentication; if your registry requires it, use
`docker login REGISTRY` first. Never put credentials in source files or image tags.
It does not deploy, modify Compose or move a `latest` tag. Reuse neither version tags
nor recipe identifiers for materially different releases.

A standalone checkout can also run `docker build .` directly once it owns its
Cargo.lock. The builder cache stays on the build machine. To update only this service,
set its new image digest in the deployment and use `docker compose up -d --no-deps
embedding-proxy`. Do not use `docker compose down` for a proxy update.

The deployment operator's Caddy/Compose configuration and rollback instructions live
outside this crate. Client applications need only the HTTP guide, not those files.
