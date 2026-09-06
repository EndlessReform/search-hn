# Shared homelab embedding API

This service embeds text for any application on the tailnet. No knowledge of the
hosting repository or its applications is needed. Access is controlled by tailnet
connectivity and ACLs; this deployment does not require an application API key.

**This is shared infrastructure.** Integrate through HTTP. Do not stop, restart,
reconfigure or redeploy the service, change the model, or run competing model
containers just to connect a new client. Ask the administrator to make operational
changes. Other users may be active even when your own application is idle.

## Address and first request

On the server hosting this document, the recommended API base is `/embeddings/v1`.
For this deployment:

```sh
export EMBEDDING_BASE_URL={{PUBLIC_BASE_URL}}/embeddings/v1
curl --fail-with-body "$EMBEDDING_BASE_URL/embeddings" \
  -H 'Content-Type: application/json' \
  -H 'X-Embedding-Workload: interactive' \
  -d '{"model":"pplx-embed-v1-0.6b","input":["How do database indexes work?"],"encoding_format":"float"}'
```

`GET /embeddings/v1/models` lists the supported model. `GET /embeddings/healthz`
checks the proxy; `GET /embeddings/readyz` also checks vLLM. Documentation is at
`/embeddings/usage.md` and `/llms.txt` on this host.

Send `model` and `input`, where input is one string or an array of strings. The only
optional body field is `encoding_format: "float"`, meaning JSON numbers. Other
options (token ID inputs, dimensions, base64, truncation, arbitrary priority) are
rejected rather than silently ignored. SDKs that default to base64 must explicitly
request `encoding_format="float"`. No task/query prefixes are added by the service.

## Output and storage

The response has the conventional `object: "list"`, `model`, `data`, and `usage`
fields. Each `data` entry has `object: "embedding"`, `index` and `embedding`.
There is one 1024-coordinate vector for each input, returned in input order.
Coordinates are **final signed int8 values, serialized as JSON numbers**, without
L2 normalization. Despite the conventional encoding name `float`, all coordinates
are integer-valued. Store them directly; do not apply tanh or quantization again.

Use cosine similarity. Promote coordinates to float32 or wider before multiplying
or summing: int8 arithmetic can overflow. A half-precision float vector can represent
each returned integer exactly; an int8 array is also lossless if your index supports
it. Normalize only if your chosen index needs normalized vectors, consistently for
both documents and queries. Never mix embeddings from different model recipes.

The JSON field `embedding_recipe` and HTTP header `X-Embedding-Recipe` both contain:

```text
pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1
```

Record this identifier with your index and require it on query and indexing
responses. A different identifier requires an explicit compatibility decision or
rebuild, rather than silently adding incompatible vectors.

This recipe pins `perplexity-ai/pplx-embed-v1-0.6b` revision
`2c4d510dd4a732063c31a0f70193e35067b51fd8`, vLLM 0.28.0, BF16, FLASH_ATTN,
non-causal mean pooling without activation, and a 2048-token serving limit.
The proxy applies float32 `tanh(raw) * 127`, rounds ties to even, clamps to int8,
and returns those integers.

## Interactive and background work

`X-Embedding-Workload: interactive` is the default for user-facing queries.
Use `X-Embedding-Workload: bulk` for backfills, scheduled indexing, evaluations and
other work that can wait. Unknown values are rejected. Start bulk work with one
outstanding request and small batches; do not label bulk work interactive to gain
capacity. The convention is available to every application, not tied to a caller.

The proxy accepts at most one bulk request and 16 interactive requests concurrently.
Further requests receive 429 with `Retry-After: 1`; it has no persistent queue.
vLLM priority scheduling prefers waiting interactive inputs (priority 0) to waiting
bulk inputs (priority 1). It cannot interrupt a GPU operation already executing.
This is a scheduling preference, not a fixed latency guarantee or GPU reservation.

Limits per request:

- 1–8 nonblank text inputs.
- At most 8192 UTF-8 bytes per input and 32768 bytes across the batch.
- At most 2048 model tokens per input, enforced separately by vLLM. Bytes are not
  tokens; passing the byte check does not guarantee passing the token check.
- At most 256 KiB encoded JSON body, and a 30-second upstream deadline.

Nothing is silently truncated. Split or deliberately shorten invalid documents
in your own application. If a batch has a token-limit error, split it to identify
which input needs attention; do not endlessly repeat the same invalid batch.

## Errors, retries and disconnects

Errors use `{"error":{"message":"...","type":"...","code":"..."}}`.
Malformed JSON/options/text return 4xx; overload returns 429; upstream failure
returns 502; upstream deadline returns 504. Readiness may return 503.
Retry 429 and transient 502/504 with bounded exponential backoff and jitter, obeying
your application's deadline. Do not automatically retry invalid input. The proxy
and Caddy do not replay failed embedding POSTs for you.

Keep unfinished indexing work in your own application so it survives an outage.
If a client disconnects, already submitted GPU work may finish; do not assume that
closing the connection immediately cancels inference or frees capacity. The upstream
deadline bounds how long the proxy retains the request. There is no job lookup API.

## Raw vLLM escape hatch

`/vllm/embeddings/v1` on the same HTTPS host forwards to stock vLLM. It is useful
when an application deliberately needs native APIs or raw pooled output. Example:

```sh
curl --fail-with-body {{PUBLIC_BASE_URL}}/vllm/embeddings/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"pplx-embed-v1-0.6b","input":["database indexes"],"encoding_format":"float","priority":1}'
```

**Its vectors are raw pooled floats, not the final vectors described above.** Raw
callers own the Pplx transform and recipe tracking. They also bypass proxy batch and
bulk admission limits; use modest requests and vLLM `priority: 1` for background
work. This route shares the same GPU and can contend with everyone else. It is not
a second model instance. Never automatically fall back from the proxy to this route:
the output contracts differ.
