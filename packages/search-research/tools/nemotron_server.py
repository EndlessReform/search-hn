"""Research-only BF16 Transformers server, run with UV on the inference VM.

The HTTP payload matches our bounded embedding client, but this is explicitly
not TEI. Prefixes belong to the client. Pooling follows NVIDIA's raw Transformers
example; downstream exact-cosine scoring handles normalization and MRL slicing.
One process and a lock keep requests serialized for reproducible timing.
"""

import json
import threading
import time
from typing import Literal

import torch
import transformers
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from transformers import AutoModel, AutoTokenizer

MODEL = "nvidia/Nemotron-3-Embed-1B-BF16"
REVISION = "c0c9fea93ea424587517f2c59e20db9f1d6bf615"
torch.set_num_threads(4)
assert torch.cuda.is_available() and torch.cuda.is_bf16_supported()
started = time.perf_counter()
tokenizer = AutoTokenizer.from_pretrained(MODEL, revision=REVISION, padding_side="left")
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token
model = (
    AutoModel.from_pretrained(
        MODEL, revision=REVISION, dtype=torch.bfloat16, attn_implementation="sdpa"
    )
    .to("cuda")
    .eval()
)
assert model.config.is_causal is False, "Nemotron requires bidirectional attention"
info = {
    "model_id": MODEL,
    "model_sha": REVISION,
    "model_dtype": "bfloat16",
    "model_type": {"embedding": {"pooling": "mean"}},
    "backend": "transformers-sdpa",
    "torch": torch.__version__,
    "transformers": transformers.__version__,
    "cuda": torch.version.cuda,
    "device": torch.cuda.get_device_name(),
    "max_input_length": 2048,
    "max_client_batch_size": 64,
    "dimensions": 2048,
}
print(
    json.dumps(
        {"event": "model_ready", "load_seconds": time.perf_counter() - started, **info}
    ),
    flush=True,
)
app = FastAPI()
lock = threading.Lock()


class EmbeddingRequest(BaseModel):
    inputs: list[str] = Field(min_length=1, max_length=64)
    normalize: Literal[False] = False
    truncate: Literal[False] = False


@app.get("/info")
def get_info():
    """Return stable recipe identity without load-time values that break resume."""
    return info


@app.post("/embed")
def embed(request: EmbeddingRequest, response: Response):
    """Reject overlength text; return unnormalized, mask-aware mean embeddings."""
    with lock, torch.inference_mode():
        started = time.perf_counter()
        encoded = tokenizer(
            request.inputs, padding=True, truncation=False, return_tensors="pt"
        )
        if encoded["input_ids"].shape[1] > 2048:
            raise HTTPException(
                413, "Input exceeds 2048 tokens; truncation is disabled"
            )
        encoded = {k: v.to("cuda") for k, v in encoded.items()}
        output = model(**encoded)
        masked = output.last_hidden_state.masked_fill(
            ~encoded["attention_mask"][..., None].bool(), 0
        )
        pooled = masked.sum(dim=1) / encoded["attention_mask"].sum(dim=1)[..., None]
        vectors = pooled.float().cpu()
        assert vectors.shape == (len(request.inputs), 2048)
        assert torch.isfinite(vectors).all() and torch.any(vectors != 0, dim=1).all()
        response.headers["x-total-time"] = str(
            round((time.perf_counter() - started) * 1000)
        )
        response.headers["x-compute-tokens"] = str(int(encoded["attention_mask"].sum()))
        # The output is already a validated numeric matrix. Bypass FastAPI's
        # recursive Python-object conversion, which otherwise walks every float
        # before JSON serialization and can cost more than GPU inference.
        return JSONResponse(vectors.tolist(), headers=dict(response.headers))
