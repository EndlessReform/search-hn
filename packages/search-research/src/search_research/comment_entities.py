"""On-demand GLiNER extraction; the frozen corpus and predictions stay read-only."""

from contextlib import closing
from importlib.util import find_spec
from threading import Lock
from time import perf_counter

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

MODEL_ID = "gliner-community/gliner_large-v2.5"


class EntityRequest(BaseModel):
    """An ontology snapshot, independent of positive/negative annotation sets."""

    model_config = ConfigDict(extra="forbid")
    labels: list[str] = Field(min_length=1)
    thresholds: dict[str, float]

    @field_validator("labels")
    @classmethod
    def clean_labels(cls, labels):
        labels = [label.strip() for label in labels]
        if any(not label for label in labels) or len(set(labels)) != len(labels):
            raise ValueError("Labels must be nonempty and unique")
        return labels

    @field_validator("thresholds")
    @classmethod
    def valid_thresholds(cls, thresholds):
        if any(not 0 <= value <= 1 for value in thresholds.values()):
            raise ValueError("Thresholds must be between 0 and 1")
        return thresholds


def text_windows(model, text, labels):
    """Fit overlapping word windows to both model and tokenizer limits.

    Include the label prompt in the token budget. Overlap by the model's maximum
    span width so boundary entities can be found in the next window. Offsets
    always refer to the original Python string, including non-BMP characters.
    """
    processor = model.data_processor
    words = list(processor.words_splitter(text))
    tokenizer = processor.transformer_tokenizer
    start = 0
    while start < len(words):
        end = min(start + model.config.max_len, len(words))
        while end > start:
            inputs, _ = processor.prepare_inputs(
                [[word[0] for word in words[start:end]]], labels
            )
            encoded = tokenizer(inputs, is_split_into_words=True, truncation=False)
            if len(encoded["input_ids"][0]) <= tokenizer.model_max_length:
                break
            end = start + (end - start) // 2
        if end == start:
            raise ValueError(
                "The label prompt and a single word exceed the model context"
            )
        left, right = words[start][1], words[end - 1][2]
        yield left, text[left:right]
        if end == len(words):
            break
        start = max(start + 1, end - model.config.max_width)


class EntityExtractor:
    """Load once on first use and serialize inference on the shared accelerator."""

    def __init__(self, device="auto"):
        self.device = device
        self.model = None
        self.lock = Lock()

    def predict(self, text, request):
        if set(request.thresholds) != set(request.labels):
            raise ValueError("Provide exactly one threshold for every label")
        if find_spec("gliner") is None:
            raise HTTPException(
                503, "Start the explorer with uv --extra ner to enable GLiNER"
            )
        import torch
        from gliner import GLiNER

        started = perf_counter()
        with self.lock, torch.inference_mode():
            if self.model is None:
                if self.device == "auto":
                    self.device = "cuda" if torch.cuda.is_available() else "cpu"
                dtype = (
                    torch.bfloat16
                    if str(self.device).startswith("cuda")
                    else torch.float32
                )
                self.model = GLiNER.from_pretrained(MODEL_ID, load_tokenizer=True).to(
                    device=self.device, dtype=dtype
                )
                self.model.eval()
            spans = {}
            windows = 0
            for offset, chunk in text_windows(self.model, text, request.labels):
                windows += 1
                for span in self.model.predict_entities(
                    chunk,
                    request.labels,
                    threshold=min(request.thresholds.values()),
                    flat_ner=False,
                    multi_label=True,
                ):
                    if span["score"] < request.thresholds[span["label"]]:
                        continue
                    left, right = offset + span["start"], offset + span["end"]
                    assert 0 <= left < right <= len(text), "Invalid model span offsets"
                    assert text[left:right] == span["text"], (
                        "Model span text differs from corpus"
                    )
                    key = (left, right, span["label"])
                    result = {
                        "start": left,
                        "end": right,
                        "text": span["text"],
                        "label": span["label"],
                        "score": float(span["score"]),
                    }
                    if key not in spans or result["score"] > spans[key]["score"]:
                        spans[key] = result
            return {
                "model": MODEL_ID,
                "device": self.device,
                "dtype": str(next(self.model.parameters()).dtype).removeprefix(
                    "torch."
                ),
                "text": text,
                "seconds": perf_counter() - started,
                "windows": windows,
                "spans": sorted(
                    spans.values(), key=lambda s: (s["start"], s["end"], s["label"])
                ),
            }


def install_entities(app, explorer, extractor=None):
    """Look up one complete frozen comment and extract using submitted labels."""
    extractor = extractor or EntityExtractor()

    @app.post("/api/comments/{comment_id}/entities")
    def entities(comment_id: int, request: EntityRequest):
        with closing(explorer.connect()) as db:
            row = db.execute(
                "SELECT text FROM comments WHERE comment_id=?", (comment_id,)
            ).fetchone()
        if row is None:
            raise HTTPException(404, "Comment is absent from this corpus")
        return extractor.predict(row[0], request)
