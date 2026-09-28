"""Offline GLiNER window scheduling with stable output identity and token lengths."""

import os
from collections import Counter, defaultdict
from itertools import pairwise
from typing import NamedTuple

from search_research.comment_entities import text_windows

NER_RECIPE = {
    "backend": "eager",
    "ordering": "whole-workload encoder tokens",
    "title_schedule": [[128, 64], [384, 16], [1536, 4]],
    "person_schedule": [[128, 64], [384, 16], [1536, 8]],
}


def batch_schedule(stage, backend="eager", override=None):
    """Share the measured defaults between stage commands and the run manifest."""
    assert stage in {"title", "person"}
    assert backend in {"eager", "flash"}
    if override is not None:
        assert override > 0
        return [(1536, override)]
    if backend == "flash":
        return [(128, 64), (384, 32), (1536, 8 if stage == "title" else 16)]
    return [tuple(row) for row in NER_RECIPE[f"{stage}_schedule"]]


def configure_backend(backend):
    """Select the measured encoder implementation before constructing GLiNER."""
    assert backend in {"eager", "flash"}
    if backend == "flash":
        import flashdeberta  # Optional ner extra; fail clearly if not installed.

        assert flashdeberta is not None
        os.environ["USE_FLASHDEBERTA"] = "1"
    else:
        os.environ.pop("USE_FLASHDEBERTA", None)


class Window(NamedTuple):
    comment_id: int
    offset: int
    text: str
    tokens: int


def prepare_windows(model, rows, labels):
    """Measure actual encoder tokens, including label prompts, once per window.

    Keep the established window boundaries. Token lengths determine scheduling,
    never truncation. Output order remains comment order followed by window order.
    """
    processor = model.data_processor
    windows = []
    for cid, text in rows:
        for offset, chunk in text_windows(model, text, labels):
            words = [word[0] for word in processor.words_splitter(chunk)]
            inputs, _ = processor.prepare_inputs([words], labels)
            encoded = processor.transformer_tokenizer(
                inputs, is_split_into_words=True, truncation=False
            )
            windows.append(Window(cid, offset, chunk, len(encoded["input_ids"][0])))
    return windows


def predict_windows(
    model, windows, labels, threshold, flat_ner, schedule, on_result=None
):
    """Run fixed offline batches grouped by token length, restoring input order.

    Schedule entries are inclusive token ceilings and batch sizes. The final
    ceiling must cover every input; unexpected longer inputs fail explicitly.
    Whole-workload ordering removes padding between unrelated comment lengths.
    Restoring order before merging keeps overlap/dedup rules independent of GPU
    scheduling. Models and windows remain resident only for this stage.
    """
    assert schedule and all(limit > 0 and batch > 0 for limit, batch in schedule)
    assert all(a[0] < b[0] for a, b in pairwise(schedule))
    assert not windows or max(w.tokens for w in windows) <= schedule[-1][0], (
        "Window exceeds the measured NER schedule; expand and benchmark its last bucket"
    )
    order = sorted(range(len(windows)), key=lambda i: windows[i].tokens)
    results = [None] * len(windows)
    previous = 0
    for limit, batch in schedule:
        indices = [i for i in order if previous < windows[i].tokens <= limit]
        previous = limit
        # Bound Python preprocessing and intermediate span storage while retaining
        # global length order. Each call still uses the bucket's fixed batch size.
        for base in range(0, len(indices), 4096):
            block = indices[base : base + 4096]
            predicted = model.inference(
                [windows[i].text for i in block],
                labels,
                batch_size=batch,
                threshold=threshold,
                flat_ner=flat_ner,
            )
            for index, spans in zip(block, predicted, strict=True):
                results[index] = spans
                if on_result is not None:
                    on_result(index, spans)
    assert all(row is not None for row in results), "Incomplete NER output"
    return results


def person_records(model, rows, schedule, emit=None):
    """Keep original overlap rules; emit complete comments during GPU processing."""
    windows = prepare_windows(model, rows, ["person"])
    texts = dict(rows)
    pending = Counter(w.comment_id for w in windows)
    by_comment = defaultdict(list)
    records = {}

    def finish(cid):
        merged = {}
        for index, spans in sorted(by_comment.pop(cid, [])):
            window = windows[index]
            for span in spans:
                start = window.offset + span["start"]
                end = window.offset + span["end"]
                assert texts[cid][start:end] == span["text"]
                merged[start, end] = {
                    "text": span["text"],
                    "start": start,
                    "end": end,
                    "score": float(span["score"]),
                }
        records[cid] = list(merged.values())
        if emit is not None:
            emit(cid, records[cid])

    def received(index, spans):
        cid = windows[index].comment_id
        by_comment[cid].append((index, spans))
        pending[cid] -= 1
        if not pending[cid]:
            finish(cid)

    for cid, _ in rows:
        if not pending[cid]:
            finish(cid)
    predict_windows(model, windows, ["person"], 0.3, True, schedule, received)
    return [(cid, records[cid]) for cid, _ in rows]
