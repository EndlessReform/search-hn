"""A frozen rewrite must be byte-identical when resumed from its durable journal."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from search_research.rewrite import rewrite


def test_resume_keeps_frozen_questions(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "eval.jsonl").write_text(json.dumps({"id": 42}) + "\n")
    review = {
        "id": 42,
        "eligible": True,
        "reason": "Plain wording",
        "entity_question": "What tool came up in the OpenLogi thread?",
        "paraphrase_question": "What Linux tool can replace Logitech software?",
        "evidence": "Source evidence",
    }
    client = AsyncMock()
    client.responses.create.return_value = SimpleNamespace(
        output_text=json.dumps({"reviews": [review]}), id="response-test"
    )
    client.__aenter__.return_value = client
    with patch("search_research.rewrite.AsyncOpenAI", return_value=client):
        asyncio.run(rewrite(target, source, "test", "http://localhost"))
        frozen = (target / "eval.jsonl").read_bytes()
        asyncio.run(rewrite(target, source, "test", "http://localhost"))
    assert client.responses.create.await_count == 1
    assert (target / "eval.jsonl").read_bytes() == frozen
