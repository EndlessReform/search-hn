"""Production contract tests; SQL/ANN behavior is checked by the live read-only harness."""

from datetime import date
from unittest.mock import MagicMock
from urllib.error import URLError

import pytest
from pydantic import ValidationError

from search_agent import production_embeddings as embeddings
from search_agent import production_search as search
from search_agent.data_access import StorySearchHit
from search_agent.tools.fetch_stories import build_fetch_stories_payload


def hit(story_id):
    return StorySearchHit(story_id, "Example", None, 25, "author", 1, date(2026, 9, 7))


@pytest.fixture
def repository(monkeypatch):
    monkeypatch.setattr(search, "create_db_engine", lambda _: MagicMock())
    return search.ProductionStoryRepository("unused", "http://proxy/v1")


def test_weighted_rrf_and_missing_branches():
    ranks = search.fuse([10, 20], [20, 30])
    assert ranks[10] == {"dense_rank": 1, "bm25_rank": None, "rrf": 1 / 61}
    assert ranks[20]["rrf"] == pytest.approx(1 / 62 + 0.125 / 61)
    assert ranks[30]["rrf"] == pytest.approx(0.125 / 62)
    assert sorted(ranks, key=lambda i: -ranks[i]["rrf"]) == [20, 10, 30]


@pytest.mark.parametrize(
    "vector",
    [[0] * 1024, [1] * 1023, [0.5] * 1024, [128] * 1024, [float("nan")] * 1024],
)
def test_invalid_vectors_fail(vector):
    with pytest.raises(ValidationError):
        embeddings.EmbeddingEntry(index=0, embedding=vector)


def test_native_coordinates_are_preserved():
    values = [-128, 127] * 512
    assert embeddings.EmbeddingEntry(index=0, embedding=values).embedding == values


@pytest.mark.parametrize("field", ["header", "body", "model", "index"])
def test_embedding_contract_rejects_mismatch(monkeypatch, field):
    response = MagicMock()
    response.__enter__.return_value = response
    response.headers = {
        "X-Embedding-Recipe": "wrong" if field == "header" else "recipe"
    }
    import json

    response.read.return_value = json.dumps(
        {
            "model": "wrong" if field == "model" else embeddings.MODEL,
            "embedding_recipe": "wrong" if field == "body" else "recipe",
            "data": [{"index": 1 if field == "index" else 0, "embedding": [1] * 1024}],
        }
    ).encode()
    monkeypatch.setattr(embeddings, "urlopen", lambda *a, **k: response)
    with pytest.raises(ValueError):
        embeddings.embed_query("http://proxy/v1", "topic", "recipe")


def test_pages_refresh_details_without_retrieval_and_preserve_deleted_slots(
    repository, monkeypatch
):
    ids = list(range(1, 61))
    rank = MagicMock(
        return_value=search.Ranking(
            search.monotonic(), ids, "hybrid", search.fuse(ids, [])
        )
    )
    monkeypatch.setattr(repository, "_rank", rank)
    # First position was deleted after retrieval; the lookahead must stay on page 2.
    monkeypatch.setattr(
        repository, "_details", lambda page: [hit(i) for i in page if i != 1]
    )
    pages = [
        build_fetch_stories_payload(repository, query="topic", page=p)
        for p in (1, 2, 3)
    ]
    assert [p["next_page"] for p in pages] == [2, 3, None]
    assert [[r["id"] for r in p["results"]] for p in pages] == [
        list(range(2, 21)),
        list(range(21, 41)),
        list(range(41, 61)),
    ]
    assert pages[0]["retrieval_mode"] == "hybrid"
    assert pages[0]["results"][0]["dense_rank"] == 2
    rank.assert_called_once()
    repository.reset_session()
    with pytest.raises(ValueError, match="page 1"):
        repository.search_stories("topic", skip=20)


def test_expired_ranking_requires_restart(repository, monkeypatch):
    monkeypatch.setattr(
        repository, "_rank", lambda key: search.Ranking(0, [1], "hybrid", {})
    )
    monkeypatch.setattr(repository, "_details", lambda ids: [hit(i) for i in ids])
    monkeypatch.setattr(search, "monotonic", lambda: 1000)
    repository.search_stories("topic")
    with pytest.raises(ValueError, match="expired"):
        repository.search_stories("topic", skip=20)


def test_inference_failure_sticks_to_keyword_only_for_pages(repository, monkeypatch):
    conn = repository._engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.scalar_one.return_value = "recipe"
    conn.execute.return_value.scalars.return_value = list(range(1, 61))
    embed = MagicMock(side_effect=URLError("offline"))
    monkeypatch.setattr(search, "embed_query", embed)
    monkeypatch.setattr(repository, "_details", lambda ids: [hit(i) for i in ids])
    first = repository.search_stories("topic")
    second = repository.search_stories("topic", skip=20)
    assert first.mode == second.mode == "keyword-only"
    assert first.ranks[1]["dense_rank"] is None
    embed.assert_called_once()
    assert not any(
        "CAST(:vector" in str(call.args[0]) for call in conn.execute.call_args_list
    )


def test_filter_only_never_calls_inference(repository, monkeypatch):
    embed = MagicMock(side_effect=AssertionError("unexpected embedding"))
    monkeypatch.setattr(search, "embed_query", embed)
    monkeypatch.setattr(repository, "_details", lambda ids: [])
    assert repository.search_stories(None, min_date=date(2026, 9, 7)).mode == "browse"
    embed.assert_not_called()


def test_configuration_defaults_to_production_and_supports_fts(monkeypatch):
    from search_agent import runtime_context

    monkeypatch.setattr(runtime_context, "resolve_database_url", lambda _: "unused")
    monkeypatch.delenv("SEARCH_RETRIEVAL", raising=False)
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://proxy/v1")
    monkeypatch.setattr(search, "create_db_engine", lambda _: MagicMock())
    assert isinstance(
        runtime_context.build_search_agent_context().repository,
        search.ProductionStoryRepository,
    )
    legacy = MagicMock()
    monkeypatch.setattr(
        runtime_context.HNStorySearchRepository, "from_database_url", lambda _: legacy
    )
    assert (
        runtime_context.build_search_agent_context(retrieval="fts").repository is legacy
    )


def test_textual_runtime_uses_production_and_hybrid_guidance(monkeypatch):
    import asyncio
    from agents import RunContextWrapper
    from search_agent.agent_config import _agent_instructions
    from search_agent.runtime import SearchRuntime
    from search_agent import runtime_context

    monkeypatch.setattr(runtime_context, "resolve_database_url", lambda _: "unused")
    monkeypatch.setattr(search, "create_db_engine", lambda _: MagicMock())
    monkeypatch.delenv("SEARCH_RETRIEVAL", raising=False)
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://proxy/v1")
    runtime = SearchRuntime(model="local-model", base_url="http://localhost/v1")
    try:
        assert isinstance(runtime.context.repository, search.ProductionStoryRepository)
        assert [tool.name for tool in runtime.agent.tools] == [
            "fetch_stories",
            "fetch_top_stories_for_date",
            "fetch_top_comments",
        ]
        instructions = _agent_instructions(
            RunContextWrapper(context=runtime.context), runtime.agent
        )
        assert "semantic title/URL embeddings with title BM25" in instructions
        assert "at least 25 HN points" in instructions
        assert "not broad semantic retrieval" not in instructions
    finally:
        asyncio.run(runtime.close())
