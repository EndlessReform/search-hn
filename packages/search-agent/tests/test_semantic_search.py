"""No-network session cache and pagination contract tests."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from search_agent import semantic_search
from search_agent.data_access import StorySearchHit
from search_agent.tools.fetch_stories import build_fetch_stories_payload


def hit(id):
    return StorySearchHit(id, "title", None, 25, "user", 0, date(2026, 9, 4))


def test_three_pages_are_disjoint_and_stop():
    repository = MagicMock()
    repository.search_stories.side_effect = lambda **kw: [hit(i) for i in range(1, 61)][
        kw["skip"] : kw["skip"] + kw["limit"]
    ]
    pages = [
        build_fetch_stories_payload(repository, query="test", page=p) for p in (1, 2, 3)
    ]
    assert [p["next_page"] for p in pages] == [2, 3, None]
    assert [r["id"] for p in pages for r in p["results"]] == list(range(1, 61))
    with pytest.raises(AssertionError, match="page"):
        build_fetch_stories_payload(repository, query="test", page=4)


def test_embedding_cache_is_session_owned(monkeypatch):
    monkeypatch.setattr(semantic_search, "create_db_engine", lambda _: MagicMock())
    monkeypatch.setattr(
        semantic_search.HNStorySearchRepository,
        "from_database_url",
        lambda _: MagicMock(),
    )
    client = MagicMock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[1.0] * 1536)],
        usage=SimpleNamespace(total_tokens=2),
    )
    monkeypatch.setattr(semantic_search, "OpenAI", lambda **_: client)
    a = semantic_search.SemanticStoryRepository("scratch", "live", mode="dense")
    b = semantic_search.SemanticStoryRepository("scratch", "live", mode="hybrid")
    with a.lock:
        a._embedding("same query")
        a._embedding("same query")
    assert client.embeddings.create.call_count == 1
    with b.lock:
        b._embedding("same query")
    assert client.embeddings.create.call_count == 2
    a.dispose()
    assert a.embeddings == {}
    assert "same query" in b.embeddings
    b.dispose()


def test_cached_pagination_avoids_db_and_embedding(monkeypatch):
    monkeypatch.setattr(semantic_search, "create_db_engine", lambda _: MagicMock())
    monkeypatch.setattr(
        semantic_search.HNStorySearchRepository,
        "from_database_url",
        lambda _: MagicMock(),
    )
    monkeypatch.setattr(semantic_search, "OpenAI", lambda **_: MagicMock())
    repository = semantic_search.SemanticStoryRepository(
        "scratch", "live", mode="dense"
    )
    repository.rankings[("test", 25, None, None, (), (), "relevance")] = [
        hit(i) for i in range(60)
    ]
    assert [r.id for r in repository.search_stories("test", skip=20, limit=20)] == list(
        range(20, 40)
    )
    repository._engine.connect.assert_not_called()
    repository.client.embeddings.create.assert_not_called()
    repository.dispose()


def test_injected_provider_never_constructs_openai(monkeypatch):
    monkeypatch.setattr(semantic_search, "create_db_engine", lambda _: MagicMock())
    monkeypatch.setattr(
        semantic_search.HNStorySearchRepository,
        "from_database_url",
        lambda _: MagicMock(),
    )
    openai = MagicMock(side_effect=AssertionError("Unexpected cloud embedding client"))
    monkeypatch.setattr(semantic_search, "OpenAI", openai)
    provider = MagicMock()
    provider.__bool__.return_value = False
    provider.query.return_value = [0.0, 1.0]
    repository = semantic_search.SemanticStoryRepository(
        "scratch",
        "live",
        mode="dense",
        embedding_provider=provider,
        vector_table="sovereign_vectors_qwen",
    )
    assert repository._embedding("question") == [0.0, 1.0]
    assert repository._embedding("question") == [0.0, 1.0]
    provider.query.assert_called_once_with("question")
    repository.dispose()
    provider.close.assert_called_once()
    openai.assert_not_called()
