"""Tests for tokenizer-independent webpage chunks."""

from search_agent.web.text import (
    DEFAULT_CHUNK_TOKENS,
    extraction_token_count,
    remaining_chunk_count,
    slice_extraction_tokens,
)


def test_long_text_slices_into_pageable_chunks() -> None:
    text = "word " * (DEFAULT_CHUNK_TOKENS + 20)

    first = slice_extraction_tokens(text)
    assert first.token_count == DEFAULT_CHUNK_TOKENS
    assert first.next_offset is not None
    assert len(first.text.split()) == DEFAULT_CHUNK_TOKENS
    assert remaining_chunk_count(text, start=first.next_offset) == 1

    second = slice_extraction_tokens(text, start=first.next_offset)
    assert len(second.text.split()) == 20
    assert second.next_offset is None


def test_token_estimate_is_conservative_for_words_and_punctuation() -> None:
    assert extraction_token_count("abcd abcde !") == 4


def test_slice_makes_progress_through_oversized_word() -> None:
    text = "x" * 20
    first = slice_extraction_tokens(text, token_limit=2)

    assert first.text == "x" * 8
    assert first.next_offset == 8
    assert first.token_count == 2
