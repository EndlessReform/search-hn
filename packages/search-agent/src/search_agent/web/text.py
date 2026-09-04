"""Tokenizer-independent text slicing for webpage previews and cached reads."""

from __future__ import annotations

import re
from dataclasses import dataclass
from math import ceil

DEFAULT_CHUNK_TOKENS = 768
"""Approximate extraction-token budget shared by previews and later reads."""

_TOKEN_PIECE = re.compile(r"\s+|[A-Za-z0-9_]+|[^\s]", re.UNICODE)


@dataclass(frozen=True)
class TextChunk:
    """One bounded slice and the character offset immediately after it."""

    text: str
    token_count: int
    next_offset: int | None


def extraction_token_count(text: str) -> int:
    """Estimate model-independent extraction tokens for a piece of text.

    ASCII word runs cost roughly one unit per four characters. Punctuation and
    non-ASCII characters cost one unit each. This deliberately errs toward
    shorter chunks for code, Markdown, and CJK text.
    """

    total = 0
    for match in _TOKEN_PIECE.finditer(text):
        value = match.group()
        if value.isspace():
            continue
        ascii_word = value.isascii() and value.replace("_", "a").isalnum()
        total += ceil(len(value) / 4) if ascii_word else 1
    return total


def slice_extraction_tokens(
    text: str,
    *,
    start: int = 0,
    token_limit: int = DEFAULT_CHUNK_TOKENS,
) -> TextChunk:
    """Return a bounded chunk beginning at an exact character offset."""

    assert 0 <= start <= len(text), f"start offset outside text: {start}"
    assert token_limit > 0, "token_limit must be positive"

    used = 0
    for piece in _TOKEN_PIECE.finditer(text, start):
        value = piece.group()
        if value.isspace():
            continue
        ascii_word = value.isascii() and value.replace("_", "a").isalnum()
        units = ceil(len(value) / 4) if ascii_word else 1
        if used + units > token_limit:
            end = piece.start()
            if ascii_word:
                allowed_characters = (token_limit - used) * 4
                if allowed_characters > 0:
                    end += allowed_characters
                    used = token_limit
            if end == start:
                end = min(piece.end(), start + 1)
                used = 1
            return TextChunk(
                text=text[start:end].strip(),
                token_count=used,
                next_offset=end,
            )
        used += units

    return TextChunk(
        text=text[start:].strip(),
        token_count=used,
        next_offset=None,
    )


def remaining_chunk_count(
    text: str,
    *,
    start: int | None,
    token_limit: int = DEFAULT_CHUNK_TOKENS,
) -> int:
    """Estimate how many same-sized chunks remain after ``start``."""

    if start is None or start >= len(text):
        return 0
    remaining_tokens = extraction_token_count(text[start:])
    return ceil(remaining_tokens / token_limit)
