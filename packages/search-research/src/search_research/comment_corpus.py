"""Deterministic comment pilot extraction and lossless token-bounded inputs."""

import hashlib
from html.parser import HTMLParser

import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor


class CommentText(HTMLParser):
    """Decode HN markup, retaining paragraph and code-block boundaries."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in {"p", "br", "pre"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag == "pre":
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)


def extract_comments(dsn: str, count: int, seed: str) -> pl.DataFrame:
    """Read the first three usable top-level replies in HN display order.

    Hash-order eligible stories across the entire history, then take replies in
    story-sample order. This samples stories, not independent comments, preserving
    the grouping of the intended full backfill. The database is never mutated.
    """
    assert count > 0
    sql = """
        WITH sampled AS MATERIALIZED (
            SELECT id, title, day, score, md5(id::text || %s) AS sample_key
            FROM items
            WHERE type='story' AND score>100
              AND NOT coalesce(deleted,false) AND NOT coalesce(dead,false)
            ORDER BY sample_key, id LIMIT %s
        )
        SELECT s.id AS story_id, s.title AS story_title, s.day AS story_day,
               s.score AS story_score, c.*
        FROM sampled s CROSS JOIN LATERAL (
            SELECT c.id AS comment_id, c.by AS author, c.text AS html,
                   k.display_order
            FROM kids k JOIN items c ON c.id=k.kid
            WHERE k.item=s.id AND c.parent=s.id AND c.type='comment'
              AND NOT coalesce(c.deleted,false) AND NOT coalesce(c.dead,false)
              AND nullif(btrim(c.text),'') IS NOT NULL
            ORDER BY k.display_order NULLS LAST,k.kid LIMIT 3
        ) c ORDER BY s.sample_key,s.id,c.display_order NULLS LAST,c.comment_id
        LIMIT %s
    """
    with psycopg2.connect(dsn) as connection:
        connection.set_client_encoding("UTF8")
        connection.set_session(readonly=True)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout='60s'")
            cursor.execute(sql, (seed, count, count))
            rows = [dict(row) for row in cursor.fetchall()]
    assert len(rows) == count, f"Only {len(rows)} usable comments for requested {count}"
    for row in rows:
        parser = CommentText()
        parser.feed(row["html"])
        text = "".join(parser.parts).strip()
        assert text, f"Empty decoded comment {row['comment_id']}"
        row["text"] = text
        row["text_sha256"] = hashlib.sha256(text.encode()).hexdigest()
    return pl.DataFrame(rows)


def chunk_comments(
    frame: pl.DataFrame, tokenizer, max_tokens: int = 2048
) -> pl.DataFrame:
    """Split decoded text by character offsets without dropping any content.

    Each contiguous chunk is re-tokenized with the actual model tokenizer,
    including special tokens. Binary search finds a fitting prefix when needed;
    no monotonicity assumption is needed for safety because every chosen prefix
    is checked directly. Chunk metadata allows exact reconstruction of the text.
    """
    assert max_tokens > 0
    tokenizer.no_truncation()
    tokenizer.no_padding()
    rows = []
    for comment in frame.iter_rows(named=True):
        text = comment["text"]
        offset = 0
        chunk = 0
        while offset < len(text):
            remaining = text[offset:]
            end = len(remaining)
            tokens = len(tokenizer.encode(remaining).ids)
            if tokens > max_tokens:
                low, high = 1, end
                best = 0
                while low <= high:
                    middle = (low + high) // 2
                    if len(tokenizer.encode(remaining[:middle]).ids) <= max_tokens:
                        best = middle
                        low = middle + 1
                    else:
                        high = middle - 1
                assert best > 0, "Token budget cannot accommodate one character"
                end = best
                tokens = len(tokenizer.encode(remaining[:end]).ids)
            value = remaining[:end]
            assert 0 < tokens <= max_tokens
            rows.append(
                {
                    "story_id": comment["story_id"],
                    "comment_id": comment["comment_id"],
                    "chunk": chunk,
                    "char_start": offset,
                    "char_end": offset + end,
                    "tokens": tokens,
                    "input": value,
                    "input_sha256": hashlib.sha256(value.encode()).hexdigest(),
                }
            )
            offset += end
            chunk += 1
    return pl.DataFrame(rows)
