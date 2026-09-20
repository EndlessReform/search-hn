"""Resolve markup-only replies without silently changing top-k selection.

SQL removes blank HTML; decoding can still reveal an empty body such as '<i>'.
Record these exclusions and, for top comments, continue in the same display order
until k usable replies have been found or that story has no more replies.
"""

import json
from itertools import groupby

from psycopg2.extras import RealDictCursor

from search_research.comment_corpus import CommentText


def decode_text(html):
    parser = CommentText()
    parser.feed(html)
    return "".join(parser.parts).strip()


def keep_text(index, row):
    """Leave a queryable record for every HTML body rejected after decoding."""
    if decode_text(row["html"]):
        return True
    index.execute(
        "INSERT INTO exclusions VALUES (?,'empty_decoded_text',?)",
        (
            row["comment_id"],
            json.dumps(row, sort_keys=True),
        ),
    )
    print(
        json.dumps(
            {
                "event": "comment_excluded",
                "comment_id": row["comment_id"],
                "reason": "empty_decoded_text",
            }
        ),
        flush=True,
    )
    return False


def replacement_pages(connection, story_id, offset):
    """Read additional ordered children only for stories needing replacements."""
    sql = """
        SELECT payload.*,k.display_order FROM kids k CROSS JOIN LATERAL (
            SELECT c.id AS comment_id,c.by AS author,c.text AS html,
                   c.day::text AS comment_day,c.parent
            FROM items c WHERE c.id=k.kid AND c.parent=%s AND c.type='comment'
              AND NOT coalesce(c.deleted,false) AND NOT coalesce(c.dead,false)
              AND nullif(btrim(c.text),'') IS NOT NULL OFFSET 0
        ) payload WHERE k.item=%s
        ORDER BY k.display_order NULLS LAST,k.kid LIMIT 32 OFFSET %s
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        while True:
            cursor.execute(sql, (story_id, story_id, offset))
            page = cursor.fetchall()
            if not page:
                return
            yield from (dict(row) for row in page)
            offset += len(page)


def usable_rows(cursor, connection, selection, index):
    """Preserve complete story groups across server-cursor fetch boundaries."""
    top = selection.kind == "top-comments"
    for _, grouped in groupby(
        cursor, key=lambda row: row["story_id"] if top else row["comment_id"]
    ):
        original = [dict(row) for row in grouped]
        usable = [row for row in original if keep_text(index, row)]
        if top and len(usable) < len(original):
            first = original[0]
            story = {
                key: first[key]
                for key in ("story_id", "story_title", "story_score", "story_day")
            }
            replacements = replacement_pages(
                connection, first["story_id"], selection.top_k
            )
            try:
                for reply in replacements:
                    candidate = {**story, **reply}
                    if keep_text(index, candidate):
                        usable.append(candidate)
                    if len(usable) == selection.top_k:
                        break
            finally:
                replacements.close()
        if top:
            usable.sort(
                key=lambda row: (
                    row["display_order"] is None,
                    row["display_order"],
                    row["comment_id"],
                )
            )
        yield from usable
