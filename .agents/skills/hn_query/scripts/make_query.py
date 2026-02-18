# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "psycopg2-binary>=2.9.11",
#     "python-dotenv>=1.2.1",
#     "sqlalchemy>=2.0.46",
# ]
# ///
"""Minimal CLI for full-text searching HN stories from the mirrored Postgres DB.

Results are emitted as JSON Lines (JSONL) to stdout: one JSON object per row.
Operational logs and errors are emitted to stderr via the `logging` module.
"""

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ENV_DATABASE_URL: Final[str] = "HN_QUERY_DATABASE_URL"
DEFAULT_LIMIT: Final[int] = 20
MAX_LIMIT: Final[int] = 100
MAX_SKIP: Final[int] = 1000
DEFAULT_COMMENT_LIMIT: Final[int] = 3


@dataclass(frozen=True)
class StoryResult:
    """A single row returned from the story search query.

    The shape intentionally mirrors the user-facing output fields:
    title, URL, date, score, and optionally body text.
    """

    id: int
    title: str
    url: str | None
    day: date | None
    score: int | None
    text: str | None


@dataclass(frozen=True)
class CommentResult:
    """A single top-level comment row for a story."""

    author: str | None
    comment: str | None


def positive_int(value: str) -> int:
    """Argparse type that accepts only positive integers."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def normalize_domain(value: str) -> str:
    """Normalize a domain-like input to lowercase host without leading `www.`."""
    raw = value.strip().lower()
    assert raw, "domain must be non-empty"
    if "://" in raw:
        host = urlparse(raw).hostname
    else:
        host = urlparse(f"http://{raw}").hostname
    assert host, f"invalid domain value: {value!r}"
    normalized = host.rstrip(".")
    if normalized.startswith("www."):
        normalized = normalized[4:]
    assert normalized, f"invalid domain value after normalization: {value!r}"
    return normalized


def normalized_domain_list(values: list[str] | None) -> list[str] | None:
    """Normalize and de-duplicate optional domain lists."""
    if not values:
        return None
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_domain(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered_unique.append(normalized)
    return ordered_unique


def create_db_engine(database_url: str):
    """Create a SQLAlchemy engine with UTF-8 text decoding."""
    return create_engine(
        database_url,
        future=True,
        connect_args={"client_encoding": "utf8"},
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for `search`, `count`, `top-domain`, and `comments`.

    Notes:
    - Bare invocations like `make_query.py "openai"` are treated as
      `make_query.py search "openai"` for backward compatibility.
    - `search` returns submission rows in JSONL.
    - `count` returns a JSON object with `count` for the provided query/filter.
    - `top-domain` returns story rows for a single domain without query terms.
    - `comments` returns top-level comments for a validated post ID in JSONL.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Query mirrored HN data from PostgreSQL. "
            f"Requires {ENV_DATABASE_URL} in your environment or .env file. "
            "Prints JSONL to stdout."
        )
    )
    subparsers = parser.add_subparsers(dest="command")

    search_parser = subparsers.add_parser(
        "search",
        help="Search submissions (stories) with full-text query over title+url.",
    )
    search_parser.add_argument(
        "query",
        help=(
            "Keyword query string sent to plainto_tsquery('simple', ...). "
            "Example: 'gpt-4.5 vector database'."
        ),
    )
    search_parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=(
            f"Maximum results to return (default: {DEFAULT_LIMIT}, max: {MAX_LIMIT}). "
            "Values above max are clamped."
        ),
    )
    search_parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help=(
            f"Number of matched rows to offset/paginate (default: 0, max: {MAX_SKIP}). "
            "Values above max are clamped."
        ),
    )
    search_parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        help=(
            "Only return submissions with score >= this value. "
            "If omitted, no minimum-score filter is applied."
        ),
    )
    search_parser.add_argument(
        "--min-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only return submissions on/after this calendar date "
            "(ISO format: YYYY-MM-DD)."
        ),
    )
    search_parser.add_argument(
        "--max-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only return submissions on/before this calendar date "
            "(ISO format: YYYY-MM-DD)."
        ),
    )
    search_parser.add_argument(
        "--sort",
        choices=("score", "rank", "date"),
        default="score",
        help=(
            "Sort mode: 'score' (default, descending score), "
            "'rank' (descending full-text relevance), or "
            "'date' (descending story date)."
        ),
    )
    search_parser.add_argument(
        "--domain",
        action="append",
        default=None,
        help=(
            "Include only this domain (repeatable). Robust to leading 'www.' and "
            "accepts plain domains or URLs."
        ),
    )
    search_parser.add_argument(
        "--exclude-domain",
        action="append",
        default=None,
        help=(
            "Exclude this domain (repeatable). Robust to leading 'www.' and "
            "accepts plain domains or URLs."
        ),
    )

    count_parser = subparsers.add_parser(
        "count",
        help="Count matching submissions for a query/filter set.",
    )
    count_parser.add_argument(
        "query",
        help="Keyword query string sent to plainto_tsquery('simple', ...).",
    )
    count_parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        help=(
            "Only count submissions with score >= this value. "
            "If omitted, no minimum-score filter is applied."
        ),
    )
    count_parser.add_argument(
        "--min-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only count submissions on/after this calendar date.",
    )
    count_parser.add_argument(
        "--max-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only count submissions on/before this calendar date.",
    )
    count_parser.add_argument(
        "--domain",
        action="append",
        default=None,
        help=(
            "Include only this domain (repeatable). Robust to leading 'www.' and "
            "accepts plain domains or URLs."
        ),
    )
    count_parser.add_argument(
        "--exclude-domain",
        action="append",
        default=None,
        help=(
            "Exclude this domain (repeatable). Robust to leading 'www.' and "
            "accepts plain domains or URLs."
        ),
    )

    top_domain_parser = subparsers.add_parser(
        "top-domain",
        help="Show top submissions from one domain without a text query.",
    )
    top_domain_parser.add_argument(
        "domain",
        help="Domain to include (accepts plain domain or URL; strips leading www.).",
    )
    top_domain_parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=(
            f"Maximum results to return (default: {DEFAULT_LIMIT}, max: {MAX_LIMIT}). "
            "Values above max are clamped."
        ),
    )
    top_domain_parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help=(
            f"Number of matched rows to offset/paginate (default: 0, max: {MAX_SKIP}). "
            "Values above max are clamped."
        ),
    )
    top_domain_parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        help=(
            "Only return submissions with score >= this value. "
            "If omitted, no minimum-score filter is applied."
        ),
    )
    top_domain_parser.add_argument(
        "--min-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only return submissions on/after this calendar date.",
    )
    top_domain_parser.add_argument(
        "--max-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only return submissions on/before this calendar date.",
    )
    top_domain_parser.add_argument(
        "--sort",
        choices=("score", "date"),
        default="score",
        help="Sort mode: 'score' (default) or 'date' (descending).",
    )

    comments_parser = subparsers.add_parser(
        "comments",
        help="Show top-level comments for a story ID, ordered by display_order.",
    )
    comments_parser.add_argument(
        "post_id",
        type=positive_int,
        help="Story ID to inspect. Must be a positive integer ID in items.",
    )
    comments_parser.add_argument(
        "--comments",
        type=int,
        default=DEFAULT_COMMENT_LIMIT,
        help=(
            f"Number of top-level comments to return "
            f"(default: {DEFAULT_COMMENT_LIMIT}, max: {MAX_LIMIT}). "
            "Values above max are clamped."
        ),
    )
    comments_parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help=(
            f"Number of top-level comments to skip (default: 0, max: {MAX_SKIP}). "
            "Values above max are clamped."
        ),
    )

    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] not in {"search", "count", "top-domain", "comments", "-h", "--help"}:
        argv = ["search", *argv]

    args = parser.parse_args(argv)
    if args.command is None:
        parser.error("Provide a subcommand: 'search' or 'comments'.")
    return args


def clamp(value: int, low: int, high: int) -> int:
    """Clamp an integer to an inclusive range."""
    return max(low, min(value, high))


def validate_date_range(min_date: date | None, max_date: date | None) -> None:
    """Assert that optional date bounds are well-ordered."""
    if min_date and max_date:
        assert min_date <= max_date, (
            "--min-date must be <= --max-date "
            f"(got {min_date.isoformat()} > {max_date.isoformat()})"
        )


def get_database_url() -> str:
    """Load `.env` and return the namespaced PostgreSQL connection string.

    This script intentionally uses a dedicated variable name so it does not
    interfere with other tooling that might rely on DATABASE_URL.
    """
    load_dotenv()
    database_url = os.getenv(ENV_DATABASE_URL)
    assert database_url, (
        f"{ENV_DATABASE_URL} is not set. Add it to your shell env or .env file, "
        "for example: HN_QUERY_DATABASE_URL=postgresql://user@host:5432/hn_database"
    )
    return database_url


def run_search(
    database_url: str,
    query_string: str,
    limit: int,
    skip: int,
    sort: str,
    min_score: int | None,
    min_date: date | None,
    max_date: date | None,
    include_domains: list[str] | None,
    exclude_domains: list[str] | None,
) -> list[StoryResult]:
    """Execute the full-text story search with safe SQL parameters.

    We always compute rank in SQL so `--sort rank` is available, but we never
    print rank in output to keep the interface intentionally minimal.
    """
    order_by = "i.score DESC NULLS LAST, rank DESC"
    if sort == "rank":
        order_by = "rank DESC, i.score DESC NULLS LAST"
    elif sort == "date":
        order_by = "i.day DESC NULLS LAST, i.score DESC NULLS LAST, rank DESC"

    stmt = text(
        f"""
        SELECT
            i.id,
            i.title,
            i.url,
            i.day,
            i.score,
            i.text,
            ts_rank_cd(i.search_tsv, plainto_tsquery('simple', :query)) AS rank
        FROM items AS i
        WHERE i.type = 'story'
          AND i.search_tsv @@ plainto_tsquery('simple', :query)
          AND (:min_score IS NULL OR i.score >= :min_score)
          AND (:min_date IS NULL OR i.day >= :min_date)
          AND (:max_date IS NULL OR i.day <= :max_date)
          AND (
            :include_domains IS NULL OR
            regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = ANY(CAST(:include_domains AS text[]))
          )
          AND (
            :exclude_domains IS NULL OR
            NOT (
                regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = ANY(CAST(:exclude_domains AS text[]))
            )
          )
        ORDER BY {order_by}
        LIMIT :limit
        OFFSET :skip
        """
    )

    engine = create_db_engine(database_url)
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "query": query_string,
                "limit": limit,
                "skip": skip,
                "min_score": min_score,
                "min_date": min_date,
                "max_date": max_date,
                "include_domains": include_domains,
                "exclude_domains": exclude_domains,
            },
        ).all()

    return [
        StoryResult(
            id=row.id,
            title=row.title or "(untitled)",
            url=row.url,
            day=row.day,
            score=row.score,
            text=row.text,
        )
        for row in rows
    ]


def run_count_search(
    database_url: str,
    query_string: str,
    min_score: int | None,
    min_date: date | None,
    max_date: date | None,
    include_domains: list[str] | None,
    exclude_domains: list[str] | None,
) -> int:
    """Count stories matching a full-text query and filter set."""
    stmt = text(
        """
        SELECT COUNT(*) AS total
        FROM items AS i
        WHERE i.type = 'story'
          AND i.search_tsv @@ plainto_tsquery('simple', :query)
          AND (:min_score IS NULL OR i.score >= :min_score)
          AND (:min_date IS NULL OR i.day >= :min_date)
          AND (:max_date IS NULL OR i.day <= :max_date)
          AND (
            :include_domains IS NULL OR
            regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = ANY(CAST(:include_domains AS text[]))
          )
          AND (
            :exclude_domains IS NULL OR
            NOT (
                regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = ANY(CAST(:exclude_domains AS text[]))
            )
          )
        """
    )
    engine = create_db_engine(database_url)
    with engine.connect() as conn:
        total = conn.execute(
            stmt,
            {
                "query": query_string,
                "min_score": min_score,
                "min_date": min_date,
                "max_date": max_date,
                "include_domains": include_domains,
                "exclude_domains": exclude_domains,
            },
        ).scalar_one()
    return int(total)


def run_top_domain(
    database_url: str,
    domain: str,
    limit: int,
    skip: int,
    sort: str,
    min_score: int | None,
    min_date: date | None,
    max_date: date | None,
) -> list[StoryResult]:
    """Fetch top stories for a single normalized domain without a text query."""
    order_by = "i.score DESC NULLS LAST, i.day DESC NULLS LAST, i.id DESC"
    if sort == "date":
        order_by = "i.day DESC NULLS LAST, i.score DESC NULLS LAST, i.id DESC"

    stmt = text(
        f"""
        SELECT
            i.id,
            i.title,
            i.url,
            i.day,
            i.score,
            i.text
        FROM items AS i
        WHERE i.type = 'story'
          AND regexp_replace(lower(coalesce(i.domain, '')), '^www\\.', '') = :domain
          AND (:min_score IS NULL OR i.score >= :min_score)
          AND (:min_date IS NULL OR i.day >= :min_date)
          AND (:max_date IS NULL OR i.day <= :max_date)
        ORDER BY {order_by}
        LIMIT :limit
        OFFSET :skip
        """
    )
    engine = create_db_engine(database_url)
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "domain": domain,
                "limit": limit,
                "skip": skip,
                "min_score": min_score,
                "min_date": min_date,
                "max_date": max_date,
            },
        ).all()

    return [
        StoryResult(
            id=row.id,
            title=row.title or "(untitled)",
            url=row.url,
            day=row.day,
            score=row.score,
            text=row.text,
        )
        for row in rows
    ]


def validate_story_id(database_url: str, post_id: int) -> None:
    """Ensure the provided ID exists and refers to a story row."""
    stmt = text(
        """
        SELECT id, type
        FROM items
        WHERE id = :post_id
        """
    )
    engine = create_db_engine(database_url)
    with engine.connect() as conn:
        row = conn.execute(stmt, {"post_id": post_id}).one_or_none()
    if row is None:
        logging.info("Post ID %d does not exist.", post_id)
        raise SystemExit(0)
    assert row.type == "story", (
        f"ID {post_id} exists but is not a story (type={row.type!r}). "
        "Use a story submission ID."
    )


def run_top_level_comments(
    database_url: str,
    post_id: int,
    limit: int,
    skip: int,
) -> tuple[int, list[CommentResult]]:
    """Fetch top-level comments for a story ID in display_order sequence."""
    count_stmt = text(
        """
        SELECT COUNT(*) AS total
        FROM kids AS k
        JOIN items AS c ON c.id = k.kid
        WHERE k.item = :post_id
          AND c.type = 'comment'
        """
    )
    fetch_stmt = text(
        """
        SELECT
            c.by AS author,
            c.text AS comment
        FROM kids AS k
        JOIN items AS c ON c.id = k.kid
        WHERE k.item = :post_id
          AND c.type = 'comment'
        ORDER BY k.display_order ASC NULLS LAST, k.kid ASC
        LIMIT :limit
        OFFSET :skip
        """
    )
    engine = create_db_engine(database_url)
    with engine.connect() as conn:
        total = int(conn.execute(count_stmt, {"post_id": post_id}).scalar_one())
        if total == 0 or skip >= total:
            return total, []
        rows = conn.execute(
            fetch_stmt,
            {"post_id": post_id, "limit": limit, "skip": skip},
        ).all()
    return total, [
        CommentResult(author=row.author, comment=row.comment) for row in rows
    ]


def render_results_jsonl(results: Sequence[StoryResult]) -> str:
    """Render search results as JSONL for stdout.

    Field policy:
    - Always include: title, url, date, score.
    - Include `text` only when non-null and non-empty.
    """
    blocks: list[str] = []
    for row in results:
        payload: dict[str, str | int | None] = {
            "id": row.id,
            "title": row.title,
            "url": row.url,
            "date": row.day.isoformat() if row.day is not None else None,
            "score": row.score,
        }
        if row.text and row.text.strip():
            payload["text"] = row.text.strip()
        blocks.append(json.dumps(payload, ensure_ascii=False))
    return "\n".join(blocks)


def render_comments_jsonl(results: Sequence[CommentResult]) -> str:
    """Render comment rows as JSONL for stdout.

    Field policy:
    - Include `author` and `comment`.
    """
    blocks: list[str] = []
    for row in results:
        payload: dict[str, str | None] = {
            "author": row.author,
            "comment": row.comment,
        }
        blocks.append(json.dumps(payload, ensure_ascii=False))
    return "\n".join(blocks)


def main() -> None:
    """CLI entrypoint: parse args, query database, print JSONL output."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        args = parse_args()
        database_url = get_database_url()
        if args.command == "search":
            limit = clamp(args.limit, 1, MAX_LIMIT)
            skip = clamp(args.skip, 0, MAX_SKIP)
            validate_date_range(args.min_date, args.max_date)
            include_domains = normalized_domain_list(args.domain)
            exclude_domains = normalized_domain_list(args.exclude_domain)
            results = run_search(
                database_url=database_url,
                query_string=args.query,
                limit=limit,
                skip=skip,
                sort=args.sort,
                min_score=args.min_score,
                min_date=args.min_date,
                max_date=args.max_date,
                include_domains=include_domains,
                exclude_domains=exclude_domains,
            )
            if not results:
                logging.info(
                    (
                        "No results found for query=%r "
                        "(limit=%d, skip=%d, sort=%s, min_score=%r, min_date=%r, "
                        "max_date=%r, include_domains=%r, exclude_domains=%r)"
                    ),
                    args.query,
                    limit,
                    skip,
                    args.sort,
                    args.min_score,
                    args.min_date.isoformat() if args.min_date else None,
                    args.max_date.isoformat() if args.max_date else None,
                    include_domains,
                    exclude_domains,
                )
                return
            print(render_results_jsonl(results))
            return

        if args.command == "count":
            validate_date_range(args.min_date, args.max_date)
            include_domains = normalized_domain_list(args.domain)
            exclude_domains = normalized_domain_list(args.exclude_domain)
            total = run_count_search(
                database_url=database_url,
                query_string=args.query,
                min_score=args.min_score,
                min_date=args.min_date,
                max_date=args.max_date,
                include_domains=include_domains,
                exclude_domains=exclude_domains,
            )
            print(json.dumps({"count": total}, ensure_ascii=False))
            if total == 0:
                logging.info(
                    (
                        "No count matches for query=%r "
                        "(min_score=%r, min_date=%r, max_date=%r, include_domains=%r, exclude_domains=%r)"
                    ),
                    args.query,
                    args.min_score,
                    args.min_date.isoformat() if args.min_date else None,
                    args.max_date.isoformat() if args.max_date else None,
                    include_domains,
                    exclude_domains,
                )
            return

        if args.command == "top-domain":
            limit = clamp(args.limit, 1, MAX_LIMIT)
            skip = clamp(args.skip, 0, MAX_SKIP)
            validate_date_range(args.min_date, args.max_date)
            normalized_domain = normalize_domain(args.domain)
            results = run_top_domain(
                database_url=database_url,
                domain=normalized_domain,
                limit=limit,
                skip=skip,
                sort=args.sort,
                min_score=args.min_score,
                min_date=args.min_date,
                max_date=args.max_date,
            )
            if not results:
                logging.info(
                    (
                        "No results found for domain=%r "
                        "(limit=%d, skip=%d, sort=%s, min_score=%r, min_date=%r, max_date=%r)"
                    ),
                    normalized_domain,
                    limit,
                    skip,
                    args.sort,
                    args.min_score,
                    args.min_date.isoformat() if args.min_date else None,
                    args.max_date.isoformat() if args.max_date else None,
                )
                return
            print(render_results_jsonl(results))
            return

        assert args.command == "comments", f"unsupported command: {args.command!r}"
        comment_limit = clamp(args.comments, 1, MAX_LIMIT)
        skip = clamp(args.skip, 0, MAX_SKIP)
        validate_story_id(database_url=database_url, post_id=args.post_id)
        total, comments = run_top_level_comments(
            database_url=database_url,
            post_id=args.post_id,
            limit=comment_limit,
            skip=skip,
        )
        if total == 0:
            logging.info("Post ID %d has no top-level comments.", args.post_id)
            logging.info("Top-level comments remaining: 0")
            return
        if skip >= total:
            logging.info(
                (
                    "Skip %d has reached the end for post ID %d "
                    "(total top-level comments: %d)."
                ),
                skip,
                args.post_id,
                total,
            )
            logging.info("Top-level comments remaining: 0")
            return
        print(render_comments_jsonl(comments))
        remaining = max(total - (skip + len(comments)), 0)
        logging.info(
            (
                "Returned %d top-level comments for post ID %d "
                "(skip=%d). Top-level comments remaining: %d"
            ),
            len(comments),
            args.post_id,
            skip,
            remaining,
        )
    except Exception as exc:
        # Preserve the original traceback by re-raising without modification.
        logging.error("make_query failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
