"""Standalone diagnostic entrypoint for the production webpage service."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from search_agent.web import (
    PublisherPolicy,
    WebConversationState,
    WebPageService,
    build_local_defuddle_extractor,
)
from search_agent.web.fetcher import WebPageFetcher
from search_agent.web.security import WebAddressError


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse a single diagnostic URL and optional provenance story ID."""

    parser = argparse.ArgumentParser(
        description="Run the search-agent webpage tool without the agent harness"
    )
    parser.add_argument("url", help="HTTP(S) page to authorize and open")
    parser.add_argument(
        "--story-id",
        type=int,
        default=None,
        help="Optional HN story ID included in failure guidance",
    )
    parser.add_argument(
        "--read-next",
        action="store_true",
        help="After opening, read the next cached chunk when one exists",
    )
    parser.add_argument(
        "--find",
        metavar="TERM",
        help="After opening, find a literal term in the cached page",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Authorize the supplied URL as a diagnostic root and print tool JSON."""

    args = parse_args(argv)
    state = WebConversationState()
    try:
        state.authorize(
            args.url,
            depth=0,
            story_id=args.story_id,
            source="standalone-diagnostic",
        )
    except WebAddressError as exc:
        print(
            json.dumps(
                {
                    "status": exc.status,
                    "reason": exc.reason,
                    "recommended_action": "Supply a valid public HTTP(S) article URL.",
                    "story_id": args.story_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2
    extractor, extractor_error = build_local_defuddle_extractor()
    service = WebPageService(
        state=state,
        policy=PublisherPolicy.load(),
        fetcher=WebPageFetcher(),
        extractor=extractor,
        extractor_error=extractor_error,
    )
    payload = service.open(args.url)
    output: dict[str, object] = payload
    if payload["status"] == "ok" and (args.read_next or args.find is not None):
        output = {"status": "ok", "open": payload}
        if args.read_next:
            next_cursor = payload.get("next_cursor")
            output["read"] = (
                service.read(page_id=str(payload["page_id"]), cursor=next_cursor)
                if isinstance(next_cursor, str)
                else None
            )
        if args.find is not None:
            output["find"] = service.find(
                page_id=str(payload["page_id"]),
                term=args.find,
            )
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
