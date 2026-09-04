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
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
