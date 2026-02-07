from __future__ import annotations

import argparse
from collections.abc import Sequence

import uvicorn

from exporter.mirror import run_mirror_job


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="exporter", description="HN exporter CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Run the FastAPI server")
    serve.add_argument("--host", default="0.0.0.0", help="Host interface to bind")
    serve.add_argument("--port", type=int, default=8000, help="Port to bind")
    serve.add_argument("--reload", action="store_true", help="Enable autoreload")

    mirror = subparsers.add_parser("mirror", help="Run one mirror job now")
    mirror.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not push datasets to Hugging Face",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "serve":
        uvicorn.run("exporter.app:app", host=args.host, port=args.port, reload=args.reload)
        return 0

    if args.command == "mirror":
        try:
            run_mirror_job(dry_run=args.dry_run)
        except RuntimeError as exc:
            parser.exit(status=2, message=f"error: {exc}\n")
        return 0

    parser.print_help()
    return 1
