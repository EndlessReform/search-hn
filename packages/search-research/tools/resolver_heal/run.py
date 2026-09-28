"""Run one frozen year through the filter, resolver, and publication stages."""

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

from search_research.ner_batching import NER_RECIPE
from search_research.resolver_counts import COUNTS_PATH

REPO = Path(__file__).resolve().parents[4]
CONFIG_SOURCE = Path("data/research/books-resolver-2025-v1/luna-config.json")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def repository_path(path):
    """Keep paths relative to the checkout so GPU containers see the same inputs."""
    return path.resolve().relative_to(REPO)


def initialize(args):
    """Freeze config and verify the identities needed for safe stage resumption.

    The 2025 config is a bootstrap template only. Once copied, each invocation
    uses the run's own config; no 2025 checkpoint is opened unless requested.
    Budget and concurrency are operational controls, recorded per invocation.
    """
    root = args.run_root
    root.mkdir(parents=True, exist_ok=True)
    config = root / "luna-config.json"
    if not config.exists():
        shutil.copyfile(args.config, config)
    with sqlite3.connect(
        (args.slice / "index.sqlite").resolve().as_uri() + "?mode=ro", uri=True
    ) as db:
        progress = db.execute(
            "SELECT total_rows,completed_rows FROM progress"
        ).fetchone()
        assert progress[0] == progress[1], "Slice embeddings are incomplete"
    recipe = json.loads(args.recipe.read_text())
    sources = [
        args.recipe,
        args.recipe.parent / recipe["anchor_file"],
        args.recipe.parent / recipe["model_file"],
        COUNTS_PATH,
        config,
    ]
    if args.baseline:
        sources.append(args.baseline)
    manifest = {
        "format": 1,
        "slice": str(args.slice),
        "recipe": str(args.recipe),
        "baseline": str(args.baseline) if args.baseline else None,
        "slice_index_sha256": digest(args.slice / "index.sqlite"),
        "sources": {str(p): digest(p) for p in sources},
    }
    path = root / "manifest.json"
    previous = json.loads(path.read_text()) if path.exists() else None
    # Older completed runs predate recipe recording. Preserve their manifest
    # contract; new runs freeze these settings and reject drift on resumption.
    if previous is None or "ner" in previous:
        manifest["ner"] = NER_RECIPE
    if path.exists():
        assert previous == manifest, "Run inputs changed; use a new run root"
    else:
        assert not (root / "references.json").exists(), (
            "Existing references lack this run manifest"
        )
        path.write_text(json.dumps(manifest, indent=2))
    with (root / "invocations.jsonl").open("a") as out:
        out.write(
            json.dumps({"budget": args.budget, "concurrency": args.concurrency}) + "\n"
        )


def main():
    """Skip completed artifacts; selectors retain paid receipts across restarts."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--slice", type=Path, required=True)
    parser.add_argument("--budget", type=float, required=True)
    parser.add_argument("--concurrency", type=int, default=512)
    parser.add_argument("--retrieval-workers", type=int, default=16)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument(
        "--recipe",
        type=Path,
        default=Path("data/research/books-quick-filter-v1/recipe.json"),
    )
    parser.add_argument("--config", type=Path, default=CONFIG_SOURCE)
    args = parser.parse_args()
    assert args.budget > 0 and args.concurrency > 0
    assert args.retrieval_workers > 0
    # Resolve caller paths before changing working directory.
    for field in ("run_root", "slice", "recipe", "config", "baseline"):
        value = vars(args)[field]
        if value is not None:
            vars(args)[field] = repository_path(value)
    os.chdir(REPO)
    initialize(args)
    env = os.environ | {
        "RESOLVER_RUN_ROOT": str(args.run_root),
        "RESOLVER_BASELINE": str(args.baseline) if args.baseline else "",
    }
    root = args.run_root
    heal = Path("packages/search-research/tools/resolver_heal")
    retry = Path("packages/search-research/tools/resolver_retry")

    def run(script, *arguments, dependency=None):
        command = ["uv", "run", "--no-sync", "--package", "search-research"]
        if dependency:
            command += ["--with", dependency]
        subprocess.run(
            [*command, "python", str(script), *map(str, arguments)], env=env, check=True
        )

    def rerank(folder, stage):
        subprocess.run(
            ["bash", str(folder / "run_rerank.sh"), stage], env=env, check=True
        )

    if (root / "checkpoint.sqlite").exists():
        print(
            "Already published; inputs verified, no work or paid calls repeated",
            flush=True,
        )
        return
    passes = root / "filter_passes.jsonl"
    if not passes.exists():
        run(
            heal.parent / "quick_filter.py",
            "run",
            "--slice",
            args.slice,
            "--recipe",
            args.recipe,
            "--output",
            passes,
            dependency="xgboost-cpu",
        )
    if not (root / "references.json").exists():
        titles = root / "titles"
        if not (titles / "title-manifest.json").exists():
            run(
                heal / "titles.py",
                "--slice",
                args.slice,
                "--passes",
                passes,
                "--output",
                titles,
                "--backend",
                NER_RECIPE["backend"],
            )
        shutil.copyfile(titles / "references.json", root / "references.json")
    if not (root / "person-ner.complete").exists():
        run(retry / "ner.py", "--backend", NER_RECIPE["backend"])
        (root / "person-ner.complete").touch()
    if not (root / "first-cases.json").exists():
        run(
            heal / "retrieve.py",
            "--stage",
            "first",
            "--workers",
            args.retrieval_workers,
            dependency="tantivy",
        )
    if not (root / "first-ranked.json").exists():
        rerank(retry, "first")
    if not (root / "round0-ready.json").exists():
        run(heal / "ready.py", "--stage", "first")
        shutil.copyfile(root / "first-ready.json", root / "round0-ready.json")
    for number in range(3):
        if number and not (root / f"round{number}-ready.json").exists():
            run(heal / "backlog.py", "--round", number - 1)
            if not (root / f"round{number}-cases.json").exists():
                run(
                    heal / "retrieve.py",
                    "--round",
                    number,
                    "--workers",
                    args.retrieval_workers,
                    dependency="tantivy",
                )
            if not (root / f"round{number}-ranked.json").exists():
                rerank(heal, f"round{number}")
            run(heal / "ready.py", "--stage", f"round{number}")
        run(
            heal / "select.py",
            "--round",
            number,
            "--concurrency",
            args.concurrency,
            "--budget",
            args.budget,
        )
    run(heal / "publish.py")
    run(heal / "backtest.py")


if __name__ == "__main__":
    main()
