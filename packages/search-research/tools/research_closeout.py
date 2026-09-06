"""Publish and byte-verify the approved research closeout in private Garage.

The policy preserves evidence and selected Pplx arrays, excludes rejected model
arrays and rebuildable databases, and records every exclusion. No deletion is
performed here. Eight workers only parallelize independent content-addressed
objects; the immutable manifest is created last.
"""

import argparse
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from search_research.artifacts import REPO, client, digest, exists

ROOTS = (
    "fts-baseline-20260904",
    "te3-large-baseline-20260904",
    "pg-duckdb-bakeoff-20260904",
    "luna-semantic-20260904",
    "sovereign-embeddings-20260905",
    "sovereign-e2e-20260905",
    "luna-throughput-review-20260905",
    "mteb-retrieval-review-20260905",
    "openrouter-capacity-20260905",
    "pplx-vllm-gate-20260905",
    "full-corpus-sizing-20260906",
    "pplx-vm-latency-20260906",
    "pplx-ef1000-20260906",
    "research-closeout-20260906",
)
SUFFIXES = {
    ".json",
    ".jsonl",
    ".parquet",
    ".csv",
    ".npy",
    ".html",
    ".png",
    ".md",
    ".py",
    ".sh",
    ".sql",
    ".toml",
    ".yaml",
    ".yml",
    ".txt",
    ".log",
    ".sha256",
    ".typ",
    ".pdf",
    ".gz",
    ".bib",
    ".epoch",
    ".csl",
    ".rs",
}


def inventory(roots):
    """Explicit data roots only; secrets, caches and rejected vectors stay out."""
    files, excluded = [], []
    for name in roots:
        root = REPO / "data" / name
        assert root.is_dir(), root
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(REPO).as_posix()
            reason = None
            if path.is_symlink() or any(
                p.startswith(".") for p in path.relative_to(root).parts
            ):
                reason = "hidden/cache/symlink"
            elif path.suffix == ".npy" and "pplx" not in relative:
                reason = "recomputable rejected/control embedding array; older releases unchanged"
            elif path.suffix not in SUFFIXES and not path.name.endswith("uv.lock"):
                reason = "rebuildable or runtime-only format"
            record = {
                "path": relative,
                "size": path.stat().st_size,
                "sha256": digest(path),
            }
            if reason:
                excluded.append({**record, "reason": reason})
            else:
                files.append(record)
    return files, excluded


def publish(release, roots, receipt):
    s3, bucket = client()
    key = f"releases/{release}/manifest.json"
    assert exists(s3, bucket, key) is None, "Release already exists"
    files, excluded = inventory(roots)
    manifest = {
        "schema_version": 1,
        "release": release,
        "created_at": datetime.now(UTC).isoformat(),
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "source_dirty": bool(
            subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
        ),
        "files": files,
        "excluded": [e["path"] for e in excluded],
        "exclusion_details": excluded,
        "policy": "Durable research evidence and selected Pplx vectors; rejected arrays omitted by user request",
        "roots": list(roots),
    }
    (receipt / "inventory.json").write_text(json.dumps(manifest, indent=2))
    print(
        json.dumps(
            {
                "files": len(files),
                "bytes": sum(e["size"] for e in files),
                "excluded_files": len(excluded),
            }
        ),
        flush=True,
    )
    unique = {e["sha256"]: e for e in files}

    def upload(entry):
        blob = "blobs/sha256/" + entry["sha256"]
        head = exists(s3, bucket, blob)
        if head is None:
            s3.upload_file(
                str(REPO / entry["path"]),
                bucket,
                blob,
                ExtraArgs={"Metadata": {"sha256": entry["sha256"]}},
            )
            head = s3.head_object(Bucket=bucket, Key=blob)
        assert (
            head["ContentLength"] == entry["size"]
            and head["Metadata"]["sha256"] == entry["sha256"]
        )
        assert digest(REPO / entry["path"]) == entry["sha256"], "Local artifact changed"

    with ThreadPoolExecutor(max_workers=8) as pool:
        for i, _ in enumerate(pool.map(upload, unique.values()), 1):
            if i % 200 == 0:
                print(f"Uploaded/checked {i}/{len(unique)} blobs", flush=True)
    payload = json.dumps(manifest, indent=2).encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        IfNoneMatch="*",
    )
    (receipt / "manifest.json").write_bytes(payload)
    print(
        json.dumps(
            {"release": release, "manifest_sha256": hashlib.sha256(payload).hexdigest()}
        ),
        flush=True,
    )


def verify(release, receipt):
    """Download every distinct archived blob and hash actual bytes, not metadata."""
    s3, bucket = client()
    payload = s3.get_object(Bucket=bucket, Key=f"releases/{release}/manifest.json")[
        "Body"
    ].read()
    manifest = json.loads(payload)
    assert manifest["release"] == release and manifest["schema_version"] == 1
    unique = {e["sha256"]: e for e in manifest["files"]}

    def check(entry):
        body = s3.get_object(Bucket=bucket, Key="blobs/sha256/" + entry["sha256"])[
            "Body"
        ]
        h = hashlib.sha256()
        count = 0
        try:
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                h.update(chunk)
                count += len(chunk)
        finally:
            body.close()
        assert h.hexdigest() == entry["sha256"] and count == entry["size"], entry[
            "path"
        ]

    with ThreadPoolExecutor(max_workers=8) as pool:
        for i, _ in enumerate(pool.map(check, unique.values()), 1):
            if i % 200 == 0:
                print(f"Byte-verified {i}/{len(unique)} blobs", flush=True)
    result = {
        "release": release,
        "bucket": bucket,
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "files": len(manifest["files"]),
        "bytes": sum(e["size"] for e in manifest["files"]),
        "unique_blobs": len(unique),
        "actual_downloaded_bytes_verified": True,
        "verified_at": datetime.now(UTC).isoformat(),
    }
    (receipt / "manifest.json").write_bytes(payload)
    (receipt / "verified.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["inventory", "publish", "verify"])
    p.add_argument("release")
    p.add_argument("--only-closeout", action="store_true")
    p.add_argument("--receipt", type=Path, required=True)
    a = p.parse_args()
    a.receipt.mkdir(parents=True, exist_ok=True)
    roots = ("research-closeout-20260906",) if a.only_closeout else ROOTS
    if a.command == "inventory":
        files, excluded = inventory(roots)
        (a.receipt / "plan.json").write_text(
            json.dumps({"files": files, "excluded": excluded}, indent=2)
        )
        print(
            json.dumps(
                {
                    "files": len(files),
                    "bytes": sum(e["size"] for e in files),
                    "excluded": len(excluded),
                    "excluded_bytes": sum(e["size"] for e in excluded),
                }
            )
        )
    elif a.command == "publish":
        publish(a.release, roots, a.receipt)
    else:
        verify(a.release, a.receipt)
