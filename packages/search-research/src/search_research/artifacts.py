"""Publish frozen research releases to Garage; restore and verify local copies.

Objects are content-addressed. A release manifest is published last, so interrupted
uploads can resume without advertising an incomplete release. Never publish a
directory while a driver is writing it. Credentials stay in the local .env.
"""

import argparse
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[4]
ROOTS = (
    "fts-baseline-20260904",
    "te3-large-baseline-20260904",
    "pg-duckdb-bakeoff-20260904",
    "luna-semantic-20260904",
)
SUFFIXES = {".json", ".jsonl", ".parquet", ".csv", ".npy", ".html", ".png", ".md"}


def digest(path: Path) -> str:
    """Hash incrementally, including large embedding shards without loading them."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def client():
    """Use conventional AWS credentials with Garage's path-style S3 endpoint."""
    load_dotenv(REPO / "packages/search-research/.env")
    return boto3.client(
        "s3",
        endpoint_url=os.environ["SEARCHHN_GARAGE_BASE_URL"].strip(),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "garage"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"mode": "standard", "max_attempts": 5},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    ), os.environ["SEARCHHN_EVAL_DATA_BUCKET"]


def exists(s3, bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def inventory():
    """Only named experiment roots and approved artifact formats leave this repo."""
    files, excluded = [], []
    for name in ROOTS:
        root = REPO / "data" / name
        assert root.is_dir(), f"Missing experiment: {root}"
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(REPO).as_posix()
            if (
                path.is_symlink()
                or path.suffix not in SUFFIXES
                or any(part.startswith(".") for part in path.relative_to(root).parts)
            ):
                excluded.append(relative)
                continue
            files.append(
                {"path": relative, "size": path.stat().st_size, "sha256": digest(path)}
            )
    return files, excluded


def publish(release):
    """Upload deduplicated blobs, then the immutable release manifest last."""
    s3, bucket = client()
    key = f"releases/{release}/manifest.json"
    assert exists(s3, bucket, key) is None, "Release exists; choose a new version"
    files, excluded = inventory()
    manifest = {
        "schema_version": 1,
        "release": release,
        "created_at": datetime.now(UTC).isoformat(),
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        "source_dirty": bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"], cwd=REPO, text=True
            ).strip()
        ),
        "files": files,
        "excluded": excluded,
    }
    for i, entry in enumerate(files, 1):
        blob = f"blobs/sha256/{entry['sha256']}"
        head = exists(s3, bucket, blob)
        if head is None:
            s3.upload_file(
                str(REPO / entry["path"]),
                bucket,
                blob,
                ExtraArgs={"Metadata": {"sha256": entry["sha256"]}},
            )
            head = s3.head_object(Bucket=bucket, Key=blob)
        assert head["ContentLength"] == entry["size"], f"Size mismatch: {blob}"
        assert head["Metadata"]["sha256"] == entry["sha256"]
        assert digest(REPO / entry["path"]) == entry["sha256"], (
            "File changed during upload"
        )
        if i % 50 == 0 or i == len(files):
            print(f"Uploaded/checked {i}/{len(files)}", flush=True)
    payload = json.dumps(manifest, indent=2).encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        IfNoneMatch="*",
    )
    print(
        json.dumps(
            {
                "manifest": f"s3://{bucket}/{key}",
                "sha256": hashlib.sha256(payload).hexdigest(),
                "files": len(files),
                "bytes": sum(e["size"] for e in files),
            }
        )
    )


def safe_destination(root, relative):
    path = (root / relative).resolve()
    assert path.is_relative_to(root.resolve()) and relative.startswith("data/"), (
        f"Unsafe artifact path: {relative}"
    )
    return path


def restore(release, destination, verify_only=False):
    """Refuse conflicting local files; checksum every downloaded or existing file."""
    s3, bucket = client()
    manifest = json.loads(
        s3.get_object(Bucket=bucket, Key=f"releases/{release}/manifest.json")[
            "Body"
        ].read()
    )
    assert manifest["schema_version"] == 1 and manifest["release"] == release
    for entry in manifest["files"]:
        path = safe_destination(destination, entry["path"])
        if not path.exists():
            assert not verify_only, f"Missing: {path}"
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + ".download")
            s3.download_file(bucket, f"blobs/sha256/{entry['sha256']}", str(temporary))
            assert digest(temporary) == entry["sha256"], f"Corrupt download: {path}"
            temporary.replace(path)
        assert (
            path.stat().st_size == entry["size"] and digest(path) == entry["sha256"]
        ), f"Local file differs; restore to a fresh directory: {path}"
    print(f"Verified {len(manifest['files'])} files for {release}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("publish", "restore", "verify"))
    parser.add_argument("release")
    parser.add_argument("--destination", type=Path, default=REPO)
    args = parser.parse_args()
    assert args.release and all(c.isalnum() or c in "-_." for c in args.release)
    if args.command == "publish":
        publish(args.release)
    else:
        restore(args.release, args.destination, args.command == "verify")


if __name__ == "__main__":
    main()
