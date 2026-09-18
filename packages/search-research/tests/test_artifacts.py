"""Artifact publication must be reconstructible without trusting remote paths."""

import hashlib
import io
import json

import pytest
from search_research import artifacts
from search_research.artifacts import digest, safe_destination
from search_research.rate_retry import is_rate_limit


def test_digest(tmp_path):
    path = tmp_path / "sample"
    path.write_bytes(b"frozen evaluation")
    assert digest(path) == hashlib.sha256(b"frozen evaluation").hexdigest()


@pytest.mark.parametrize("path", ["../secret", "/etc/passwd", "data/../../secret"])
def test_path_escape(tmp_path, path):
    with pytest.raises(AssertionError):
        safe_destination(tmp_path, path)


def test_safe_path(tmp_path):
    assert (
        safe_destination(tmp_path, "data/run/eval.jsonl")
        == tmp_path / "data/run/eval.jsonl"
    )


def test_stream_rate_classification():
    assert is_rate_limit("APIError", "Rate limit reached for model on tokens per min")
    assert is_rate_limit("RateLimitError", "HTTP 429")
    assert not is_rate_limit("APIError", "Content policy refusal")
    assert not is_rate_limit("MaxTurnsExceeded", "Max turns (10) exceeded")


def test_restore_checks_bytes_and_refuses_conflict(tmp_path, monkeypatch):
    payload = b"frozen data"
    manifest = {
        "schema_version": 1,
        "release": "v1",
        "files": [
            {
                "path": "data/run/eval.jsonl",
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ],
    }

    class Store:
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(json.dumps(manifest).encode())}

        def download_file(self, bucket, key, filename):
            from pathlib import Path

            Path(filename).write_bytes(payload)

    monkeypatch.setattr(artifacts, "client", lambda: (Store(), "test"))
    artifacts.restore("v1", tmp_path)
    artifacts.restore("v1", tmp_path, verify_only=True)
    path = tmp_path / "data/run/eval.jsonl"
    path.write_bytes(b"modified local file")
    with pytest.raises(AssertionError, match="Local file differs"):
        artifacts.restore("v1", tmp_path)
    assert path.read_bytes() == b"modified local file"
