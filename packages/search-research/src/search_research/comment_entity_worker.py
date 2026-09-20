"""Persistent GLiNER worker, isolated from FAISS's native OpenMP runtime.

One JSON line in, one JSON line out. Library output goes to stderr so it cannot
corrupt the protocol. Tracebacks remain visible in the explorer service log.
"""

import atexit
import json
import subprocess
import sys
import traceback
from contextlib import redirect_stdout
from threading import Lock

from search_research.comment_entities import EntityExtractor, EntityRequest


class EntityWorker:
    """Own one lazy child process; serialize requests without reloading weights."""

    def __init__(self, device="auto"):
        self.device = device
        self.process = None
        self.lock = Lock()
        atexit.register(self.close)

    def predict(self, text, request):
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                self.process = subprocess.Popen(
                    [sys.executable, "-m", __name__, self.device],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
            assert self.process.stdin is not None and self.process.stdout is not None
            self.process.stdin.write(
                json.dumps({"text": text, "request": request.model_dump()}) + "\n"
            )
            self.process.stdin.flush()
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError("GLiNER worker exited; see the service log")
            result = json.loads(line)
            if "error" in result:
                raise ValueError(result["error"])
            return result

    def close(self):
        """Release model memory when the owning explorer exits."""
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
            if self.process.stdin:
                self.process.stdin.close()
            if self.process.stdout:
                self.process.stdout.close()
            self.process = None


def main():
    extractor = EntityExtractor(sys.argv[1])
    for line in sys.stdin:
        try:
            payload = json.loads(line)
            with redirect_stdout(sys.stderr):
                result = extractor.predict(
                    payload["text"], EntityRequest.model_validate(payload["request"])
                )
        except Exception as error:  # noqa: BLE001 -- report each failed request across the process boundary
            traceback.print_exc(file=sys.stderr)
            result = {"error": f"{type(error).__name__}: {error}"}
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
