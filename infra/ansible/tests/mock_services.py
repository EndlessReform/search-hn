"""Local-only Firebase and embedding fixtures for systemd deployment rehearsal.

Run with UV. The control endpoint edits one synthetic story so tests can verify
real ingestion and embedding writes without connecting to either live service.
"""
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock

RECIPE = "pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1"
CREATED = int(time.time())
state = {"title": "fixture initial", "score": 50, "calls": 0, "requests": 0}
lock = Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def respond(self, body):
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("x-embedding-recipe", RECIPE)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/v0/updates.json":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            try:
                while True:
                    self.wfile.write(b'event: put\ndata: {"path":"/","data":{"items":[1]}}\n\n')
                    self.wfile.flush()
                    time.sleep(1)
            except (BrokenPipeError, ConnectionResetError):
                return
        elif self.path == "/v0/maxitem.json":
            self.respond(1)
        elif self.path == "/v0/item/1.json":
            with lock:
                state["requests"] += 1
                item = dict(id=1, type="story", title=state["title"], score=state["score"],
                            url="https://example.invalid/fixture", time=CREATED, by="fixture")
            self.respond(item)
        elif self.path == "/state":
            with lock:
                self.respond(dict(state))
        else:
            self.respond(None)

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/control":
            with lock:
                state.update({k: v for k, v in payload.items() if k in ("title", "score")})
            self.respond({"ok": True})
        elif self.path == "/embeddings":
            with lock:
                state["calls"] += 1
            self.respond({"model": "pplx-embed-v1-0.6b", "embedding_recipe": RECIPE,
                          "data": [{"index": i, "embedding": [1] * 1024}
                                   for i, _ in enumerate(payload["input"])]})
        else:
            self.send_error(404)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
