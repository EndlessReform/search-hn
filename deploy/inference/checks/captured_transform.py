# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.28,<1", "numpy>=2,<3"]
# ///
"""Verify proxy math against the SAME pooled response, avoiding inference batch variance.

Configure a temporary proxy instance's EMBED_BACKEND to this recorder's port 18083.
Run: uv run captured_transform.py PROXY_BASE RAW_BASE JSON_INPUTS
For Docker Desktop/OrbStack the proxy backend is http://host.docker.internal:18083.
This recorder binds local loopback only; it is a verification tool, never a deployment.
"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import numpy as np

proxy_base, raw_base = [s.rstrip('/') for s in sys.argv[1:3]]
queries = json.loads(Path(sys.argv[3]).read_text())
queries = [q['input'] if isinstance(q, dict) else q for q in queries]
captured = []


class Recorder(BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.path == '/v1/embeddings'
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with httpx.Client(timeout=35) as client:
            response = client.post(f'{raw_base}/embeddings', json=body)
        response.raise_for_status()
        captured.append(response.json())
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response.content)))
        self.end_headers()
        self.wfile.write(response.content)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(('127.0.0.1', 18083), Recorder)
threading.Thread(target=server.serve_forever, daemon=True).start()
changed = coordinates = 0
recipes = set()
try:
    with httpx.Client(timeout=40) as client:
        for offset in range(0, len(queries), 8):
            response = client.post(f'{proxy_base}/embeddings', json={
                'model':'pplx-embed-v1-0.6b', 'input':queries[offset:offset+8],
                'encoding_format':'float'}, headers={'X-Embedding-Workload':'bulk'})
            response.raise_for_status()
            assert len(captured) == 1
            raw = captured.pop()
            actual = response.json()
            assert actual['embedding_recipe'] == response.headers['X-Embedding-Recipe']
            recipes.add(actual['embedding_recipe'])
            pooled = np.asarray([r['embedding'] for r in sorted(raw['data'],key=lambda r:r['index'])], dtype=np.float32)
            expected = np.clip(np.rint(np.tanh(pooled)*np.float32(127)),-128,127).astype(np.int8)
            vectors = np.asarray([r['embedding'] for r in actual['data']])
            assert vectors.shape == expected.shape
            changed += int((vectors != expected).sum())
            coordinates += vectors.size
finally:
    server.shutdown()
print(json.dumps({'queries':len(queries),'coordinates':coordinates,
                  'different_coordinates':changed,'same_pooled_response':True,
                  'recipes':sorted(recipes)},indent=2))
assert changed == 0, f'{changed} transform differences require investigation'
