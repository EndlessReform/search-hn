# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.28,<1", "numpy>=2,<3"]
# ///
"""Compare final proxy coordinates with raw vLLM plus the research NumPy transform.

uv run transform.py PROXY_BASE RAW_BASE [JSON_INPUTS]
Bases include /v1. Optional input file is a JSON list of strings or {"input":text}.
Uses one bulk call at a time. This is output compatibility, not a retrieval eval.
"""
import json
import sys
from pathlib import Path

import httpx
import numpy as np

proxy_base, raw_base = [value.rstrip('/') for value in sys.argv[1:3]]
queries = json.loads(Path(sys.argv[3]).read_text()) if len(sys.argv) == 4 else [
    'database indexes', 'a short query', 'How does garbage collection work?',
    '日本語の検索', 'naïve café — Unicode embeddings',
]
queries = [q['input'] if isinstance(q, dict) else q for q in queries]
changed = 0
coordinates = 0
recipes = set()
with httpx.Client(timeout=40) as client:
    for offset in range(0, len(queries), 8):
        body = {'model': 'pplx-embed-v1-0.6b', 'input': queries[offset:offset + 8],
                'encoding_format': 'float'}
        raw = client.post(f'{raw_base}/embeddings', json={**body, 'priority': 1})
        raw.raise_for_status()
        result = client.post(f'{proxy_base}/embeddings', json=body,
                             headers={'X-Embedding-Workload': 'bulk'})
        result.raise_for_status()
        data = result.json()
        assert data['embedding_recipe'] == result.headers['X-Embedding-Recipe']
        recipes.add(data['embedding_recipe'])
        pooled = np.asarray([r['embedding'] for r in sorted(raw.json()['data'], key=lambda r:r['index'])], dtype=np.float32)
        expected = np.clip(np.rint(np.tanh(pooled)*np.float32(127)), -128, 127).astype(np.int8)
        actual = np.asarray([r['embedding'] for r in data['data']])
        assert actual.shape == expected.shape
        assert np.isfinite(actual).all() and ((actual >= -128) & (actual <= 127)).all()
        assert (actual == np.rint(actual)).all()
        changed += int((actual != expected).sum())
        coordinates += actual.size
print(json.dumps({'queries':len(queries),'coordinates':coordinates,
                  'different_coordinates':changed,'recipes':sorted(recipes)},indent=2))
assert changed == 0, f'{changed} coordinate differences require investigation'
