## mock_firebase

Fixture-driven Firebase test server for `catchup_worker` integration tests.

### Endpoints

- `GET /healthz`
- `GET /v0/maxitem.json`
- `GET /v0/item/{id}.json`

### Fixture format

```json
{
  "maxitem": 5,
  "items": {
    "1": { "id": 1, "type": "story", "title": "hello" },
    "2": null
  },
  "scripts": {
    "1": [429, 200],
    "3": [401]
  }
}
```

- `items[id] = null` returns `200` with JSON `null`.
- `scripts[id]` is an optional list of status codes, one per request attempt.
  `200` means "serve fixture item normally"; any non-200 returns that status.

### Run

```bash
uv run --project packages/mock_firebase \
  python packages/mock_firebase/main.py \
  --fixture /path/to/fixture.json \
  --host 127.0.0.1 \
  --port 18080
```
