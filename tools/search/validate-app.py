#!/usr/bin/env -S uv run --script
"""Read-only HTTP contract checks for a running hn_app preview.

Run with `uv run tools/search/validate-app.py --base-url http://127.0.0.1:3081`.
An optional keyword-only preview exercises inference fallback and empty results.
No database mutation or credentials are involved; output contains small evidence.
"""
import argparse
import json
from time import monotonic
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


def request(base, params):
    start = monotonic()
    try:
        with urlopen(base + '/api/search?' + urlencode(params), timeout=45) as response:
            return response.status, json.load(response), round((monotonic() - start) * 1000)
    except HTTPError as error:
        return error.code, json.load(error), round((monotonic() - start) * 1000)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--fallback-url')
    args = parser.parse_args()
    base = args.base_url.rstrip('/')
    query = 'How to build a programming language compiler'
    status, first, elapsed = request(base, {'q': query})
    assert status == 200, first
    assert first['retrieval_mode'] == 'hybrid', first
    assert len(first['results']) == 20 and first['has_more']
    assert all(r['score'] >= 25 and r['url'] for r in first['results'])
    params = {'q': query, 'session': first['session'], 'page': 2}
    status, second, page_ms = request(base, params)
    assert status == 200 and second['page'] == 2, second
    assert not {r['id'] for r in first['results']} & {r['id'] for r in second['results']}
    _, repeated, _ = request(base, params)
    assert [r['id'] for r in repeated['results']] == [r['id'] for r in second['results']]
    evidence = {'hybrid_ms': elapsed, 'page_two_ms': page_ms, 'first_id': first['results'][0]['id'], 'page_size': len(first['results']), 'nonoverlapping_stable_pages': True}
    for sort, field in [('score', 'score'), ('date', 'time')]:
        status, body, ms = request(base, {'q': query, 'sort': sort})
        assert status == 200, body
        values = [r[field] for r in body['results']]
        assert values == sorted(values, reverse=True)
        evidence[sort + '_ms'] = ms
    for params, expected in [({'q': ''}, 400), ({'q': 'a' * 2049}, 400), ({'q': query, 'page': 0}, 400), ({'q': query, 'page': 2}, 410), ({'q': query, 'session': 999999999}, 410), ({'q': 'different', 'session': first['session']}, 400)]:
        status, body, _ = request(base, params)
        assert status == expected and body['error'], (params, status, body)
    evidence['invalid_and_expired_requests'] = 'passed'
    if args.fallback_url:
        status, body, ms = request(args.fallback_url, {'q': 'compiler'})
        assert status == 200 and body['retrieval_mode'] == 'keyword-only' and body['results'], body
        status, empty, _ = request(args.fallback_url, {'q': 'zzzzxqnonexistenttoken987654321'})
        assert status == 200 and empty['results'] == [] and not empty['has_more'], empty
        evidence['keyword_fallback_ms'] = ms
        evidence['empty_keyword_results'] = 'passed'
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
