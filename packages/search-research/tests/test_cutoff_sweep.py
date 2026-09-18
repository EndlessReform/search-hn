"""Guard replay semantics against accidentally repairing the recorded queries."""

import json
from datetime import date

from search_research.cutoff_sweep import predicate


def test_preserves_encoded_array_and_filters():
    query = '["ISS", "space station leak"]'
    lexical, where, params = predicate({
        'tool': 'fetch_stories', 'query': query,
        'arguments': json.dumps({'min_score': 50, 'min_date': '2025-01-01',
                                 'include_domains': ['www.Example.com']}),
    })
    assert params['query'] == query
    assert params['min_date'] == date(2025, 1, 1)
    assert params['include_domains'] == ['example.com']
    assert 'min_score' not in lexical
    assert 'i.score >= :min_score' in where
    assert 'plainto_tsquery' in lexical


def test_daily_top_and_filter_only():
    lexical, _, params = predicate({
        'tool': 'fetch_top_stories_for_date', 'query': '',
        'arguments': '{"target_date":"2026-09-04","limit":20}',
    })
    assert 'i.day = :day' in lexical
    assert params['day'] == date(2026, 9, 4)
    lexical, where, _ = predicate({
        'tool': 'fetch_stories', 'query': '',
        'arguments': '{"min_date":"2026-09-04"}',
    })
    assert 'plainto_tsquery' not in lexical
    assert 'i.day >= :min_date' in where
