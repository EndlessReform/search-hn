"""One Tantivy title search, with author scoring confined to its candidate pool.

The index stores author names and their Unicode letter/number tokens with each
work. Retrieval never builds a global name/work mapping or sends
catalog-wide ID lists back into a search engine. The fixed title pool is an
explicit recall/latency tradeoff: authors cannot rescue works outside that pool.
"""

import heapq
import json
import struct
import time
from pathlib import Path

import tantivy
from search_research.resolver_counts import COUNTS_PATH, load_counts


def float32(value):
    """Match Tantivy's score precision when adding the existing author bonus."""
    return struct.unpack("f", struct.pack("f", value))[0]


def author_bonus(authors, suggested_tokens, person_tokens):
    """Apply the original per-author formula without joining the whole catalog.

    A suggested name earns five times its best token overlap with one author.
    Contributors cannot combine their partial overlaps. With no suggestion,
    any complete person-name token set earns one non-stacking five-point bonus.
    An empty suggested-token set (e.g. initials only) does not enable fallback.
    """
    if suggested_tokens is not None:
        if not suggested_tokens:
            return 0.0
        return 5.0 * max(
            (
                sum(t in a["tokens"] for t in suggested_tokens) / len(suggested_tokens)
                for a in authors
            ),
            default=0.0,
        )
    return (
        5.0
        if any(
            tokens.issubset(a["tokens"])
            for tokens in person_tokens
            if tokens
            for a in authors
        )
        else 0.0
    )


class Catalog:
    """Retrieve at most 10,000 title hits and retain only fifty decoded works.

    Python receives bounded title hits from Tantivy and decodes one stored work
    at a time. The heap retains at most fifty metadata objects. A score bound
    can skip decoding the rest of this already-returned pool; it never issues
    another query or expands the pool. Score ties prefer higher frozen reading-log
    counts, then lower numeric work IDs. Counts stay outside the title index.
    """

    def __init__(self, path: Path, title_pool=10_000, *, counts_path=COUNTS_PATH):
        assert title_pool >= 50, "Title pool must contain at least fifty hits"
        self.title_pool = title_pool
        self.counts = load_counts(counts_path)
        self.index = tantivy.Index.open(str(path))
        self.analyzer = (
            tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
            .filter(tantivy.Filter.lowercase())
            .build()
        )
        self.index.register_tokenizer("lower", self.analyzer)
        self.searcher = self.index.searcher()

    def search(self, title, author, person_spans):
        """Return candidate dictionaries and per-query timing/volume counters."""
        started = time.monotonic()
        tokens = self.analyzer.analyze(title)
        assert tokens, f"Empty title query: {title!r}"
        query = self.index.parse_query(
            " ".join('"' + t + '"' for t in tokens), ["title"]
        )
        suggested = (
            {t for t in self.analyzer.analyze(author) if len(t) > 1} if author else None
        )
        people = [set(self.analyzer.analyze(s["text"])) for s in person_spans]
        search_started = time.monotonic()
        hits = self.searcher.search(query, limit=self.title_pool, count=False).hits
        search_seconds = time.monotonic() - search_started
        top = []
        decoded = 0
        certified = len(hits) < self.title_pool
        for raw, address in hits:
            if len(top) == 50 and float32(raw + 5.0) < top[0][0]:
                certified = True
                break
            stored = self.searcher.doc(address).to_dict()
            authors = json.loads(stored["author_data"][0])
            decoded += 1
            bonus = author_bonus(authors, suggested, people)
            score = float32(raw + float32(bonus))
            key = stored["id"][0]
            candidate = {
                "id": key,
                "title": stored["title"][0],
                "authors": sorted({a["name"] for a in authors}),
                "bm25": raw,
                "retrieval_score": score,
                "author_match": bonus > 0,
                "author_match_score": bonus,
            }
            # Negating the numeric ID makes a smaller ID a better heap key.
            # The root is the worst retained work; the same key orders output.
            numeric_id = int(key.removeprefix("/works/OL").removesuffix("W"))
            item = (score, self.counts.get(key, 0), -numeric_id, candidate)
            if len(top) < 50:
                heapq.heappush(top, item)
            elif item[:3] > top[0][:3]:
                heapq.heapreplace(top, item)
        return [v[3] for v in sorted(top, key=lambda v: v[:3], reverse=True)], {
            "seconds": time.monotonic() - started,
            "search_seconds": search_seconds,
            "search_calls": 1,
            "title_pool": self.title_pool,
            "hits_returned": len(hits),
            "metadata_decoded": decoded,
            "global_score_bound_satisfied": certified,
        }
