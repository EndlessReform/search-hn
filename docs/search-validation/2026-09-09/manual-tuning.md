# Manual ranking boosts — local preview, September 9

Added Freshness and Votes sliders (0–100, steps of 5) plus Apply/Reset. Slider
release submits the tuning form; no JavaScript is required for Apply. Sliders
rerank the entire cached candidate set, not just the visible page, with no new
embedding request. URL parameters preserve values through pagination; changing
boosts returns to page 1. The tuning form explicitly selects relevance order.

Formula (experimental, not a selected production recipe):

```
freshness = 2 ^ (-age_days / 30)
popularity = ln(1 + clamp(points, 0, 1000)) / ln(1001)
tuned = RRF * (1 + 4 * freshness_slider/100 * freshness
                + 4 * votes_slider/100 * popularity)
```

Missing time gives zero freshness; future time is clamped to age zero. Signals
are read for at most 200 retrieved candidates and frozen for the snapshot's
five-minute lifetime, allowing fair comparisons. Both zero restores RRF exactly.
RRF and Tuned are displayed ×1000 to one decimal; cosine remains three decimals.
Raw scores remain unscaled in JSON. This does not improve candidate recall or
remove the 25-point corpus floor; boosts can promote irrelevant recent/popular
candidates. No cutoff or semantic-relevance calibration is included.

Checks: app build passed; unit tests cover neutral/bounded boosts, half-life,
missing data and URL parsing. Live read-only preview checks confirmed freshness
changes ordering, reset restores identical IDs, and tuned pages do not overlap.
For “folding iphone,” baseline first result was the Motorola Razr DIY story;
freshness 5 (and 50) promoted “Apple Announces Foldable 'iPhone Duo'” to first.
Browser slider keyboard interaction and Reset passed. Desktop and 390×844 dark
mobile views inspected; mobile document width equals viewport width. No agent
subtasks, deployment, release or production data changes performed.

## Details toggle correction during review

Slider CSS insertion had accidentally duplicated a block and replaced the scoped
`.show-search-details .search-details` selector with an unconditional display rule.
Removed the duplicate and restored the scoped selector. Verified on isolated preview
3083: details hidden initially, visible after toggle, still visible after reload,
and hidden again after toggling off. This check did not restart the reviewers'
3081 snapshot sessions. No ranking behavior changed.
