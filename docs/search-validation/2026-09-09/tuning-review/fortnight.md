# Fortnight freshness/votes tuning review — September 9, 2026

This is a subjective review of the local `hn_app` API at `http://127.0.0.1:3081`.
The source anchor is `items.day = DATE '2026-08-26'` (14 days before the review),
`public.story_search_eligible(i)`, and score at least 25, ordered by score
descending then id descending. There were 125 eligible stories; the ten selected
below are the requested top ten. Source metadata came from the read-only database.

Each query is a natural topical query, rather than an exact copied title. The
topic interpretation is broad: the API was asked to find stories about the topic,
not to enforce the source date. The baseline (`0/0`) and alternatives reuse the
same returned session/candidate snapshot per query. Every query tried
`0/0`, `25/25`, `50/0`, `0/50`, `50/50`, and `75/75`; exact target positions are
by ID in the returned page (not by similar title). The three targets absent from
the first broad query were retried with a more specific natural query, with the
same six settings. Full API payloads are in [fortnight.json](fortnight.json).

| Source target (id; score) | Natural query | Baseline target position; ratings (topic, quality) | Preferred setting; target position; ratings | Satisfactory | Review reason |
|---|---|---:|---:|---|---|
| GLM-5.3-Flash (49449507; 1132) | `Zhipu GLM language model` (adapted after broad query) | absent; 4, 3 | `50/50`; 3; 5, 4 | yes | Specific wording recovered the target; balanced boosts kept a tight GLM list. |
| AWS Acquires DuckLabs (49448321; 1102) | `AWS acquisitions and developer tools` | 7; 4, 3 | `25/25`; 1; 5, 4 | yes | Both boosts promoted the exact AWS acquisition while retaining related AWS results. |
| U.S. State Department pauses immigrant visa applications (49452709; 840) | `immigrant visa policy changes` | 6; 4, 3 | `25/25`; 1; 5, 4 | yes | Target became first and the page stayed focused on visa policy. |
| Tim Curry has died (49451448; 724) | `Tim Curry film career` | 2; 5, 4 | `25/25`; 1; 5, 4 | yes | Target moved first; later ranks included a few generic film/feature tangents. |
| Qwen3.8-Flash-Next (49448210; 704) | `Qwen language model` (adapted after broad query) | absent; 4, 3 | `25/25`; 13; 4, 3 | no | Specific wording retrieved the target only at 13; `50/0` reached position 9, but its whole page was less useful, so 25/25 was retained as the better page-level compromise. |
| Tailcat – Like netcat, but over Tailscale’s data plane (49452990; 687) | `Tailscale networking tools` | 17; 4, 4 | `25/25`; 1; 5, 4 | yes | Freshness plus votes strongly promoted the exact Tailcat story; `0/50` instead put it at 4. |
| Twitter Viewer – View Twitter Without Account (49449576; 581) | `view Twitter without an account` | 1; 5, 4 | `50/50`; 1; 5, 4 | yes | Baseline was already excellent; boosts preserved first place, though `0/50` moved it to 2. |
| Meta reaches $17B settlement over social media harms to children (49448819; 543) | `social media harms children lawsuits` | 1; 5, 4 | `50/50`; 1; 5, 4 | yes | All settings kept the exact target first and produced a focused legal/harms page. |
| Mechanical Turk shutting down September 30 (49457545; 534) | `Amazon Mechanical Turk shutdown` | 3; 5, 4 | `25/25`; 1; 5, 4 | yes | Exact target moved first; higher `75/75` added more unrelated Amazon/AI noise lower down. |
| RAG Is Simpler Than You Think (49445727; 516) | `retrieval augmented generation RAG` (adapted after broad query) | absent; 4, 4 | `50/50`; 1; 5, 4 | yes | Specific wording plus balanced boosts recovered and promoted the target; the page remained RAG-focused. |

Ratings use topic relevance and usefulness separately: 1 means mostly unrelated or
unhelpful, 3 means mixed/usable with misses, and 5 means almost entirely topical or
strongly useful. The preferred choices are judgments about the overall top-ten page,
not just whether the selected target moved upward. In this sample, no preferred
setting harmed an otherwise strong page enough to reject it, but the high `75/75`
setting generally increased older or adjacent-topic items lower in the page. For
GLM, Qwen, and RAG, wording/candidate retrieval was the limiting factor before
weight tuning. For those three adapted rows, the displayed baseline position and
ratings are for the more specific query's own `0/0` run; the original broad-query
baselines were also target-absent in the returned top 20 and received the same
subjective ratings. “Absent” means absent from that returned page, not absent from
the full internal candidate set: tuning cannot add candidates to a snapshot, and
the adapted runs show that wording changes can expose them.

Across the ten rows, baseline averages were topic 4.4 and quality 3.6; preferred
averages were topic 4.9 and quality 3.9. Nine preferred settings were satisfactory.

The formula under review is the experimental multiplier described in
`crates/hn_app/src/search_tuning.rs`: `RRF * (1 + 4*freshness_weight*freshness +
4*votes_weight*popularity)`, with a 30-day freshness half-life and logarithmic
votes capped at 1000. This review is based on titles and metadata only; it does
not verify article full text or factual claims.
