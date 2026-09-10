# Four Luna tuning reviews — September 9, 2026

## Conclusion

Manual boosts help recent-topic ordering, but this review does not justify a
single new default. Keep 0/0 as the neutral default and the controls experimental.
For a user explicitly seeking recent stories, 25/25 is a reasonable next preset
to evaluate—not an optimized or validated production choice. Historical searches
need stronger topic/date intent handling more than a larger freshness multiplier.

Four separate `gpt-5.6-luna` reviewers covered 40 intents: ten COVID historical
queries, ten React early-history queries, ten source stories dated September 9,
and ten dated August 26 (14 days earlier). Three ran concurrently; the fourth
started when a slot opened. Each tried baseline plus several weight pairs and
selected its preferred page subjectively. The fortnight reviewer also tried three
more specific phrasings, with each phrase's own unboosted comparison. Saved
sessions held candidates constant within each query's settings comparison.

## Reviewer scores

Top-ten ratings are 1–5. Topic measures topical relevance; quality also considers
usefulness, noise and intent fit. These are four unblinded model reviewers, one
per cohort, judging titles/metadata; the means are descriptive, not calibrated
measurements or independent human relevance labels.

| Cohort | Topic, baseline → selected | Quality, baseline → selected | Selected freshness/votes |
| --- | --- | --- | --- |
| COVID history | 4.5 → 4.6 | 3.9 → 4.1 | Varied; small gains, no consistent winner |
| React history | 3.9 → 3.9 | 2.9 → 2.9 | 0/0 for 9 of 10; 10/20 for license query |
| September 9 stories | 4.7 → 4.8 | 3.7 → 4.2 | 25/25 for 4; 60/40 for 6 |
| August 26 stories | 4.4 → 4.9 | 3.6 → 3.9 | 25/25 for 6; 50/50 for 4 |

COVID quality scores describe retrospective historical-topic usefulness. Two
preferred pages failed strict dated intent: March 2020 lockdowns included later
meta-analyses; 2021–22 long-COVID research included 2024–26 work. React had only
3/10 satisfactory preferred pages. The fortnight reviewer marked 9/10 satisfactory,
with Qwen remaining unsatisfactory despite a better exact-target position under
another setting. Do not compare those satisfaction rates as calibrated scores.

## What earns its keep

- **Freshness can promote a relevant recent match already below the fold.**
  `Tailscale networking tools`: Tailcat moved from #17 to #1 at 25/25; votes-only
  0/50 reached #4. The AWS-acquisition target moved #7 → #1 and visa-policy target
  #6 → #1 at 25/25.
- **Votes sometimes improve ordering, not recall.** Today's exact targets were
  already in the top 20 for 9/10 queries; that remained 9/10 after tuning. Top-1
  targets rose from 7/10 to 9/10. The Flock surveillance story was absent from
  every tested top-20 page. We did not inspect all 200 candidates, so that is not
  proof it was absent from the full candidate pool.
- **Historical boosts are weak medicine.** React-announcement queries still mix
  React Native, generic open-source announcements and later releases. COVID
  vaccine queries still mix HIV/influenza trials with COVID trials. Popularity
  sometimes raises those adjacent-topic stories instead of the intended ones.
- **Strong freshness can amplify lexical collisions.** The AirPods query at 75/0
  promoted Claude Opus 5 and Pixel Watch 5. The preferred 25/25 response excluded
  these. This is a reason to improve topical precision before increasing boosts.
- **Finding the target is not the whole objective.** On the adapted Qwen query,
  50/0 put the target at #9; the reviewer preferred the overall page at 25/25 even
  though the target was #13. That preference still did not earn “satisfactory.”

A 30-day decay contributes effectively nothing to years-old stories. A historical
reviewer selecting freshness 50 therefore does not establish that historical search
benefited from freshness; much of the change is attributable to votes or to newer
retrospectives. These trials varied limited grids, with subjective choices on the
same examples, and do not identify globally optimal weights.

## Next slice recommendation

Keep today's defaults unchanged. Use these cases as a small repeatable evaluation
set for (1) better multi-term/entity matching and (2) explicit date constraints or
clear recent-vs-historical intent. Then compare those changes with the current
candidate generation before choosing any stronger default boosts. No new relevance
threshold, query rewriting, preset or production deployment was implemented here.

## Evidence and audit

- [COVID report](covid.md), [raw runs and judgments](covid.json)
- [React report](react.md), [raw runs and judgments](react.json)
- [Today report](today.md), [source sample](today.json), [exact raw runs](today-raw.json)
- [Fortnight report](fortnight.md), [source sample and raw runs](fortnight.json)

The dated samples are the ten highest-score eligible stories on each DB day
(85 eligible on September 9; 125 on August 26), not random samples. That favors
popular targets and limits what we can conclude about a votes boost for ordinary
stories. Natural queries were derived from known targets, and three fortnight
queries were refined after misses. No held-out evaluation or article-body review
was performed. Story titles are mirror data, not independently verified news.

Synthesis checked raw IDs and score arithmetic rather than copying agent claims:
corrected a surveillance target misidentification, today's hit counts and averages,
COVID's baseline quality mean, and fortnight's preferred quality mean. The final
reports include those corrections. Today's exact response records were retained
through an audit rerun; compact earlier transcripts are marked accordingly.

During the review, fixed a CSS regression that made Details always visible.
Verified hidden → visible → reload-visible → hidden on an isolated preview, then
restarted the main local preview after data collection. No ranking formula or
production service was changed. See [implementation notes](../manual-tuning.md).
