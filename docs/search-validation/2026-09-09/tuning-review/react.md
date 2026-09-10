# React historical-query tuning review — September 9, 2026

This read-only review used the local `hn_app` API at `http://127.0.0.1:3081`. Each query began with freshness/votes `0/0`; six alternatives (`10/20`, `25/25`, `50/0`, `0/50`, `50/50`, `10/50`) were then evaluated with the returned session ID, so every comparison used the same cached candidate snapshot. The complete response payloads, including every displayed top 10, are in [react.json](react.json).

The topic-relevance rubric is 1 (mostly unrelated), 3 (mixed), 5 (almost entirely on-topic). Overall quality is 1 (unhelpful), 3 (usable with substantial misses/noise), 5 (strong useful results for the stated intent). Scores are subjective judgments of the displayed top 10; a preferred setting was chosen by inspection rather than by a mechanical score formula.

Across ten intents, baseline and preferred averages were unchanged: topic relevance **3.9/5**, overall quality **2.9/5**. Three queries were satisfactory: “Why did we build React 2013” (0/0), “React BSD license open source 2013” (10/20), and “React early performance diff algorithm 2013” (0/0). The other seven had useful items but significant year drift, generic matches, or missing direct evidence. Mild boosts generally reshuffled results; they did not improve historical recall. The strongest tuning win was the license query, where 10/20 kept React license explainers prominent while adding React Native open-source context.

| # | Query | Preferred F/V | Baseline → preferred (topic, quality) | Satisfactory | Main evidence / limitation |
|---:|---|---:|---:|:---:|---|
| 1 | `React announcement 2013 open source` | 0/0 | 3,2 → 3,2 | No | React Native and generic Facebook/Reddit open-source stories dominate; direct 2013 React announcement is absent. IDs 9180971, 15727823, 220733. |
| 2 | `Why did we build React 2013` | 0/0 | 5,4 → 5,4 | Yes | Exact rationale story 5826558 is first; later criticism and releases add noise. |
| 3 | `React early design architecture 2013` | 0/0 | 4,3 → 4,3 | No | Useful design/diff material (12096710, 9032370), but Fiber and newer architecture intrude. |
| 4 | `React JSX syntax 2013` | 0/0 | 3,2 → 3,2 | No | JSX-related results exist, but E4X, modern transforms, and later releases crowd out focused 2013 history. IDs 13900830, 7232695, 24555415. |
| 5 | `React virtual DOM 2013` | 0/0 | 4,3 → 4,3 | No | Strong virtual-DOM topic match but duplicates and later opinion/tooling stories dominate; boosts worsen year fit. IDs 31576634, 9675697, 9308088. |
| 6 | `React adoption early users 2013 2014` | 0/0 | 3,2 → 3,2 | No | Mostly release announcements, with later React Native adoption and 2024 commentary; adoption evidence is thin. |
| 7 | `React BSD license open source 2013` | 10/20 | 5,4 → 5,4 | Yes | React license explainers and Facebook React license remain prominent (15050841, 12692552), with React Native context (9271246); generic BSD items remain. |
| 8 | `React Facebook internal open source announcement` | 0/0 | 3,2 → 3,2 | No | Facebook generic infrastructure announcements and React Native crowd out a precise React announcement. |
| 9 | `React early performance diff algorithm 2013` | 0/0 | 5,4 → 5,4 | Yes | Direct diff/performance results fill most of top 10 (9032370, 6980469, 9824884); boosts add newer Fiber/release material. |
| 10 | `React first release 2013 history` | 0/0 | 4,3 → 4,3 | No | Early releases appear, but v0.13/v0.14 dominate and React 18/modern history intrudes. IDs 6936951, 8958731, 5826558. |

The principal limitation is candidate recall and temporal intent handling: the tuning formula only reranks the retrieved set and cannot force a 2013 result into the set. Popularity can also elevate newer or generic results. These results support keeping relevance-zero as the default for historical searches and treating freshness/votes as user-controlled exploratory signals rather than a historical-search fix.
