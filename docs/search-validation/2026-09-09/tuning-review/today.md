# Today freshness/votes tuning review — 2026-09-09

Date anchor is disclosed and held constant: `items.day = DATE '2026-09-09'`, story type, score >= 25, and `public.story_search_eligible(i)`, ordered deterministically by score descending then id descending. The database count is 85 eligible stories, so the requested ten are available; this is the top-score ten, not a fallback to another day or corpus. Source sample is in [today.json](today.json). API: `http://127.0.0.1:3081/api/search?q=...&freshness=...&votes=...`.

Ratings use topic relevance (1 mostly unrelated, 3 mixed, 5 almost entirely on topic) and intent quality (1 unhelpful, 3 usable with substantial misses/noise, 5 strong useful results). Every query received baseline plus four varied alternatives: `0,0`, `25,25`, `75,0`, `0,75`, `60,40`. Scores below are `topic/quality`; positions refer to the exact selected story ID; absent means not found in the returned top 20.

| story | natural topic query | preferred | baseline -> preferred | why |
|---|---|---:|---:|---|
| 49623754 | shopping cart button blue | 60,40 | 4/3 -> 4/4, pos 1 | Target stays first, but cart wheels and QR-code results are noisy for a button-design intent; balanced tuning improves the lower list modestly. |
| 49630931 | foldable iphone duo | 25,25 | 5/4 -> 5/4, pos 1 | Balanced tuning puts the two Apple links first and keeps foldable-phone context; baseline has a folding-paper tangent. |
| 49626190 | Shopify Tailwind acquisition | 60,40 | 5/4 -> 5/4, pos 1 | Target stays first and the remainder is broadly Shopify/Tailwind company context, though not every item is acquisition-specific. |
| 49619227 | resigning from Anthropic | 60,40 | 5/4 -> 5/4, pos 1 | Same-day resignation duplicates lead, followed by relevant safety/governance context; some broader Anthropic noise remains. |
| 49624394 | surveillance dystopian society | 25,25 | 4/3 -> 4/3, absent | The requested source story is absent from all five top-20 responses; returned surveillance material is topical but cannot satisfy source-specific intent. |
| 49624823 | local models run on device | 25,25 | 4/3 -> 5/4, pos 1 | Balanced results are nearly all local-model/on-device setup material; baseline target is rank 3 and votes-only rank 4. |
| 49624603 | DeepSeek v4.1 flash | 60,40 | 5/4 -> 5/5, pos 1 | Target stays first; balanced ordering concentrates current Flash/V4 implementation, benchmark, and price material. |
| 49630253 | AirPods 5 | 25,25 | 5/4 -> 5/4, pos 1 | AirPods family results dominate, but freshness-only introduces two Claude Opus 5 collisions; the preferred 25/25 list removes those collisions. |
| 49624856 | malicious Google Ads | 60,40 | 5/4 -> 5/5, pos 1 | Target and malvertising result stay first; balanced ranking supplies concrete ad abuse/exploit coverage with low noise. |
| 49627370 | GPT-6 Astra reasoning | 60,40 | 5/4 -> 5/5, pos 1 | Strong Astra cluster across model, system card, robotics, coding, and rollout; votes-only puts a generic Astra page first. |

## Aggregate judgement

Preferred settings: `60,40` for 6/10, `25,25` for 4/10. Mean topic relevance is 4.7 at baseline and 4.8 preferred; mean intent quality is 3.7 at baseline and 4.2 preferred. Exact target hit count in the top 20 is 9/10 at both baseline and preferred. Top-1 targets improve from 7/10 to 9/10 (the surveillance target is absent in every setting; the foldable target is rank 2 and local-model target rank 3 at baseline). Ratings are intentionally lower where cart wheels/QR codes, generic company context, or duplicate/adjacent product stories weaken the stated intent.

The main limitation is candidate recall and corpus-floor behavior documented by the implementation: boosts rerank a frozen retrieved set (up to 200 candidates), do not retrieve new candidates, and can promote recent/popular but less relevant items. Ratings are one reviewer’s qualitative judgement over top-ten titles/metadata, without click labels or task-specific relevance judgments. The API session snapshot was reused per query as designed.

## Raw compact top-ten candidates

The exact API records for all 50 runs, each with top 20 candidates and metadata, are in [today-raw.json](today-raw.json). The compact transcript below preserves the earlier top-ten view for quick reading; repeated rankings are marked `same as`. Judgement is from titles and metadata only; no article body or full-text factual verification was performed.

```text
49623754 | shopping cart button blue
0,0: 49623754:Claude, change the “Add to Cart” button to blue || 44980004:Control shopping cart wheels with your phone (2021) || 477233:How I Reimplemented My Shopping Cart To Sell More Software, w/ Code || 5810742:QR Code in shopping cart handle || 6123076:Stripe Shop || 45416572:Instant Checkout for Merchants in ChatGPT || 7698733:AmazonCart – Add items to your Amazon.com shopping cart without leaving Twitter || 12852047:Shopping coming to Instagram || 484668:Shopping Cart Redesign Boosted Software Sales 94% in A/B Test || 23259159:Shopify Goes Digital by Default
25,25: 49623754:Claude, change the “Add to Cart” button to blue || 44980004:Control shopping cart wheels with your phone (2021) || 5810742:QR Code in shopping cart handle || 477233:How I Reimplemented My Shopping Cart To Sell More Software, w/ Code || 23259159:Shopify Goes Digital by Default || 45416572:Instant Checkout for Merchants in ChatGPT || 6123076:Stripe Shop || 12852047:Shopping coming to Instagram || 4820626:SimpleCart(js) - Javascript Shopping Cart || 4633443:Stripe Button (beta)
75,0: same as 0,0
0,75: 49623754:Claude, change the “Add to Cart” button to blue || 44980004:Control shopping cart wheels with your phone (2021) || 5810742:QR Code in shopping cart handle || 23259159:Shopify Goes Digital by Default || 477233:How I Reimplemented My Shopping Cart To Sell More Software, w/ Code || 45416572:Instant Checkout for Merchants in ChatGPT || 12852047:Shopping coming to Instagram || 7349607:Stripe Checkout || 4633443:Stripe Button (beta) || 6123076:Stripe Shop
60,40: same ordering as 25,25 through rank 8; ranks 9–10 are 4820626 SimpleCart(js), 4633443 Stripe Button (beta)

49630931 | foldable iphone duo
0,0: 49630666:Apple Announces Foldable 'iPhone Duo' || 49630931:iPhone Duo || 49630964:iPhone Duo || 4874540:Foldify - print foldable 3D figures from iOS || 21137450:Microsoft announces new Surface Duo phone || 18401636:This is Samsung’s foldable smartphone || 21533049:Moto Razr 2019: A foldable smartphone with no display crease || 19211428:Samsung’s Foldable Phone Is the $1,980 Galaxy Fold || 9204724:Microsoft's new foldable keyboard || 33525070:Primitive folding iPhone built from Motorola Razr and iPhone parts [video]
25,25: 49630931:iPhone Duo || 49630964:iPhone Duo || 49630666:Apple Announces Foldable 'iPhone Duo' || 49273330:Pixel 11 Pro Fold || 21533049:Moto Razr 2019: A foldable smartphone with no display crease || 9204724:Microsoft's new foldable keyboard || 21137450:Microsoft announces new Surface Duo phone || 4874540:Foldify - print foldable 3D figures from iOS || 19211428:Samsung’s Foldable Phone Is the $1,980 Galaxy Fold || 35892286:Google Pixel Fold
75,0: same target three; then 49273330 Pixel 11 Pro Fold, 49395605 Motorola non-folding device, 4874540 Foldify, 48461226 WWDC Apple is Folding, 49093845 Folding Paper Globes, 21137450 Surface Duo, 18401636 Samsung foldable
0,75: 49630931:iPhone Duo || 49630964:iPhone Duo || 21533049:Moto Razr 2019 || 9204724:Microsoft's new foldable keyboard || 21137450:Microsoft Surface Duo || 35892286:Google Pixel Fold || 19211428:Samsung Galaxy Fold || 45885813:iPhone Pocket || 4874540:Foldify || 49630666:Apple Announces Foldable 'iPhone Duo'
60,40: same target three; then 49273330 Pixel 11 Pro Fold, 49395605 Motorola non-folding device, 21533049 Moto Razr, 9204724 Microsoft keyboard, 21137450 Surface Duo, 4874540 Foldify, 35892286 Google Pixel Fold

49626190 | Shopify Tailwind acquisition
0,0: 49626190:Shopify acquires Tailwind || 40604997:Shopify acquiring Threads team || 6140168:Shopify Acquires Jet Cooper || 35813763:Shopify smaller/Flexport buys Logistics || 31273303:Shopify Acquire Delivrr || 41983607:Shopify Winning Salesforce Clients || 10545103:Shopify grows Q3 revenue || 33405997:Remix framework acquired by Shopify || 21388896:Shopify shares tumble || 32034643:The dark side of Shopify
25,25: 49626190 target || 35813763 Shopify/Flexport || 31273303 Shopify/Delivrr || 33405997 Remix/Shopify || 32034643 dark side Shopify || 6140168 Jet Cooper || 40604997 Threads team || 41983607 Salesforce || 33787218 Tailwind leaky abstraction || 10545103 Shopify revenue
75,0: same as 0,0
0,75: 49626190 target || 35813763 Shopify/Flexport || 32034643 dark side || 33405997 Remix || 31273303 Delivrr || 6140168 Jet Cooper || 33787218 Tailwind leaky abstraction || 40604997 Threads || 41983607 Salesforce || 22463769 Tailwind UI sales
60,40: target || 35813763 Shopify/Flexport || 31273303 Delivrr || 32034643 dark side || 33405997 Remix || 6140168 Jet Cooper || 40604997 Threads || 41983607 Salesforce || 33787218 Tailwind leaky abstraction || 22463769 Tailwind UI sales

49619227 | resigning from Anthropic
0,0: 49619227 target || 49624157 same title (Jacob Coxon) || 48194352 joined Anthropic || 47140734 safeguards || 46586766 third-party clients || 46625445 huge mistake || 47166397 Opus 3 exit interview || 45189053 SB 53 || 44534291 bleeding out || 47662350 dev goodwill
25,25: target || 49624157 || 48194352 || 46586766 || 47165397 core safety promise || 49623306 researcher quits/warning || 47140734 || 47292381 resigned OpenAI || 49082338 governance || 45064284 interview
75,0: target || 49624157 || 49623306 || 49619639 AI Responsibility || 49082338 governance || 48194352 || 49401229 IPO || 49401549 Claude Code effort || 49320144 revenue || 49283891 watermarks
0,75: target || 48194352 || 46586766 || 47165397 || 49624157 || 47292381 || 45064284 || 42915905 applicants || 47140734 || 47186127 government ties
60,40: target || 49624157 || 48194352 || 49623306 || 49619639 || 46586766 || 49082338 || 47165397 || 47140734 || 47292381

49624394 | surveillance dystopian society
0,0: 41213151 surveilled society || 16441347 Xinjiang surveillance || 25973598 democracy/surveillance || 8700136 Assange || 18285312 surveillance capitalism || 7279062 surveillance-state dystopia || 25468887 AR surveillance || 42584258 Serbia || 41574396 Ellison AI surveillance || 29252059 Singapore surveillance
25,25: 16441347 || 41213151 || 8700136 || 49628704 Anthropic predictive surveillance || 25973598 || 5991576 Distributed Everything || 7279062 || 29252059 Singapore || 25468887 AR || 6772527 surveillance control
75,0: 49628704 || 41213151 || 16441347 || 25973598 || 8700136 || 18285312 || 7279062 || 25468887 || 42584258 || 41574396
0,75: 16441347 || 41213151 || 8700136 || 5991576 || 29252059 || 25973598 || 6772527 || 7279062 || 47836730 default || 17486040 China AI cameras
60,40: 49628704 || 16441347 || 41213151 || 8700136 || 5991576 || 25973598 || 7279062 || 29252059 || 47836730 || 6772527

49624823 | local models run on device
0,0: 48555993 local models good now || 48089091 M4 24GB || 49624823 target || 33539192 iPhone Stable Diffusion || 49529132 M4 Pro setup || 38589520 hardware || 40262206 affordable hardware || 44429116 USB LLM || 39258361 open-source Ruby || 48636377 GLM local
25,25: target || 49529132 || 48555993 || 48089091 || 33539192 || 48636377 || 47363754 run AI locally || 49394148 290B local || 48982681 Nativ Mac || 47744255 Gemma Codex
75,0: target || 49529132 || 49394148 || 48995134 laptop model fit || 48982681 || 48555993 || 48636377 || 48089091 || 48775921 SOTA guide || 33539192
0,75: 48555993 || 33539192 || 48089091 || target || 47363754 || 49529132 || 48636377 || 46348329 coding models || 47744255 || 39258361 Ruby
60,40: target || 49529132 || 48555993 || 48089091 || 49394148 || 48636377 || 33539192 || 48982681 || 47363754 || 47744255

49624603 | DeepSeek v4.1 flash
0,0: target || 49214008 V4 Flash || 48160807 steering || 49166386 MI300X || 48050751 Metal || 48373675 MI300X || 49119559 update || 49120299 analysis || 49229621 Terminal Bench || 49386163 vision
25,25: target || 49214008 || 49386163 || 49166386 || 49119559 || 49120299 || 49274600 V4 Pro || 48050751 || 48160807 || 49229621
75,0: target || 49386163 || 49214008 || 49166386 || 49229621 || 49274600 || 49119559 || 49120299 || 49145463 || 49275114
0,75: 49214008 || target || 49119559 || 47884971 V4 || 48050751 || 49166386 || 49120299 || 49274600 || 49386163
60,40: target || 49214008 || 49386163 || 49166386 || 49119559 || 49274600 || 49120299 || 49229621 || 48050751 || 48160807

49630253 | AirPods 5
0,0: target || 19441487 AirPods || 21378197 AirPods Pro || 19441396 wireless case || 12446094 AirPods || 32754440 Pro 2nd gen || 47398681 Max 2 || 48592832 AirPods Effect || 35871565 Into Thin AirPods || 32349725 AirPods suck
25,25: target || 48592832 || 21378197 || 48710232 Librepods || 19441396 || 47398681 || 19441487 || 32754440 || 35871565 || 45941596 Librepods ecosystem
75,0: target || 48592832 || 48710232 || 49038393 Claude Opus 5 || 49038433 Claude Opus 5 || 49274757 Pixel Watch 5 || 19441487 || 21378197 || 47398681 || 19441396
0,75: target || 21378197 || 19441396 || 48592832 || 47398681 || 32754440 || 48710232 || 45941596 || 25344762 AirPods Max || 19441487
60,40: target || 48592832 || 48710232 || 21378197 || 19441396 || 47398681 || 32754440 || 19441487 || 49038433 Claude Opus 5 || 45941596

49624856 | malicious Google Ads
0,0: target || 35060972 malvertising || 1993747 DoubleClick malicious || 37932141 Notepad++ ads || 13129029 exploit kit || 34262227 MasquerAds || 45389500 malware Meta/Google || 40814552 info-stealer || 9628967 ad fraud || 47386868 Claude Code result
25,25: target || 35060972 || 22348568 ban ads || 33384236 Gimp ad || 19846555 AdWords exploit || 16583348 crypto ads || 13129029 || 1993747 || 37932141 || 9628967
75,0: target || 49116642 Fdroid malicious || 35060972 || 1993747 || 37932141 || 13129029 || 34262227 || 47386868 || 45389500 || 40814552
0,75: target || 35060972 || 22348568 || 33384236 || 16583348 || 19846555 || 13129029 || 19459604 hidden video ads || 37932141 || 9628967
60,40: target || 35060972 || 22348568 || 33384236 || 19846555 || 16583348 || 13129029 || 49116642 || 1993747 || 37932141

49627370 | GPT-6 Astra reasoning
0,0: target || 49554643 GPT-6 Astra || 49570545 OpenRouter || 49556147 coding index || 49555691 ARC-AGI || 49582582 robot arms || 49572875 code review || 49554273 rollout || 49555440 system card || 49554048 AGI era
25,25: target || 49554643 || 49570545 || 49582582 || 49555691 || 49554273 || 49572875 || 49556147 || 49555440 || 49554048
75,0: target || 49570545 || 49554643 || 49582582 || 49556147 || 49555691 || 49572875 || 49554273 || 49555440 || 49554048
0,75: 49554643 || target || 49570545 || 49555691 || 49582582 || 49554273 || 37050257 GPT-4 reason || 49572875 || 46982792 GPT-5 judges || 48789428 Codex reasoning
60,40: target || 49554643 || 49570545 || 49582582 || 49555691 || 49554273 || 49572875 || 49556147 || 49555440 || 49554048
```

Synthesis audit: parent recomputed means from the ten score rows and exact-ID hits from today-raw.json, correcting remaining arithmetic and target-count errors. The AirPods preferred-list description was corrected against the saved 25/25 response. Scores remain the reviewer's subjective judgments.
