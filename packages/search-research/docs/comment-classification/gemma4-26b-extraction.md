# Gemma 4 26B NVFP4: book extraction and labeling suitability

The larger model and worked examples improve extraction, but do not produce
reliable unattended training labels. At the measured direct-output concurrency,
the random-sample book gate is 70% precision / 82% recall. Thinking improves
returned-title precision in book-heavy comments to 154/156 (98.7%), but misses
5/17 positive random comments, produces one incomplete response, and retains
incorrect author links. Use these outputs as annotation proposals. The next
useful step is a stronger teacher comparison, followed by reviewed training
comments and NER fine-tuning.

## Deployment

For this experiment, all other GPU consumers were stopped on melchior's RTX 5090: the E4B extraction
container and the embedding container. GLiNER was already unloaded. Embedding
search was unavailable while the larger model owned the GPU. After the
[managed API bakeoff](api-extraction-bakeoff.md), Gemma 26B was stopped and the
embedding container restarted.

The experiment used container `searchhn-gemma4-26b-extraction`, endpoint
`http://127.0.0.1:18082/v1`, served model `gemma4-26b`:

```sh
vllm serve RedHatAI/gemma-4-26B-A4B-it-NVFP4 \
  --served-model-name gemma4-26b --dtype bfloat16 \
  --gpu-memory-utilization .94 --max-model-len 96000 \
  --max-num-seqs 128 --max-num-batched-tokens 2048 \
  --language-model-only --async-scheduling --no-enable-prefix-caching \
  --reasoning-parser gemma4 --host 0.0.0.0 --port 8000
```

vLLM 0.23.0; image ID
`sha256:f37691f675bb82f734f606de8af90e777d3f80a20b120e699fd43fd10e60b8d7`.
Resolved model cache revision: `5557756b8dce33ac72f2bd702b11729fdba3b839`.
The checkpoint uses NVFP4 quantization; remaining computation and KV cache are
BF16. Weights occupied 14.8 GiB; KV allocation 12.46 GiB; observed GPU process
usage 30,292 MiB. The full 96,000-token limit fits without CPU offloading.
Startup reports approximately 494k aggregate KV tokens and 5.15 simultaneous
full-length requests. That capacity is unrelated to the much shorter comments.

The runtime warns about differing NVFP4 global scales in fused linear layers.
No unquantized control was run, so these results characterize this checkpoint
and runtime, not every possible deployment of the architecture.

Sources: [quantized model](https://huggingface.co/RedHatAI/gemma-4-26B-A4B-it-NVFP4),
[Google sampling guidance](https://huggingface.co/google/gemma-4-26B-A4B-it#best-practices),
[vLLM recipe](https://docs.vllm.ai/projects/recipes/en/stable/Google/Gemma4.html).

## Controlled prompt trials

All runs use the same 320 comments and existing corrected gold labels; one
previously excluded ambiguous comment leaves 319 scored. The 192 random comments
estimate the filtered population. The other 127 comments are deliberately
book-heavy and contain 163 title references in 60 positive comments. These are
reused development audits, not a new held-out test. No new aliases were added.
The matching rules and prior aliases are unchanged from the E4B comparison.

P1 is the previous E4B prompt. P2 adds six synthetic worked examples plus explicit
rules about unnamed books, URLs, slash-separated titles and author attribution.
P3 reorganizes the task into five decisions and nine worked examples. Neither
prompt copies an evaluation comment or its answer. Both requested prompt
revisions were tested on the complete sample.

| Configuration | Random gate TP / FP / FN | Gate precision / recall | Book-heavy title references found | Valid emitted titles | Clean complete positive comments |
|---|---:|---:|---:|---:|---:|
| Prior E4B direct | 15 / 19 / 2 | 44.1% / 88.2% | 154/163 | 153/180 | 50/60 |
| 26B P1 greedy, c32 | 15 / 11 / 2 | 57.7% / 88.2% | 158/163 | 158/174 | 53/60 |
| 26B P2 greedy, c32 | 14 / 7 / 3 | 66.7% / 82.4% | 153/163 | 152/159 | 52/60 |
| 26B P3 greedy, c32 | 14 / 8 / 3 | 63.6% / 82.4% | 156/163 | 155/172 | 50/60 |
| 26B P2 sampled, c32 | 15 / 5 / 2 | 75.0% / 88.2% | 151/163 | 150/158 | 52/60 |
| **26B P2 sampled, c64** | **14 / 6 / 3** | **70.0% / 82.4%** | **156/163** | **155/163** | **52/60** |
| 26B P2 sampled + thinking, c32 | 12 / 3 / 5 | 80.0% / 70.6% | 155/163 | 154/156 | 54/60 |

Sampled means temperature 1, top-p .95, top-k 64, seed 20260920, as recommended
by Google. Direct output has a 2,048-token ceiling; thinking has 8,192 tokens.
JSON Schema constrains the same `{has_any_book, books: [{title, author}]}` object.
Titles and author wording come from the comment; author may be null.

All direct runs returned 320 valid objects. Thinking with greedy decoding hit
the token ceiling on 12 comments, with repeated self-checks in the raw output.
Using recommended sampling reduced this to one incomplete response, comment
43838776. The table's thinking title-recall and complete-comment denominators
include that failure. Its three titles were not recovered; the failure is not
silently dropped. An earlier greedy-thinking trial is retained separately and
is not a usable configuration.

Changing batching from c32 to c64 changed some sampled answers despite a fixed
seed. Report the c64 result alongside throughput, and retain c32 as a robustness
check; do not select only its higher gate score. Title matching does not evaluate
author correctness. Alias references can map two gold names to one returned book,
so recovered references and valid emitted objects have slightly different counts.

## Manual checks and remaining errors

- Comment 46392391: the 16-title screenshot list is correctly split by both
  greedy prompt revisions, sampled c64, and sampled thinking. Sampled c32 still
  merges the two Primo Levi books. All author assignments in the thinking result
  match the stated names; the unmentioned author of The Lord of the Rings is null.
- Comment 46396803: thinking extracts all 19 books/series and their author links,
  including Southern Reach and The Final Architecture. Direct P2 leaves the
  latter's author null despite its placement in the Adrian Tchaikovsky list.
- Comments 42662453 and 44881788: revised prompts stop assigning the publisher
  Manning and actor Gary Cooper as book authors.
- Comment 43838776: sampled c64 incorrectly assigns The Deluge to Doctorow;
  sampled c32 instead misses Walkaway's author; thinking does not finish.
- Comment 45711672: direct P2 fixes the Van Vogt spillover, but sampled thinking
  again assigns A voyage to Arcturus to Van Vogt. Its author should be null.
- Comment 45895433: thinking correctly separates Perceptrons from Minsky & Papert
  and extracts Parallel Distributed Processing without surrounding volume wording.
- Comment 43617144: both distinct The Emigrants books are retained, with their
  different authors. Thinking omits the author of The Rings of Saturn.
- Thinking misses short references such as Napkin, Pepys, CYOA and UHH. It also
  misses I Am Error and Final Fantasy V in an explicitly book-related paragraph.
  It still returns a Benjamin essay and Telephone Tapes as books.

Some old labels need further adjudication before becoming training gold: for
example, “seen A Scanner Darkly” can denote the film, and “Foundation ... Time
Bandits” lacks parent context. Scores here intentionally preserve the earlier
labels instead of changing the benchmark to fit the current model. These cases
do not explain away the clear author, omission, and generation failures above.

## Throughput

One complete comment per structured-output request; no input truncation, no
prefix-cache reuse, no retries. Warmup is excluded. HTTP, inference, constrained
decoding and validation are timed; corpus reads and startup are not.

A 256-comment greedy P2 sweep gave 9.6, 22.5, 43.1, 50.9, 55.8 and 54.7 comments/s
at concurrency 1, 4, 16, 32, 64 and 128. The sampled configuration was then measured
on all 512 frozen uniform filter-pass comments:

| Concurrent requests | Comments/s | p95 latency |
|---:|---:|---:|
| 32 | 48.65 | 1.14 s |
| **64** | **54.62** | **1.87 s** |
| 128 | 54.09 | 2.95 s |

The sampled knee is 64. At that setting, 108,194 filter passes project to
**33 minutes**, versus E4B's 19m42s and GLiNER's 3m19s. A separate uniform
512-comment full-corpus sample gives 56.17/s, projecting **16.2 hours** for all
3,266,889 comments. These are sampled warm projections, not completed corpus jobs.

Sampled thinking produced 319 valid answers in 166.2 seconds on the mixed audit
set: 1.92 valid comments/s, versus direct sampled c32's 37.1/s on the same inputs.
No population-scale or concurrency-knee claim is made for thinking. At the audit
mix alone, proposing 1k–10k comment labels would take roughly 9–87 minutes,
excluding review and handling incomplete responses.

## Training direction and receipts

Generate 1k reviewed training comments first, including title-free and confusing
negative cases, then expand toward 10k if teacher quality and learning curves
justify it. Convert copied titles into checked source spans for NER; keep author
linking as a separately scored relation task. Do not turn every teacher-negative
comment into unquestioned negative gold. Keep a fresh thread-separated test set
outside prompt development and training.

All raw outputs, prompts, timings, server configuration and eleven aligned
spotchecks are in `data/probes/books-gemma4-26b-v1/`. Main receipts:

- `summary.json`: trial counts and corpus-time calculations.
- `prompt-v2.txt`, `prompt-v3.txt`: both explicit prompt revisions.
- `*-audit/metrics.json`, `*-audit/cases.jsonl`: aggregate scores and each mismatch.
- `direct-sampled-prompt2-c64/`: quality at the measured throughput concurrency.
- `thinking-sampled-prompt2/`: complete responses, raw reasoning, and the failure.
- `confirm-sampled-prompt2/`, `full-sampled-prompt2/`: representative timings.
- `spotcheck-criteria.jsonl`, `spotchecks.jsonl`: manually reviewed source comments
  and aligned predictions, rather than a population author-accuracy estimate.

The reusable client is `tools/comment_book_llm.py`; it now accepts `--model`,
`--prompt-file`, `--thinking`, `--max-tokens`, and `--sampling gemma`.
