"""Validate four manual review shards and render a separate diagnostic annotation set.

Annotations are subjective, target-aware judgments, not replacement relevance
labels or measured counterfactual retrieval. Preserve raw reviewer evidence.
"""

import json
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from search_research.dataset import read_jsonl
from search_research.miss_audit import AUDIT


class Annotation(BaseModel):
    model_config = ConfigDict(extra="allow")
    case: str
    title_url_solvability: Literal["clear", "plausible", "weak", "not_supported"]
    question_validity: Literal["supported", "ambiguous", "unsupported"]
    primary_cause: Literal[
        "retrieval_ranking",
        "query_or_stopping",
        "missing_title_signal",
        "ambiguous_target",
        "unsupported_question",
        "infrastructure",
    ]
    evidence: str
    question_answer_evidence_present: bool | None = None
    answer_supported_by_frozen_source: bool | None
    alternate_answer_reasonable: bool | None
    suggested_title_only_query: str | None
    hindsight_warning: str
    recommendation: Literal["keep", "flag_ambiguous", "flag_unanswerable"]
    confidence: Literal["high", "medium", "low"]


def main():
    selection = json.loads((AUDIT / "selection.json").read_text())
    rows = []
    packets = {}
    for shard in range(1, 5):
        packet = {r["case"]: r for r in read_jsonl(AUDIT / f"packet-{shard}.jsonl")}
        packets.update(packet)
        annotations = [
            Annotation.model_validate(r)
            for r in json.loads((AUDIT / f"review-{shard}.json").read_text())
        ]
        assert {a.case for a in annotations} == set(packet), (
            f"Shard {shard} coverage mismatch"
        )
        for annotation in annotations:
            rows.append(
                {
                    "reviewer": f"luna-{shard}",
                    **annotation.model_dump(),
                    "style": packet[annotation.case]["style"],
                    "question": packet[annotation.case]["question"],
                    "title": packet[annotation.case]["target"]["title"],
                }
            )
    assert len(rows) == len({r["case"] for r in rows}) == selection["case_count"]
    assert {r["case"] for r in rows} == set(selection["cases"])
    (AUDIT / "annotations.jsonl").write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
    )
    frame = pl.DataFrame(rows)
    frame.write_parquet(AUDIT / "annotations.parquet")
    summary = {
        field: frame.group_by(field).len().sort(field).to_dicts()
        for field in (
            "title_url_solvability",
            "question_validity",
            "primary_cause",
            "recommendation",
            "question_answer_evidence_present",
            "alternate_answer_reasonable",
        )
    }
    (AUDIT / "summary.json").write_text(json.dumps(summary, indent=2))
    lines = [
        "# Residual miss audit — four Luna reviewers",
        "",
        "18 question cases; one reviewer per case. Target-aware manual judgments, not proven ranking causes. No relevance labels changed.",
        "",
        "| Case | Title | Title/URL findability | Question validity | Suggested disposition |",
        "|---|---|---|---|---|",
    ]
    for r in sorted(rows, key=lambda r: r["case"]):
        lines.append(
            f"| {r['case']} | {r['title'].replace('|', '/')} | {r['title_url_solvability']} | {r['question_validity']} | {r['recommendation']} |"
        )
    for r in sorted(rows, key=lambda r: r["case"]):
        lines.extend(
            [
                "",
                f"## {r['case']} — {r['title']}",
                "",
                r["question"],
                "",
                r["evidence"],
                "",
                "Hindsight / limits: " + r["hindsight_warning"],
            ]
        )
    (AUDIT / "review.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
