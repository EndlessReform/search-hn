"""Manual annotations remain explicit and separate from scored relevance labels."""

import pytest
from pydantic import ValidationError
from search_research.miss_audit_report import Annotation


def sample():
    return {
        "case": "42-paraphrase",
        "title_url_solvability": "weak",
        "question_validity": "supported",
        "primary_cause": "missing_title_signal",
        "evidence": "The answer is in the frozen comment, not the title.",
        "question_answer_evidence_present": True,
        "answer_supported_by_frozen_source": False,
        "alternate_answer_reasonable": None,
        "suggested_title_only_query": None,
        "hindsight_warning": "Exact target title was not supplied by question.",
        "recommendation": "keep",
        "confidence": "medium",
    }


def test_grounded_question_does_not_imply_supported_final_answer():
    annotation = Annotation.model_validate(sample())
    assert annotation.question_answer_evidence_present
    assert not annotation.answer_supported_by_frozen_source


def test_unknown_verdict_rejected():
    with pytest.raises(ValidationError):
        Annotation.model_validate({**sample(), "question_validity": "probably junk"})
