# tests/enrichment/test_coauthorship.py
from __future__ import annotations
import pytest
from enrichment.coauthorship import (
    is_nonacademic, compute_coauth_metrics, NON_ACADEMIC_TYPES,
)

def test_non_academic_types_known():
    assert "company" in NON_ACADEMIC_TYPES
    assert "government" in NON_ACADEMIC_TYPES
    assert "education" not in NON_ACADEMIC_TYPES

def test_is_nonacademic_company():
    assert is_nonacademic(["company", "education"]) is True

def test_is_nonacademic_pure_academic():
    assert is_nonacademic(["education"]) is False

def test_is_nonacademic_empty():
    assert is_nonacademic([]) is False

def test_metrics_full_data():
    papers = [
        {"affiliation_types": [["education", "company"]], "ror_resolved": True},
        {"affiliation_types": [["education"]], "ror_resolved": True},
        {"affiliation_types": [["government"]], "ror_resolved": False},
    ]
    m = compute_coauth_metrics(papers)
    assert m["detectability"] == 1.0
    assert abs(m["volume_rate"] - 2/3) < 0.01
    assert abs(m["quality_score"] - 0.5) < 0.01

def test_metrics_zero_papers():
    m = compute_coauth_metrics([])
    assert m["detectability"] == 0.0
    assert m["volume_rate"] == 0.0
    assert m["quality_score"] == 0.0

def test_metrics_no_affiliation_types():
    papers = [{"affiliation_types": None, "ror_resolved": False}]
    m = compute_coauth_metrics(papers)
    assert m["detectability"] == 0.0

def test_composite_score_range():
    m = compute_coauth_metrics([
        {"affiliation_types": [["company"]], "ror_resolved": True}
    ])
    score = 0.4 * m["detectability"] + 0.3 * m["volume_rate"] + 0.3 * m["quality_score"]
    assert 0.0 <= score <= 1.0
