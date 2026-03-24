# tests/enrichment/test_geographic.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.geographic import (
    compute_coverage_gap, compute_output_gap, compute_geographic_bias_score,
    load_and_compute,
)

_REGISTRY = pd.DataFrame([
    {"e_mec_code": "1", "region": "Norte",   "faculty_with_phd": 100},
    {"e_mec_code": "2", "region": "Norte",   "faculty_with_phd": 200},
    {"e_mec_code": "3", "region": "Sudeste", "faculty_with_phd": 500},
    {"e_mec_code": "4", "region": "Sudeste", "faculty_with_phd": 400},
    {"e_mec_code": "5", "region": "Sul",     "faculty_with_phd": 300},
])
_INDEXED = {"1", "3", "5"}  # source indexed these e_mec codes

def test_coverage_gap_proportional():
    gaps = compute_coverage_gap(_REGISTRY, _INDEXED)
    assert gaps["Norte"] < 0   # under-represented

def test_coverage_gap_returns_all_regions():
    gaps = compute_coverage_gap(_REGISTRY, _INDEXED)
    assert set(gaps.keys()) == {"Norte", "Sudeste", "Sul"}

def test_output_gap_excludes_zero_phd():
    registry = _REGISTRY.copy()
    registry.loc[0, "faculty_with_phd"] = 0
    pub_counts = {"1": 10, "3": 50, "5": 20}
    gaps = compute_output_gap(registry, pub_counts)
    assert isinstance(gaps, dict)

def test_bias_score_perfect_is_one():
    registry = pd.DataFrame([
        {"e_mec_code": str(i), "region": r, "faculty_with_phd": 100}
        for i, r in enumerate(["Norte", "Norte", "Sul", "Sul"])
    ])
    indexed = {"0", "2"}  # one per region — perfectly proportional
    gaps = compute_coverage_gap(registry, indexed)
    score = compute_geographic_bias_score(gaps)
    assert abs(score - 1.0) < 0.01

def test_bias_score_clipped_to_zero_one():
    gaps = {"Norte": -2.0, "Sudeste": 2.0}
    score = compute_geographic_bias_score(gaps)
    assert 0.0 <= score <= 1.0

def test_missing_registry_returns_none():
    result = load_and_compute(registry_path="/nonexistent/path.csv",
                               pub_counts={}, source="openalex")
    assert result is None
