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


def test_geographic_rows_include_inst_type():
    from enrichment.geographic import compute_coverage_gap_stratified
    registry = pd.DataFrame([
        {"e_mec_code": "1", "region": "Norte",   "inst_type": "federal_university", "faculty_with_phd": 100},
        {"e_mec_code": "2", "region": "Sudeste", "inst_type": "isolated_faculty",   "faculty_with_phd": 20},
    ])
    indexed = {"1"}
    rows = compute_coverage_gap_stratified(registry, indexed, "openalex")
    assert all("inst_type" in r for r in rows)
    assert all("sub_dimension" in r for r in rows)


def test_geographic_sub_dimension_values():
    from enrichment.geographic import compute_coverage_gap_stratified
    registry = pd.DataFrame([
        {"e_mec_code": "1", "region": "Norte", "inst_type": "federal_university", "faculty_with_phd": 100},
    ])
    rows = compute_coverage_gap_stratified(registry, {"1"}, "openalex")
    sub_dims = {r["sub_dimension"] for r in rows}
    assert "geographic_coverage_gap" in sub_dims


def test_build_geographic_comparison_writes_source_strata_rows():
    from enrichment.geographic import build_geographic_comparison

    cohort = pd.DataFrame([
        {"e_mec_code": "1", "inst_type": "federal_university", "region": "Sudeste", "faculty_with_phd": 100},
        {"e_mec_code": "2", "inst_type": "federal_university", "region": "Norte", "faculty_with_phd": 50},
        {"e_mec_code": "3", "inst_type": "private_university", "region": "Sudeste", "faculty_with_phd": 50},
    ])
    coverage = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "1", "n_records": 70},
        {"source": "openalex", "e_mec_code": "2", "n_records": 20},
        {"source": "openalex", "e_mec_code": "3", "n_records": 10},
        {"source": "scopus", "e_mec_code": "1", "n_records": 20},
        {"source": "scopus", "e_mec_code": "3", "n_records": 80},
    ])

    result = build_geographic_comparison(coverage, cohort)

    assert len(result) == 6
    assert set(result.columns) >= {
        "source",
        "inst_type",
        "region",
        "n_records",
        "source_publication_share",
        "peer_mean_share",
        "comparative_skew",
        "cohort_institution_share",
        "cohort_phd_faculty_share",
        "delta_vs_cohort_institution_share",
        "delta_vs_cohort_phd_faculty_share",
        "cohort_institutions",
    }


def test_build_geographic_comparison_includes_zero_record_source_strata():
    from enrichment.geographic import build_geographic_comparison

    cohort = pd.DataFrame([
        {"e_mec_code": "1", "inst_type": "federal_university", "region": "Sudeste", "faculty_with_phd": 100},
        {"e_mec_code": "2", "inst_type": "federal_university", "region": "Norte", "faculty_with_phd": 50},
    ])
    coverage = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "1", "n_records": 10},
        {"source": "openalex", "e_mec_code": "2", "n_records": 5},
        {"source": "scopus", "e_mec_code": "1", "n_records": 5},
    ])

    result = build_geographic_comparison(coverage, cohort)
    row = result[
        (result["source"] == "scopus") &
        (result["inst_type"] == "federal_university") &
        (result["region"] == "Norte")
    ].iloc[0]

    assert row["n_records"] == 0
    assert row["source_publication_share"] == 0.0


def test_build_geographic_comparison_centers_skew_on_peer_mean():
    from enrichment.geographic import build_geographic_comparison

    cohort = pd.DataFrame([
        {"e_mec_code": "1", "inst_type": "federal_university", "region": "Sudeste", "faculty_with_phd": 100},
        {"e_mec_code": "2", "inst_type": "federal_university", "region": "Norte", "faculty_with_phd": 50},
        {"e_mec_code": "3", "inst_type": "private_university", "region": "Sudeste", "faculty_with_phd": 50},
    ])
    coverage = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "1", "n_records": 70},
        {"source": "openalex", "e_mec_code": "2", "n_records": 20},
        {"source": "openalex", "e_mec_code": "3", "n_records": 10},
        {"source": "scopus", "e_mec_code": "1", "n_records": 20},
        {"source": "scopus", "e_mec_code": "3", "n_records": 80},
    ])

    result = build_geographic_comparison(coverage, cohort)
    grouped = result.groupby(["inst_type", "region"])["comparative_skew"].sum().round(8)

    assert (grouped == 0.0).all()
