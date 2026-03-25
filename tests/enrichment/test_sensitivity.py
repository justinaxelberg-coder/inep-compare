# tests/enrichment/test_sensitivity.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.sensitivity import (
    compute_sensitivity, aggregate_by_stratum, build_sensitivity_rows,
)

# coverage_df: source, e_mec_code, n_records
_COV = pd.DataFrame([
    {"source": "openalex",   "e_mec_code": "4925", "n_records": 800},
    {"source": "scopus",     "e_mec_code": "4925", "n_records": 560},
    {"source": "dimensions", "e_mec_code": "4925", "n_records": 620},
    {"source": "openalex",   "e_mec_code": "1810", "n_records": 450},
    {"source": "scopus",     "e_mec_code": "1810", "n_records": 180},
    {"source": "dimensions", "e_mec_code": "1810", "n_records": 200},
])

# crosswalk: e_mec_code, inst_type, region
_XW = pd.DataFrame([
    {"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"},
    {"e_mec_code": "1810", "inst_type": "federal_institute",  "region": "Sudeste"},
])


def test_sensitivity_ratio():
    result = compute_sensitivity(_COV, _XW)
    oa_fed = result[(result["source"] == "scopus") &
                    (result["inst_type"] == "federal_university")]
    assert abs(float(oa_fed["sensitivity"].iloc[0]) - 560/800) < 0.01


def test_openalex_sensitivity_is_one():
    result = compute_sensitivity(_COV, _XW)
    oa_rows = result[result["source"] == "openalex"]
    assert (oa_rows["sensitivity"] == 1.0).all()


def test_missing_e_mec_in_crosswalk_excluded():
    cov = _COV.copy()
    cov = pd.concat([cov, pd.DataFrame([{"source": "openalex", "e_mec_code": "9999", "n_records": 100}])])
    result = compute_sensitivity(cov, _XW)
    assert "9999" not in result["e_mec_code"].values


def test_aggregate_by_stratum():
    result = compute_sensitivity(_COV, _XW)
    agg = aggregate_by_stratum(result)
    assert "inst_type" in agg.columns
    assert "region" in agg.columns


def test_build_sensitivity_rows_schema():
    result = compute_sensitivity(_COV, _XW)
    agg = aggregate_by_stratum(result)
    rows = build_sensitivity_rows(agg)
    assert all("sub_dimension" in r for r in rows)
    assert all(r["sub_dimension"] == "sensitivity" for r in rows)


def test_no_openalex_returns_empty():
    cov_no_oa = _COV[_COV["source"] != "openalex"].copy()
    result = compute_sensitivity(cov_no_oa, _XW)
    assert result.empty
