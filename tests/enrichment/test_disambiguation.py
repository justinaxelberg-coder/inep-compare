# tests/enrichment/test_disambiguation.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.disambiguation import (
    compute_disambiguation_rate, build_disambiguation_rows,
)

_PAPERS = [
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": True},
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": True},
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": False},
    {"source": "scopus",   "e_mec_code": "4925", "ror_resolved": False},
    {"source": "scopus",   "e_mec_code": "4925", "ror_resolved": False},
]
_XW = pd.DataFrame([
    {"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"},
])


def test_disambiguation_rate_openalex():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    oa_row = result[(result["source"] == "openalex") &
                    (result["inst_type"] == "federal_university")]
    assert abs(float(oa_row["ror_rate"].iloc[0]) - 2/3) < 0.01


def test_disambiguation_rate_scopus_zero():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    sc_row = result[(result["source"] == "scopus") &
                    (result["inst_type"] == "federal_university")]
    assert float(sc_row["ror_rate"].iloc[0]) == 0.0


def test_build_rows_schema():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    rows = build_disambiguation_rows(result)
    assert all(r["sub_dimension"] == "disambiguation_quality" for r in rows)
    assert all(0.0 <= r["value"] <= 1.0 for r in rows)


def test_empty_papers_returns_empty():
    result = compute_disambiguation_rate(pd.DataFrame(), _XW)
    assert result.empty


def test_missing_ror_field_treated_as_false():
    papers = pd.DataFrame([{"source": "openalex", "e_mec_code": "4925"}])
    result = compute_disambiguation_rate(papers, _XW)
    assert float(result.iloc[0]["ror_rate"]) == 0.0


def test_confidence_tier_on_small_n():
    rows = build_disambiguation_rows(pd.DataFrame([{
        "source": "scopus", "inst_type": "isolated_faculty",
        "region": "Norte", "ror_rate": 0.3, "n_papers": 8,
    }]))
    assert rows[0]["confidence_tier"] == "insufficient"
