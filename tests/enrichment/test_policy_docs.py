# tests/enrichment/test_policy_docs.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.policy_docs import (
    is_policy_document, compute_policy_rates, build_policy_rows,
    POLICY_DOC_TYPES,
)


def test_policy_types_populated():
    assert len(POLICY_DOC_TYPES) >= 3


def test_is_policy_report():
    assert is_policy_document("policy_report") is True


def test_is_not_policy_article():
    assert is_policy_document("journal-article") is False


def test_is_none_not_policy():
    assert is_policy_document(None) is False


_PAPERS = pd.DataFrame([
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "policy_report"},
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "journal-article"},
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "working_paper"},
    {"source": "openalex",   "e_mec_code": "4925", "document_type": "article"},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])


def test_policy_rate_dimensions():
    result = compute_policy_rates(_PAPERS, _XW)
    dim = result[result["source"] == "dimensions"].iloc[0]
    assert abs(dim["policy_rate"] - 2/3) < 0.01


def test_policy_rate_openalex_zero():
    result = compute_policy_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert oa["policy_rate"] == 0.0


def test_build_rows_flagged_overton_pending():
    result = compute_policy_rates(_PAPERS, _XW)
    rows = build_policy_rows(result)
    assert all(r["sub_dimension"] == "policy_document_rate" for r in rows)
