# tests/enrichment/test_patents.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.patents import (
    has_patent_link, compute_patent_link_rate, build_patent_rows,
)

_PAPERS = pd.DataFrame([
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": ["US1234"]},
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": []},
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": None},
    {"source": "scopus",   "e_mec_code": "4925", "patent_citations": []},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])


def test_has_patent_link_true():
    assert has_patent_link(["US1234"]) is True


def test_has_patent_link_empty():
    assert has_patent_link([]) is False


def test_has_patent_link_none():
    assert has_patent_link(None) is False


def test_patent_link_rate():
    result = compute_patent_link_rate(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["patent_rate"] - 1/3) < 0.01


def test_build_rows_derwent_flag():
    result = compute_patent_link_rate(_PAPERS, _XW)
    rows = build_patent_rows(result)
    assert all(r["sub_dimension"] == "patent_link_rate" for r in rows)
