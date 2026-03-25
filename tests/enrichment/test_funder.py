# tests/enrichment/test_funder.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.funder import (
    is_brazilian_funder, compute_funder_rates, build_funder_rows,
    BR_FUNDER_KEYWORDS,
)


def test_br_funder_keywords_present():
    assert "cnpq" in BR_FUNDER_KEYWORDS
    assert "capes" in BR_FUNDER_KEYWORDS
    assert "fapesp" in BR_FUNDER_KEYWORDS
    assert "finep" in BR_FUNDER_KEYWORDS


def test_is_br_funder_cnpq():
    assert is_brazilian_funder("Conselho Nacional de Desenvolvimento Científico") is True


def test_is_br_funder_foreign():
    assert is_brazilian_funder("National Institutes of Health") is False


def test_is_br_funder_empty():
    assert is_brazilian_funder("") is False


_PAPERS = pd.DataFrame([
    {"source": "openalex", "e_mec_code": "4925",
     "funding": [{"funder": "CNPq", "funder_id": None, "funder_ror": None}]},
    {"source": "openalex", "e_mec_code": "4925",
     "funding": [{"funder": "NIH",  "funder_id": None, "funder_ror": None}]},
    {"source": "openalex", "e_mec_code": "4925", "funding": []},
    {"source": "scopus",   "e_mec_code": "4925", "funding": []},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])


def test_funder_rate_openalex():
    result = compute_funder_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["funder_rate"] - 2/3) < 0.01


def test_br_funder_rate():
    result = compute_funder_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["br_funder_rate"] - 1/3) < 0.01


def test_build_rows_produces_two_sub_dims():
    result = compute_funder_rates(_PAPERS, _XW)
    rows = build_funder_rows(result)
    sub_dims = {r["sub_dimension"] for r in rows}
    assert "funder_metadata_rate" in sub_dims
    assert "br_funder_rate" in sub_dims
