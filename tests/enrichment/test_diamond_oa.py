from __future__ import annotations
import pandas as pd
import pytest
from enrichment.diamond_oa import classify_oa, enrich_oa_file, _DIAMOND_PATTERNS

def test_native_diamond_status():
    assert classify_oa("diamond", None) == "diamond"

def test_scielo_url_is_diamond():
    assert classify_oa("gold", "https://www.scielo.br/article/123") == "diamond"

def test_redalyc_url_is_diamond():
    assert classify_oa("green", "https://redalyc.org/pdf/123") == "diamond"

def test_gold_no_diamond_url():
    assert classify_oa("gold", "https://doi.org/10.1234/xyz") == "gold"

def test_closed_is_closed():
    assert classify_oa("closed", None) == "closed"

def test_none_status_is_closed():
    assert classify_oa(None, None) == "closed"

def test_enrich_oa_file_adds_oa_type_column(tmp_path):
    csv = tmp_path / "oa_phase2_2026-03-24.csv"
    df = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "1", "oa_rate": 0.5,
         "oa_status": "gold", "pdf_url": "https://scielo.br/abc"},
        {"source": "openalex", "e_mec_code": "1", "oa_rate": 0.5,
         "oa_status": "gold", "pdf_url": "https://doi.org/10.1"},
    ])
    df.to_csv(csv, index=False)
    enrich_oa_file(csv)
    result = pd.read_csv(csv)
    assert "oa_type" in result.columns
    assert result.iloc[0]["oa_type"] == "diamond"
    assert result.iloc[1]["oa_type"] == "gold"

def test_enrich_oa_file_idempotent(tmp_path):
    csv = tmp_path / "oa_phase2_test.csv"
    df = pd.DataFrame([{"source": "openalex", "oa_status": "gold",
                        "pdf_url": None, "oa_rate": 0.5}])
    df.to_csv(csv, index=False)
    enrich_oa_file(csv)
    enrich_oa_file(csv)
    result = pd.read_csv(csv)
    assert list(result.columns).count("oa_type") == 1
