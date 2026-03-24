# tests/enrichment/test_sdg.py
from __future__ import annotations
import json
import pytest
from enrichment.sdg import (
    compute_sdg_rates, compute_sdg_agreement, write_sdg_flag, SDG_LABELS,
)

_PAPERS_OA = [
    {"id": "W1", "sdgs": [3, 4]},
    {"id": "W2", "sdgs": [4]},
    {"id": "W3", "sdgs": []},
]

def test_sdg_rates_goal_present():
    rates = compute_sdg_rates(_PAPERS_OA)
    assert abs(rates[4]["rate"] - 2/3) < 0.01
    assert rates[3]["rate"] == pytest.approx(1/3)

def test_sdg_rates_missing_goal_is_zero():
    rates = compute_sdg_rates(_PAPERS_OA)
    assert rates.get(1, {}).get("rate", 0.0) == 0.0

def test_sdg_rates_all_goals_present():
    rates = compute_sdg_rates(_PAPERS_OA)
    assert 3 in rates and 4 in rates

def test_sdg_agreement_on_matched():
    matched = [{"id_a": "W1", "id_b": "D1"}]
    oa_map = {"W1": {3, 4}, "W2": {4}}
    dim_map = {"D1": {3}, "D2": {4, 10}}
    agreement = compute_sdg_agreement(matched, oa_map, dim_map)
    assert agreement[3]["agreement_rate"] == pytest.approx(1.0)

def test_sdg_agreement_empty_matched():
    agreement = compute_sdg_agreement([], {}, {})
    assert agreement == {}

def test_sdg_labels_covers_all_goals():
    for g in range(1, 18):
        assert g in SDG_LABELS, f"SDG goal {g} missing from SDG_LABELS"

def test_write_sdg_flag_creates_file(tmp_path):
    path = tmp_path / "source_metadata.json"
    write_sdg_flag(path, "scopus", available=False)
    data = json.loads(path.read_text())
    assert data["scopus"]["sdg_available"] is False

def test_write_sdg_flag_merges(tmp_path):
    path = tmp_path / "source_metadata.json"
    path.write_text('{"openalex": {"sdg_available": true}}')
    write_sdg_flag(path, "scopus", available=False)
    data = json.loads(path.read_text())
    assert data["openalex"]["sdg_available"] is True
    assert data["scopus"]["sdg_available"] is False
