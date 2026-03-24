# tests/enrichment/test_stratified.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.stratified import (
    STRATIFIED_SCHEMA, assign_confidence_tier, make_stratum_row,
    write_stratified_csv, load_stratified_csv,
)

def test_schema_columns():
    assert STRATIFIED_SCHEMA == [
        "source", "inst_type", "region", "sub_dimension",
        "value", "n_papers", "confidence_tier",
    ]

def test_confidence_tier_reliable():
    assert assign_confidence_tier(250) == "reliable"

def test_confidence_tier_moderate():
    assert assign_confidence_tier(75) == "moderate"

def test_confidence_tier_low():
    assert assign_confidence_tier(25) == "low"

def test_confidence_tier_insufficient():
    assert assign_confidence_tier(5) == "insufficient"

def test_make_stratum_row():
    row = make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312)
    assert row["value"] == 0.85
    assert row["confidence_tier"] == "reliable"

def test_write_and_load_roundtrip(tmp_path):
    rows = [
        make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312),
        make_stratum_row("scopus",   "isolated_faculty",   "Norte",   "sensitivity", 0.41,  47),
    ]
    path = tmp_path / "sensitivity_2026-03-24.csv"
    write_stratified_csv(rows, path)
    df = load_stratified_csv(path)
    assert len(df) == 2
    assert list(df.columns) == STRATIFIED_SCHEMA

def test_write_idempotent(tmp_path):
    rows = [make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312)]
    path = tmp_path / "test.csv"
    write_stratified_csv(rows, path)
    write_stratified_csv(rows, path)  # second write should overwrite, not append
    df = load_stratified_csv(path)
    assert len(df) == 1
