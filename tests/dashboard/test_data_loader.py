from __future__ import annotations
import sqlite3
import pandas as pd
import pytest
from pathlib import Path

import sys
sys.path.insert(0, ".")

from dashboard.data_loader import (
    load_fitness_profiles,
    load_convergence,
    load_registry,
    FITNESS_COLUMNS,
    CONVERGENCE_COLUMNS,
    REGISTRY_COLUMNS,
)


# --- fixtures ---

@pytest.fixture
def fitness_db(tmp_path):
    db = tmp_path / "fitness.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE fitness_profiles "
        "(source TEXT, inst_type TEXT, coverage REAL, data_quality REAL, "
        "reliability REAL, accessibility REAL, social_impact REAL, "
        "governance REAL, innovation_link REAL, composite REAL)"
    )
    conn.execute(
        "INSERT INTO fitness_profiles VALUES "
        "('openalex','federal_university',0.17,0.29,0.60,0.99,0.25,0.90,0.0,0.49)"
    )
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def fitness_csv(tmp_path):
    df = pd.DataFrame([{
        "source": "scopus", "inst_type": "federal_university",
        "coverage": 0.17, "data_quality": 0.31, "reliability": 0.47,
        "accessibility": 0.33, "social_impact": 0.24, "governance": 0.43,
        "innovation_link": 0.0, "composite": 0.30,
    }])
    path = tmp_path / "fitness_matrix_2026-03-24.csv"
    df.to_csv(path, index=False)
    return tmp_path


@pytest.fixture
def overlap_csv(tmp_path):
    df = pd.DataFrame([{
        "source_a": "openalex", "source_b": "scopus",
        "e_mec_code": "1982", "n_a": 500, "n_b": 225,
        "n_matched": 155, "overlap_pct_a": 0.31,
        "overlap_pct_b": 0.69, "overlap_pct_min": 0.31,
    }])
    path = tmp_path / "overlap_phase2_2026-03-23.csv"
    df.to_csv(path, index=False)

    div_df = pd.DataFrame([{
        "e_mec_code": "1982", "institution_name": "Inst A",
        "source_a": "openalex", "source_b": "scopus",
        "count_a": 500, "count_b": 225,
        "discrepancy_pct": 0.55, "direction": "a_higher",
    }])
    div_df.to_csv(tmp_path / "divergences_phase2_2026-03-23.csv", index=False)
    return tmp_path


@pytest.fixture
def registry_csv(tmp_path):
    df = pd.DataFrame([{
        "e_mec_code": "000001", "name": "UFMT", "abbreviation": "UFMT",
        "city": "Cuiabá", "state": "MT", "faculty_with_phd": 1263,
        "faculty_total": 1588, "org_type": "federal", "category": "universidade",
        "region": "Centro-Oeste", "sinaes_type": "federal_university",
        "phd_faculty_share": 0.80, "censo_year": 2023,
    }])
    path = tmp_path / "institutions.csv"
    df.to_csv(path, index=False)
    return tmp_path


# --- load_fitness_profiles ---

def test_fitness_loads_from_sqlite(fitness_db, tmp_path):
    df = load_fitness_profiles(db_path=fitness_db, csv_dir=tmp_path)
    assert len(df) == 1


def test_fitness_returns_correct_columns_from_sqlite(fitness_db, tmp_path):
    df = load_fitness_profiles(db_path=fitness_db, csv_dir=tmp_path)
    assert set(FITNESS_COLUMNS).issubset(df.columns)


def test_fitness_falls_back_to_csv_when_db_absent(fitness_csv):
    df = load_fitness_profiles(
        db_path=fitness_csv / "nonexistent.db",
        csv_dir=fitness_csv,
    )
    assert len(df) == 1
    assert df.iloc[0]["source"] == "scopus"


def test_fitness_returns_empty_df_when_nothing_present(tmp_path):
    df = load_fitness_profiles(db_path=tmp_path / "x.db", csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == FITNESS_COLUMNS
    assert len(df) == 0


# --- load_convergence ---

def test_convergence_loads_overlap_and_divergences(overlap_csv):
    overlap, divs = load_convergence(csv_dir=overlap_csv)
    assert len(overlap) >= 1
    assert len(divs) >= 1


def test_convergence_overlap_has_required_columns(overlap_csv):
    overlap, _ = load_convergence(csv_dir=overlap_csv)
    for col in CONVERGENCE_COLUMNS:
        assert col in overlap.columns


def test_convergence_returns_empty_dfs_when_absent(tmp_path):
    overlap, divs = load_convergence(csv_dir=tmp_path)
    assert isinstance(overlap, pd.DataFrame)
    assert isinstance(divs, pd.DataFrame)
    assert len(overlap) == 0
    assert len(divs) == 0


# --- load_registry ---

def test_registry_loads_institutions(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    assert len(df) == 1
    assert df.iloc[0]["sinaes_type"] == "federal_university"


def test_registry_has_required_columns(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    for col in REGISTRY_COLUMNS:
        assert col in df.columns


def test_registry_returns_empty_df_when_absent(tmp_path):
    df = load_registry(csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
