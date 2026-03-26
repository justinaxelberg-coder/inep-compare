from __future__ import annotations
import sqlite3
import pandas as pd
import pytest
from pathlib import Path

from dashboard.data_loader import (
    load_fitness_profiles,
    load_convergence,
    load_registry,
    load_geographic,
    load_enrichment_combined,
    load_source_reliability_summary,
    load_source_reliability_flags,
    FITNESS_COLUMNS,
    CONVERGENCE_COLUMNS,
    DIVERGENCE_COLUMNS,
    REGISTRY_COLUMNS,
    STRATIFIED_SCHEMA,
    GEOGRAPHIC_COLUMNS,
    SOURCE_RELIABILITY_SUMMARY_COLUMNS,
    SOURCE_RELIABILITY_FLAG_COLUMNS,
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


@pytest.fixture
def geographic_csv(tmp_path):
    df = pd.DataFrame([{
        "source": "openalex",
        "inst_type": "federal_university",
        "region": "Sudeste",
        "n_records": 70,
        "source_publication_share": 0.70,
        "peer_mean_share": 0.45,
        "comparative_skew": 0.25,
        "cohort_institution_share": 0.33,
        "cohort_phd_faculty_share": 0.50,
        "delta_vs_cohort_institution_share": 0.37,
        "delta_vs_cohort_phd_faculty_share": 0.20,
        "cohort_institutions": 1,
    }])
    path = tmp_path / "geographic_coverage_2026-03-25.csv"
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


def test_fitness_csv_fallback_source_name(fitness_csv):
    df = load_fitness_profiles(
        db_path=fitness_csv / "nonexistent.db",
        csv_dir=fitness_csv,
    )
    assert df.iloc[0]["source"] == "scopus"


def test_fitness_empty_returns_dataframe(tmp_path):
    df = load_fitness_profiles(db_path=tmp_path / "x.db", csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)


def test_fitness_empty_has_correct_columns(tmp_path):
    df = load_fitness_profiles(db_path=tmp_path / "x.db", csv_dir=tmp_path)
    assert list(df.columns) == FITNESS_COLUMNS


def test_fitness_empty_has_zero_rows(tmp_path):
    df = load_fitness_profiles(db_path=tmp_path / "x.db", csv_dir=tmp_path)
    assert len(df) == 0


# --- load_convergence ---

def test_convergence_loads_overlap(overlap_csv):
    overlap, _ = load_convergence(csv_dir=overlap_csv)
    assert len(overlap) >= 1


def test_convergence_loads_divergences(overlap_csv):
    _, divs = load_convergence(csv_dir=overlap_csv)
    assert len(divs) >= 1


def test_convergence_overlap_has_required_columns(overlap_csv):
    overlap, _ = load_convergence(csv_dir=overlap_csv)
    assert set(CONVERGENCE_COLUMNS).issubset(overlap.columns)


def test_convergence_empty_overlap_is_dataframe(tmp_path):
    overlap, _ = load_convergence(csv_dir=tmp_path)
    assert isinstance(overlap, pd.DataFrame)


def test_convergence_empty_divs_is_dataframe(tmp_path):
    _, divs = load_convergence(csv_dir=tmp_path)
    assert isinstance(divs, pd.DataFrame)


def test_convergence_empty_overlap_has_zero_rows(tmp_path):
    overlap, _ = load_convergence(csv_dir=tmp_path)
    assert len(overlap) == 0


def test_convergence_empty_divs_has_zero_rows(tmp_path):
    _, divs = load_convergence(csv_dir=tmp_path)
    assert len(divs) == 0


# --- load_registry ---

def test_registry_loads_correct_count(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    assert len(df) == 1


def test_registry_loads_sinaes_type(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    assert df.iloc[0]["sinaes_type"] == "federal_university"


def test_registry_has_required_columns(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    assert set(REGISTRY_COLUMNS).issubset(df.columns)


def test_registry_empty_is_dataframe(tmp_path):
    df = load_registry(csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)


def test_registry_empty_has_zero_rows(tmp_path):
    df = load_registry(csv_dir=tmp_path)
    assert len(df) == 0


# --- load_geographic ---

def test_geographic_loads_comparison_schema(geographic_csv):
    df = load_geographic(csv_dir=geographic_csv)
    assert set(GEOGRAPHIC_COLUMNS).issubset(df.columns)
    assert df.iloc[0]["comparative_skew"] == 0.25


def test_combined_loader_excludes_geography(geographic_csv):
    df = load_enrichment_combined(csv_dir=geographic_csv)
    assert df.empty
    assert list(df.columns) == STRATIFIED_SCHEMA


# --- load_source_reliability_summary ---

def test_load_source_reliability_summary_empty_has_expected_columns(tmp_path):
    df = load_source_reliability_summary(csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == SOURCE_RELIABILITY_SUMMARY_COLUMNS
    assert len(df) == 0


def test_load_source_reliability_summary_reads_latest(tmp_path):
    older = pd.DataFrame([{
        "source": "openalex",
        "record_type": "__all__",
        "canonical_works": 1,
        "integration_ready_share": 1.0,
    }])
    newer = pd.DataFrame([{
        "source": "scopus",
        "record_type": "__all__",
        "canonical_works": 2,
        "integration_ready_share": 0.5,
    }])
    older.to_csv(tmp_path / "source_reliability_summary_2026-03-24.csv", index=False)
    newer.to_csv(tmp_path / "source_reliability_summary_2026-03-25.csv", index=False)

    df = load_source_reliability_summary(csv_dir=tmp_path)

    assert len(df) == 1
    assert df.iloc[0]["source"] == "scopus"


# --- load_source_reliability_flags ---

def test_load_source_reliability_flags_empty_has_expected_columns(tmp_path):
    df = load_source_reliability_flags(csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == SOURCE_RELIABILITY_FLAG_COLUMNS
    assert len(df) == 0


def test_load_source_reliability_flags_reads_latest(tmp_path):
    older = pd.DataFrame([{
        "source": "openalex",
        "record_type": "journal_article",
        "flag": "major_conflict",
        "n_works": 1,
        "denominator": 2,
        "share": 0.5,
    }])
    newer = pd.DataFrame([{
        "source": "scopus",
        "record_type": "thesis",
        "flag": "doi_expected_missing",
        "n_works": 2,
        "denominator": 4,
        "share": 0.5,
    }])
    older.to_csv(tmp_path / "source_reliability_flags_2026-03-24.csv", index=False)
    newer.to_csv(tmp_path / "source_reliability_flags_2026-03-25.csv", index=False)

    df = load_source_reliability_flags(csv_dir=tmp_path)

    assert len(df) == 1
    assert df.iloc[0]["flag"] == "doi_expected_missing"
