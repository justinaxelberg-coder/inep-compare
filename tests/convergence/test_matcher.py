"""Tests for the convergence engine — unit tests only."""

import pytest
from convergence.matcher import (
    ConvergenceEngine,
    DivergenceFlag,
    MatchRecord,
    _normalise_doi,
    _normalise_title,
    _title_year_key,
)

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

REC_A1 = {
    "source_record_id": "oa_1",
    "doi": "https://doi.org/10.1000/test.2022",
    "title": "Methods in Brazilian Higher Education Research",
    "year": 2022,
    "authors": [{"name": "Silva, João"}],
}

REC_B1 = {
    "source_record_id": "sc_1",
    "doi": "10.1000/test.2022",          # bare DOI — same as A1
    "title": "Methods in Brazilian Higher Education Research",
    "year": 2022,
    "authors": [{"name": "Silva, J."}],
}

REC_A2 = {
    "source_record_id": "oa_2",
    "doi": None,
    "title": "Open Access and Science Communication in Brazil",
    "year": 2021,
    "authors": [{"name": "Santos, Maria"}],
}

REC_B2 = {
    "source_record_id": "sc_2",
    "doi": None,
    "title": "Open Access and Science Communication in Brazil",
    "year": 2021,
    "authors": [{"name": "Santos, M."}],
}

REC_A3 = {
    "source_record_id": "oa_3",
    "doi": None,
    "title": "Bibliometric Analysis of Environmental Sciences Publications",
    "year": 2023,
    "authors": [{"name": "Costa, Pedro"}],
}

REC_B3 = {
    "source_record_id": "sc_3",
    "doi": None,
    # Slightly different wording — should match fuzzy
    "title": "Bibliometric Analysis of Environmental Science Publications",
    "year": 2023,
    "authors": [{"name": "Costa, P."}],
}

REC_UNMATCHED = {
    "source_record_id": "sc_99",
    "doi": "10.9999/different.2020",
    "title": "Completely Unrelated Paper on Ancient History",
    "year": 2020,
    "authors": [{"name": "Other, Author"}],
}


@pytest.fixture
def engine():
    return ConvergenceEngine(source_ids=["openalex", "scopus"])


@pytest.fixture
def engine_three():
    return ConvergenceEngine(source_ids=["openalex", "scopus", "dimensions"])


# ------------------------------------------------------------------
# Helper normalisation
# ------------------------------------------------------------------

def test_normalise_doi_strips_url():
    assert _normalise_doi("https://doi.org/10.1000/test") == "10.1000/test"


def test_normalise_doi_strips_prefix():
    assert _normalise_doi("doi:10.1000/test") == "10.1000/test"


def test_normalise_doi_bare():
    assert _normalise_doi("10.1000/test") == "10.1000/test"


def test_normalise_doi_none():
    assert _normalise_doi(None) is None


def test_normalise_doi_invalid():
    assert _normalise_doi("not-a-doi") is None


def test_normalise_title_accents():
    result = _normalise_title("Análise Bibliométrica")
    assert "á" not in result
    assert "é" not in result


def test_normalise_title_lowercase():
    assert _normalise_title("Mixed Case TITLE") == "mixed case title"


def test_normalise_title_punctuation():
    result = _normalise_title("Title: Sub-title (2022).")
    assert ":" not in result
    assert "." not in result


def test_title_year_key_returns_tuple():
    key = _title_year_key(REC_A1)
    assert isinstance(key, tuple)
    assert len(key) == 3


def test_title_year_key_missing_year():
    rec = {**REC_A1, "year": None}
    assert _title_year_key(rec) is None


def test_title_year_key_missing_title():
    rec = {**REC_A1, "title": None}
    assert _title_year_key(rec) is None


# ------------------------------------------------------------------
# DOI matching (Level 1)
# ------------------------------------------------------------------

def test_doi_match(engine):
    matches = engine._match_pair([REC_A1], [REC_B1], "openalex", "scopus", "E001")
    assert len(matches) == 1
    assert matches[0].match_key == "doi"
    assert matches[0].confidence == 1.0
    assert not matches[0].flagged


def test_doi_match_normalises_url(engine):
    """Connector A stores full URL DOI, connector B stores bare DOI."""
    matches = engine._match_pair([REC_A1], [REC_B1], "openalex", "scopus", "E001")
    assert matches[0].doi_a == REC_A1["doi"]
    assert matches[0].doi_b == REC_B1["doi"]


# ------------------------------------------------------------------
# Title + year + author matching (Level 2)
# ------------------------------------------------------------------

def test_title_year_author_match(engine):
    matches = engine._match_pair([REC_A2], [REC_B2], "openalex", "scopus", "E001")
    assert len(matches) == 1
    assert matches[0].match_key == "title_year_author"
    assert matches[0].confidence == 0.85
    assert not matches[0].flagged


def test_level2_match_stores_correct_ids(engine):
    matches = engine._match_pair([REC_A2], [REC_B2], "openalex", "scopus", "E001")
    assert matches[0].record_id_a == "oa_2"
    assert matches[0].record_id_b == "sc_2"


# ------------------------------------------------------------------
# Fuzzy title matching (Level 3)
# ------------------------------------------------------------------

def test_fuzzy_title_match(engine):
    """Slight title variation should trigger fuzzy match."""
    matches = engine._match_pair([REC_A3], [REC_B3], "openalex", "scopus", "E001")
    assert len(matches) == 1
    assert matches[0].match_key == "fuzzy_title"
    assert matches[0].flagged is True


def test_fuzzy_match_confidence_range(engine):
    matches = engine._match_pair([REC_A3], [REC_B3], "openalex", "scopus", "E001")
    assert 0.0 < matches[0].confidence <= 1.0


def test_fuzzy_no_match_different_year(engine):
    """Same title but year differs by >1 should not fuzzy-match."""
    rec_a = {**REC_A3, "year": 2023}
    rec_b = {**REC_B3, "year": 2019}
    matches = engine._match_pair([rec_a], [rec_b], "openalex", "scopus", "E001")
    assert len(matches) == 0


def test_no_match_unrelated(engine):
    matches = engine._match_pair([REC_A1], [REC_UNMATCHED], "openalex", "scopus", "E001")
    assert len(matches) == 0


# ------------------------------------------------------------------
# No double-matching
# ------------------------------------------------------------------

def test_no_double_match(engine):
    """A record in B matched to A1 should not be reused for A2."""
    rec_a2_clone = {**REC_A1, "source_record_id": "oa_clone", "doi": None}
    matches = engine._match_pair(
        [REC_A1, rec_a2_clone], [REC_B1], "openalex", "scopus", "E001"
    )
    # Only one record in B — can only match once
    assert len(matches) == 1


# ------------------------------------------------------------------
# Divergence detection
# ------------------------------------------------------------------

def test_divergence_triggered(engine):
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test Inst",
        src_a="openalex", src_b="scopus",
        n_a=100, n_b=50,
    )
    assert div is not None
    assert div.discrepancy_pct == pytest.approx(0.5, abs=0.001)
    assert div.direction == "a_higher"


def test_divergence_not_triggered_within_threshold(engine):
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test Inst",
        src_a="openalex", src_b="scopus",
        n_a=100, n_b=90,   # 10% difference < 15% threshold
    )
    assert div is None


def test_divergence_complete_absence(engine):
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test Inst",
        src_a="openalex", src_b="scopus",
        n_a=10, n_b=0,
    )
    assert div is not None
    assert div.discrepancy_pct == pytest.approx(1.0)


def test_divergence_both_zero(engine):
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test Inst",
        src_a="openalex", src_b="scopus",
        n_a=0, n_b=0,
    )
    assert div is None


def test_divergence_direction_b_higher(engine):
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test Inst",
        src_a="openalex", src_b="scopus",
        n_a=40, n_b=100,
    )
    assert div.direction == "b_higher"


def test_divergence_custom_threshold():
    engine = ConvergenceEngine(source_ids=["a", "b"], divergence_threshold=0.30)
    div = engine._check_divergence(
        e_mec="E001", institution_name="Test",
        src_a="a", src_b="b",
        n_a=100, n_b=80,   # 20% — above 15% default but below 30%
    )
    assert div is None


# ------------------------------------------------------------------
# Full run — overlap matrix and review queue
# ------------------------------------------------------------------

def _make_records_by_source():
    return {
        "openalex": {
            "E001": [REC_A1, REC_A2, REC_A3],
        },
        "scopus": {
            "E001": [REC_B1, REC_B2, REC_B3],
        },
    }


def test_run_returns_expected_keys(engine):
    result = engine.run(_make_records_by_source())
    assert set(result.keys()) == {"match_table", "overlap_matrix", "divergences", "review_queue"}


def test_run_match_table_populated(engine):
    result = engine.run(_make_records_by_source())
    assert len(result["match_table"]) >= 2  # at least DOI + title matches


def test_run_review_queue_contains_flagged(engine):
    result = engine.run(_make_records_by_source())
    # Fuzzy match on A3/B3 should be in review queue
    assert len(result["review_queue"]) >= 1
    for m in result["review_queue"]:
        assert m.flagged is True


def test_run_overlap_matrix_has_entry(engine):
    result = engine.run(_make_records_by_source())
    matrix = result["overlap_matrix"]
    assert len(matrix) >= 1
    entry = matrix[0]
    assert "source_a" in entry
    assert "source_b" in entry
    assert "overlap_pct_min" in entry


def test_run_no_divergence_when_equal(engine):
    """Equal record counts should not trigger divergence."""
    result = engine.run(_make_records_by_source())
    assert len(result["divergences"]) == 0


def test_run_divergence_when_counts_differ():
    engine = ConvergenceEngine(source_ids=["openalex", "scopus"])
    records_by_source = {
        "openalex": {"E001": [REC_A1] * 10},
        "scopus":   {"E001": [REC_B1]},         # 10 vs 1 — large divergence
    }
    result = engine.run(records_by_source)
    assert len(result["divergences"]) >= 1


def test_run_three_sources(engine_three):
    records_by_source = {
        "openalex":   {"E001": [REC_A1, REC_A2]},
        "scopus":     {"E001": [REC_B1, REC_B2]},
        "dimensions": {"E001": [REC_A1, REC_B2]},  # shares records
    }
    result = engine_three.run(records_by_source)
    # 3 sources → 3 pairs → at least 3 overlap entries
    assert len(result["overlap_matrix"]) == 3


def test_run_empty_source(engine):
    """Missing source records should not crash the engine."""
    records_by_source = {
        "openalex": {"E001": [REC_A1, REC_A2]},
        "scopus":   {},  # no data for E001
    }
    result = engine.run(records_by_source)
    assert "match_table" in result


def test_run_e_mec_stored_in_match(engine):
    result = engine.run(_make_records_by_source())
    for m in result["match_table"]:
        assert m.e_mec_code == "E001"
