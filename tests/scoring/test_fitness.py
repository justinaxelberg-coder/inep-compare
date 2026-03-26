import pytest
from scoring.fitness import FitnessProfile, FitnessScorer, FitnessMatrix

MOCK_COVERAGE = {
    "openalex": {"federal_university": {"institutional_coverage": 0.95, "field_coverage": 0.85,
                  "temporal_coverage": 1.0, "language_coverage": 0.70, "record_count": 450,
                  "doi_coverage_rate": 0.90}},
    "scopus":   {"federal_university": {"institutional_coverage": 0.80, "field_coverage": 0.75,
                  "temporal_coverage": 1.0, "language_coverage": 0.50, "record_count": 300,
                  "doi_coverage_rate": 0.85}},
}
MOCK_OA = {
    "openalex": {"federal_university": {"oa_rate": 0.72, "diamond_rate": 0.15,
                  "unpaywall_agreement": 0.93}},
    "scopus":   {"federal_university": {"oa_rate": 0.55, "diamond_rate": 0.05,
                  "unpaywall_agreement": 0.80}},
}
MOCK_CONVERGENCE = {
    ("openalex", "scopus"): {"overlap_pct": 0.60, "divergence_pct": 0.05},
}

def test_fitness_profile_has_required_keys():
    scorer = FitnessScorer()
    profile = scorer.build_profile(
        source_id="openalex",
        inst_type="federal_university",
        coverage=MOCK_COVERAGE["openalex"]["federal_university"],
        oa=MOCK_OA["openalex"]["federal_university"],
        convergence=MOCK_CONVERGENCE,
    )
    assert set(profile.keys()) >= {"coverage", "data_quality", "reliability",
                                    "accessibility", "social_impact", "governance",
                                    "innovation_link", "composite"}

def test_composite_between_0_and_1():
    scorer = FitnessScorer()
    profile = scorer.build_profile("openalex", "federal_university",
                                    MOCK_COVERAGE["openalex"]["federal_university"],
                                    MOCK_OA["openalex"]["federal_university"],
                                    MOCK_CONVERGENCE)
    assert 0.0 <= profile["composite"] <= 1.0

def test_fitness_matrix_shape():
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(
        coverage_by_source_type=MOCK_COVERAGE,
        oa_by_source_type=MOCK_OA,
        convergence=MOCK_CONVERGENCE,
    )
    assert isinstance(matrix, FitnessMatrix)
    assert len(matrix.rows) >= 1

def test_ranking_per_inst_type():
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    ranking = matrix.rank_by_inst_type("federal_university")
    assert isinstance(ranking, list)
    assert len(ranking) == 2
    # openalex should rank first (better coverage, higher OA, better Barcelona score)
    assert ranking[0]["source"] == "openalex"

def test_open_sources_score_higher_accessibility():
    scorer = FitnessScorer()
    oa_profile = scorer.build_profile("openalex", "federal_university",
                                       MOCK_COVERAGE["openalex"]["federal_university"],
                                       MOCK_OA["openalex"]["federal_university"],
                                       MOCK_CONVERGENCE)
    prop_profile = scorer.build_profile("scopus", "federal_university",
                                         MOCK_COVERAGE["scopus"]["federal_university"],
                                         MOCK_OA["scopus"]["federal_university"],
                                         MOCK_CONVERGENCE)
    assert oa_profile["accessibility"] > prop_profile["accessibility"]

def test_fitness_matrix_to_records():
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    records = matrix.to_records()
    assert len(records) == 2
    assert all("source" in r and "composite" in r for r in records)

def test_missing_source_in_static_scores():
    """Sources not in static_scores.yaml should not crash — defaults to 0.5."""
    scorer = FitnessScorer()
    profile = scorer.build_profile("unknown_source", "federal_university",
                                    MOCK_COVERAGE["openalex"]["federal_university"],
                                    MOCK_OA["openalex"]["federal_university"],
                                    MOCK_CONVERGENCE)
    assert 0.0 <= profile["composite"] <= 1.0

def test_empty_patents_gives_zero_innovation():
    scorer = FitnessScorer()
    profile = scorer.build_profile("openalex", "federal_university",
                                    MOCK_COVERAGE["openalex"]["federal_university"],
                                    MOCK_OA["openalex"]["federal_university"],
                                    MOCK_CONVERGENCE, patents=None)
    assert profile["innovation_link"] == 0.0

def test_patents_increase_innovation_score():
    scorer = FitnessScorer()
    patents = {"patent_count": 20, "intl_patent_families": 5, "unique_npl_papers": 8}
    profile = scorer.build_profile("lens", "federal_university",
                                    MOCK_COVERAGE["openalex"]["federal_university"],
                                    MOCK_OA["openalex"]["federal_university"],
                                    MOCK_CONVERGENCE, patents=patents)
    assert profile["innovation_link"] > 0.0


# --- add these at the bottom of tests/scoring/test_fitness.py ---
from outputs.dataset.exporter import DatasetExporter
import pandas as pd
from pathlib import Path
from run_fitness import _resolve_run_id

def test_export_fitness_matrix_creates_csv(tmp_path):
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    exp    = DatasetExporter(output_dir=str(tmp_path))
    path   = exp.export_fitness_matrix(matrix, run_id="test")
    assert path.exists()
    df = pd.read_csv(path)
    assert "source" in df.columns
    assert "composite" in df.columns
    assert len(df) == 2

def test_export_fitness_report_creates_markdown(tmp_path):
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    exp    = DatasetExporter(output_dir=str(tmp_path))
    path   = exp.export_fitness_report(matrix, run_id="test")
    assert path.exists()
    content = path.read_text()
    assert "Source Fitness" in content
    assert "openalex" in content
    assert "Barcelona" in content

def test_export_fitness_matrix_sqlite(tmp_path):
    import sqlite3
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    exp    = DatasetExporter(output_dir=str(tmp_path))
    exp.export_fitness_matrix(matrix, run_id="test")
    db_path = tmp_path / "fitness_test.db"
    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM fitness_matrix").fetchall()
    assert len(rows) == 2

def test_dedup_score_wired():
    scorer = FitnessScorer()
    profile = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        dedup_score=0.85,
    )
    assert profile.data_quality > 0.0

def test_sdg_rate_raises_social_impact():
    scorer = FitnessScorer()
    base = dict(
        source_id="openalex", inst_type="federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
    )
    p_with = scorer.build_profile(**base, sdg_rate=0.80)
    p_without = scorer.build_profile(**base, sdg_rate=0.0)
    assert p_with.social_impact > p_without.social_impact

def test_diamond_oa_rate_wired():
    scorer = FitnessScorer()
    p = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        diamond_oa_rate=0.30,
    )
    assert 0.0 <= p.social_impact <= 1.0

def test_nonacademic_coauth_wired():
    scorer = FitnessScorer()
    base = dict(
        source_id="openalex", inst_type="federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
    )
    p_high = scorer.build_profile(**base, nonacademic_coauth=0.80)
    p_low  = scorer.build_profile(**base, nonacademic_coauth=0.0)
    assert p_high.innovation_link > p_low.innovation_link


def test_geography_not_accepted_as_profile_input():
    scorer = FitnessScorer()
    with pytest.raises(TypeError):
        scorer.build_profile(
            "openalex", "federal_university",
            coverage=MOCK_COVERAGE["openalex"]["federal_university"],
            oa=MOCK_OA["openalex"]["federal_university"],
            convergence=MOCK_CONVERGENCE,
            geographic_bias=0.25,
        )


def test_geographic_enrichment_does_not_change_composite():
    scorer = FitnessScorer()
    base = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    with_geo = scorer.build_matrix(
        MOCK_COVERAGE,
        MOCK_OA,
        MOCK_CONVERGENCE,
        enrichment={("openalex", "federal_university"): {"geographic_bias": 0.0}},
    )

    base_openalex = next(r for r in base.rows if r.source == "openalex")
    geo_openalex = next(r for r in with_geo.rows if r.source == "openalex")
    assert geo_openalex.composite == pytest.approx(base_openalex.composite)


def test_resolve_run_id_prefers_explicit_value():
    assert _resolve_run_id("2026-03-25-geo-reframe") == "2026-03-25-geo-reframe"
