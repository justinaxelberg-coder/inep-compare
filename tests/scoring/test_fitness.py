import pytest
from scoring.fitness import FitnessProfile, FitnessScorer, FitnessMatrix

MOCK_COVERAGE = {
    "openalex": {"federal_university": {"institution_coverage": 0.95, "field_coverage": 0.85,
                  "temporal_coverage": 1.0, "language_coverage": 0.70, "record_count": 450,
                  "doi_coverage_rate": 0.90}},
    "scopus":   {"federal_university": {"institution_coverage": 0.80, "field_coverage": 0.75,
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
