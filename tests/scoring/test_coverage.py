"""
Tests for the coverage scorer.
"""

import pytest
from scoring.coverage import CoverageScorer, CoverageResult
from scoring.confidence import ConfidenceTier

INSTITUTION = {
    "e_mec_code": "000572",
    "name": "Universidade Federal do ABC",
    "category": "universidade",
    "org_type": "federal",
    "region": "Sudeste",
}


def make_records(n: int, language: str = "en", fields: list = None) -> list[dict]:
    return [
        {
            "source": "openalex",
            "doi": f"10.1000/test{i}",
            "title": f"Test paper {i}",
            "year": 2022,
            "language": language,
            "fields": fields or ["Computer Science", "Mathematics"],
            "oa_status": "gold" if i % 2 == 0 else "closed",
        }
        for i in range(n)
    ]


def test_empty_records():
    scorer = CoverageScorer(source="openalex")
    result = scorer.score(records=[], institution=INSTITUTION)
    assert result.institution_present is False
    assert result.n_records == 0


def test_institution_present():
    scorer = CoverageScorer(source="openalex")
    records = make_records(50)
    result = scorer.score(records=records, institution=INSTITUTION)
    assert result.institution_present is True
    assert result.n_records == 50


def test_language_coverage_portuguese():
    scorer = CoverageScorer(source="openalex")
    records = make_records(200, language="pt")
    result = scorer.score(records=records, institution=INSTITUTION)
    assert result.language_coverage_estimate is not None
    assert result.language_coverage_estimate.estimate == pytest.approx(1.0)


def test_language_coverage_english_only():
    scorer = CoverageScorer(source="openalex")
    records = make_records(200, language="en")
    result = scorer.score(records=records, institution=INSTITUTION)
    assert result.language_coverage_estimate.estimate == pytest.approx(0.0)


def test_extensao_always_false():
    scorer = CoverageScorer(source="openalex")
    records = make_records(100)
    result = scorer.score(records=records, institution=INSTITUTION)
    assert result.extensao_coverage is False


def test_capes_area_detection():
    scorer = CoverageScorer(source="openalex")
    records = make_records(50, fields=["medicine", "physics"])
    result = scorer.score(records=records, institution=INSTITUTION)
    areas = result.capes_areas_found
    assert "Ciências da Saúde" in areas
    assert "Ciências Exatas e da Terra" in areas


def test_oa_count():
    scorer = CoverageScorer(source="openalex")
    records = make_records(100)  # 50 gold, 50 closed
    result = scorer.score(records=records, institution=INSTITUTION)
    assert result.n_oa == 50


def test_institutional_coverage_summary():
    scorer = CoverageScorer(source="openalex")
    institutions = [INSTITUTION, {**INSTITUTION, "e_mec_code": "000097", "name": "UNIFESP"}]

    # First institution has records, second doesn't
    results = [
        scorer.score(records=make_records(100), institution=institutions[0]),
        scorer.score(records=[], institution=institutions[1]),
    ]
    summary = scorer.institutional_coverage_summary(results)
    # 1 out of 2 present — but N=2 is insufficient tier
    assert summary.tier == ConfidenceTier.INSUFFICIENT


def test_as_dict_keys():
    scorer = CoverageScorer(source="openalex")
    records = make_records(200)
    result = scorer.score(records=records, institution=INSTITUTION)
    d = result.as_dict()
    assert "source" in d
    assert "e_mec_code" in d
    assert "institution_present" in d
    assert "n_records" in d
    assert "extensao_coverage" in d
