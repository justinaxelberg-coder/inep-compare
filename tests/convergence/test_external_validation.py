from convergence.external_validation import (
    external_corroboration_for_work,
    normalise_crossref_validation,
)


def test_normalise_crossref_validation_keeps_core_fields():
    payload = normalise_crossref_validation(
        {
            "doi": "10.1234/x",
            "document_type": "journal-article",
            "title": "Observed work title",
            "published_year": 2023,
            "ror_affiliation_present": True,
        }
    )
    assert payload["record_type"] == "journal_article"
    assert payload["title"] == "Observed work title"
    assert payload["year"] == 2023


def test_external_corroboration_flags_major_conflict_on_type():
    work = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    crossref = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "book"}
    result = external_corroboration_for_work(work, crossref)
    assert result["has_major_conflict"] is True


def test_external_corroboration_flags_major_conflict_on_title_mismatch():
    work = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    crossref = {"doi": "10.1234/x", "title": "Different work title", "year": 2023, "record_type": "journal_article"}
    result = external_corroboration_for_work(work, crossref)
    assert result["has_major_conflict"] is True
    assert "title" in result["conflict_fields"]


def test_external_corroboration_ignores_minor_title_variance():
    work = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    crossref = {
        "doi": "10.1234/x",
        "title": "Observed work title: extended subtitle",
        "year": 2023,
        "record_type": "journal_article",
    }
    result = external_corroboration_for_work(work, crossref)
    assert result["has_major_conflict"] is False
    assert "title" not in result["conflict_fields"]


def test_external_corroboration_normalizes_doi_variants():
    work = {"doi": "https://doi.org/10.1234/X", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    crossref = {"doi": "doi:10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    result = external_corroboration_for_work(work, crossref)
    assert result["has_external_corroboration"] is True
