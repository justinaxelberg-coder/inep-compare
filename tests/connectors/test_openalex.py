"""
Tests for the OpenAlex connector.

Uses pytest-httpx to mock HTTP responses — no live API calls in CI.
Live integration tests require OPENALEX_EMAIL env var and are marked separately.
"""

import pytest
import httpx

from connectors.api.openalex import OpenAlexConnector

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

KNOWN_UFABC_ROR = "https://ror.org/01v9rjf43"
KNOWN_DOI = "https://doi.org/10.1038/s41586-020-2649-2"  # used for schema validation

MOCK_WORK = {
    "id": "https://openalex.org/W2741809807",
    "doi": "https://doi.org/10.1038/s41586-020-2649-2",
    "title": "Array programming with NumPy",
    "publication_year": 2022,
    "type": "article",
    "language": "en",
    "cited_by_count": 100,
    "authorships": [
        {
            "author": {"display_name": "Test Author", "orcid": None},
            "institutions": [
                {"display_name": "Universidade Federal do ABC",
                 "ror": "https://ror.org/01v9rjf43"}
            ],
        }
    ],
    "open_access": {"oa_status": "gold", "oa_url": "https://example.com/paper.pdf"},
    "primary_location": {"source": {}, "license": "cc-by"},
    "topics": [{"display_name": "Computer Science", "score": 0.9}],
    "grants": [],
}

MOCK_RESPONSE = {
    "results": [MOCK_WORK],
    "meta": {"count": 1, "next_cursor": None},
}


# ------------------------------------------------------------------
# Unit tests (mocked HTTP)
# ------------------------------------------------------------------

def test_normalize_schema():
    """Normalised record must conform to the common schema."""
    connector = OpenAlexConnector(email="test@example.com", cache_dir="/tmp/test_cache")
    result = connector.normalize(MOCK_WORK)

    required_fields = [
        "source", "source_record_id", "doi", "title", "year",
        "authors", "institutions", "e_mec_codes",
        "fields", "primary_topic", "primary_field", "primary_domain",
        "keywords", "document_type", "language",
        "oa_status", "oa_url", "licence",
        "citation_count", "fwci", "funding", "sdgs",
        "patent_citations", "source_url", "retrieved_at",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_normalize_oa_status():
    connector = OpenAlexConnector(email="test@example.com", cache_dir="/tmp/test_cache")
    result = connector.normalize(MOCK_WORK)
    assert result["oa_status"] == "gold"


def test_normalize_empty_grants():
    connector = OpenAlexConnector(email="test@example.com", cache_dir="/tmp/test_cache")
    result = connector.normalize(MOCK_WORK)
    assert result["funding"] == []


def test_normalize_concept_threshold():
    """Topics below score 0.3 should be filtered."""
    work = {**MOCK_WORK, "topics": [
        {"display_name": "Physics", "score": 0.8},
        {"display_name": "Noise", "score": 0.1},   # below threshold
    ]}
    connector = OpenAlexConnector(email="test@example.com", cache_dir="/tmp/test_cache")
    result = connector.normalize(work)
    assert "Physics" in result["fields"]
    assert "Noise" not in result["fields"]


def test_source_id():
    connector = OpenAlexConnector(email="test@example.com", cache_dir="/tmp/test_cache")
    assert connector.source_id == "openalex"


def test_no_email_warning(caplog):
    import logging
    with caplog.at_level(logging.WARNING):
        OpenAlexConnector(cache_dir="/tmp/test_cache")
    assert "OPENALEX_EMAIL" in caplog.text


# ------------------------------------------------------------------
# Integration tests — require live API (skipped in CI)
# ------------------------------------------------------------------

@pytest.mark.integration
def test_live_ufabc_fetch():
    """
    Live test: fetch UFABC records from OpenAlex.
    Requires OPENALEX_EMAIL env var.
    """
    import os
    email = os.environ.get("OPENALEX_EMAIL")
    if not email:
        pytest.skip("OPENALEX_EMAIL not set")

    connector = OpenAlexConnector(email=email, max_records=50, cache_dir="/tmp/test_cache")
    records = connector.query_institution(
        e_mec_code="000572",
        ror_id=KNOWN_UFABC_ROR,
        start_year=2022,
        end_year=2023,
        use_cache=False,
    )

    assert len(records) > 0, "UFABC should have records in OpenAlex"
    assert all(r["source"] == "openalex" for r in records)
    assert all("doi" in r for r in records)
