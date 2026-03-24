"""Tests for the Scopus connector — unit tests only (no live API calls)."""

import pytest
from connectors.api.scopus import ScopusConnector

MOCK_ENTRY = {
    "dc:identifier": "SCOPUS_ID:85123456789",
    "prism:doi": "10.1000/test.2022",
    "dc:title": "Test Paper",
    "prism:coverDate": "2022-06-01",
    "author": [{"authname": "Silva, J.", "afid": {"$": "60016835"}}],
    "affiliation": [{"affilname": "Universidade Federal do ABC"}],
    "subtypeDescription": "Article",
    "subtype": "ar",
    "citedby-count": "42",
    "openaccess": "1",
    "fund-sponsor": "CNPq",
    "fund-no": "123456",
    "prism:url": "https://api.elsevier.com/content/abstract/scopus_id/85123456789",
}


@pytest.fixture
def connector():
    return ScopusConnector(api_key="test_key", cache_dir="/tmp/test_scopus")


def test_normalize_schema(connector):
    result = connector.normalize(MOCK_ENTRY)
    required = [
        "source", "source_record_id", "doi", "title", "year",
        "authors", "institutions", "oa_status", "citation_count",
        "funding", "document_type", "retrieved_at",
    ]
    for field in required:
        assert field in result, f"Missing: {field}"


def test_normalize_doi_prefix(connector):
    result = connector.normalize(MOCK_ENTRY)
    assert result["doi"].startswith("https://doi.org/")


def test_normalize_oa_open(connector):
    result = connector.normalize(MOCK_ENTRY)
    assert result["oa_status"] == "gold"


def test_normalize_oa_closed(connector):
    entry = {**MOCK_ENTRY, "openaccess": "0"}
    result = connector.normalize(entry)
    assert result["oa_status"] == "closed"


def test_normalize_year(connector):
    result = connector.normalize(MOCK_ENTRY)
    assert result["year"] == 2022


def test_normalize_funding(connector):
    result = connector.normalize(MOCK_ENTRY)
    assert len(result["funding"]) == 1
    assert result["funding"][0]["funder"] == "CNPq"


def test_normalize_document_type(connector):
    result = connector.normalize(MOCK_ENTRY)
    assert result["document_type"] == "article"


def test_source_id(connector):
    assert connector.source_id == "scopus"
