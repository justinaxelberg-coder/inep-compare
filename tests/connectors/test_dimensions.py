"""Tests for the Dimensions connector — unit tests only (no live API calls)."""

import pytest
from connectors.api.dimensions import DimensionsConnector

# open_access is a flat list of category strings in the real Dimensions API
# times_cited replaces citations_count; language is not returned by the API
MOCK_PUB = {
    "id": "pub.1234567890",
    "doi": "10.1000/dim.2022",
    "title": "Test Dimensions Paper",
    "year": 2022,
    "document_type": "article",
    "times_cited": 15,
    "field_citation_ratio": 1.8,
    "open_access": ["oa_all", "gold"],
    "authors": [
        {
            "last_name": "Santos",
            "first_name": "Maria",
            "orcid": "0000-0001-2345-6789",
            "affiliations": [{"name": "Universidade Federal do Pará"}],
        }
    ],
    "research_orgs": [{"name": "Universidade Federal do Pará", "id": "grid.271300.7"}],
    "funders": [{"name": "FAPESP", "id": "funder.1234", "grant_number": "2021/12345-6"}],
    "category_sdg": [{"id": "sdg/3", "name": "Good Health and Well-Being"}],
}


@pytest.fixture
def connector():
    return DimensionsConnector(api_key="test_key", cache_dir="/tmp/test_dim")


def test_normalize_schema(connector):
    result = connector.normalize(MOCK_PUB)
    required = [
        "source", "source_record_id", "doi", "title", "year",
        "authors", "institutions", "oa_status", "citation_count",
        "fwci", "funding", "sdgs", "document_type", "retrieved_at",
    ]
    for field in required:
        assert field in result, f"Missing: {field}"


def test_normalize_doi_prefix(connector):
    result = connector.normalize(MOCK_PUB)
    assert result["doi"].startswith("https://doi.org/")


def test_normalize_oa_gold(connector):
    result = connector.normalize(MOCK_PUB)
    assert result["oa_status"] == "gold"


def test_normalize_oa_closed(connector):
    pub = {**MOCK_PUB, "open_access": ["closed"]}
    result = connector.normalize(pub)
    assert result["oa_status"] == "closed"


def test_normalize_oa_diamond(connector):
    pub = {**MOCK_PUB, "open_access": ["oa_all", "diamond"]}
    result = connector.normalize(pub)
    assert result["oa_status"] == "diamond"


def test_normalize_sdgs(connector):
    result = connector.normalize(MOCK_PUB)
    assert len(result["sdgs"]) == 1
    assert result["sdgs"][0]["display_name"] == "Good Health and Well-Being"


def test_normalize_fwci(connector):
    result = connector.normalize(MOCK_PUB)
    assert result["fwci"] == 1.8


def test_normalize_funding(connector):
    result = connector.normalize(MOCK_PUB)
    assert result["funding"][0]["funder"] == "FAPESP"
    assert result["funding"][0]["grant_number"] == "2021/12345-6"


def test_normalize_language(connector):
    # Dimensions API does not return a language field
    result = connector.normalize(MOCK_PUB)
    assert result["language"] is None


def test_source_id(connector):
    assert connector.source_id == "dimensions"
