"""Tests for The Lens connector — unit tests using mocked API responses."""

import pytest
from unittest.mock import MagicMock, patch

from connectors.api.lens import LensConnector, _bare_ror


# ------------------------------------------------------------------
# Raw fixtures — scholarly
# ------------------------------------------------------------------

RAW_SCHOLARLY = {
    "lens_id": "123-456-789",
    "doi": "10.1234/lens.test.2023",
    "title": "Machine Learning for Biodiversity Monitoring in the Amazon",
    "year_published": 2023,
    "date_published": "2023-06-15",
    "authors": [
        {
            "display_name": "Silva, Ana",
            "orcid": "0000-0001-2345-6789",
            "affiliations": [
                {
                    "name": "UFPA",
                    "institution": {
                        "name": "Universidade Federal do Pará",
                        "ror_id": "03q9sr818",
                    },
                    "country_code": "BR",
                }
            ],
        },
        {
            "last_name": "Santos",
            "first_name": "Carlos",
            "affiliations": [],
        },
    ],
    "source": {
        "title": "Bioinformatics Journal",
        "issn": ["1234-5678"],
    },
    "open_access": {
        "is_open_access": True,
        "colour": "gold",
        "pdf_url": "https://example.com/paper.pdf",
    },
    "citations_count": 12,
    "keywords": ["machine learning", "biodiversity"],
    "fields_of_study": ["Computer Science", "Ecology"],
    "funding": [
        {"org": "CNPq", "funding_id": "409865/2021-0", "country": "BR"},
    ],
}

RAW_SCHOLARLY_GREEN_OA = {**RAW_SCHOLARLY, "lens_id": "111", "doi": "10.1234/green",
                           "open_access": {"is_open_access": True, "colour": "green"}}
RAW_SCHOLARLY_CLOSED  = {**RAW_SCHOLARLY, "lens_id": "222", "doi": "10.1234/closed",
                          "open_access": {"is_open_access": False}}
RAW_SCHOLARLY_NO_DOI  = {**RAW_SCHOLARLY, "lens_id": "333", "doi": None}

# ------------------------------------------------------------------
# Raw fixtures — patents
# ------------------------------------------------------------------

RAW_PATENT = {
    "lens_id": "pat-001",
    "pub_number": "BR102021012345A2",
    "pub_key": "BR_102021012345_A2",
    "title": {"text": "Sistema de Monitoramento de Biodiversidade"},
    "year_published": 2022,
    "date_published": "2022-03-10",
    "applicant": [
        {"name": "Universidade Federal do Pará", "ror_id": "03q9sr818",
         "type": "university", "country": "BR"},
    ],
    "inventor": [
        {"name": "Silva, Ana", "country": "BR"},
    ],
    "ipc_classifications": [
        {"code": "G06N 20/00"},   # section G — physics
        {"code": "A01B 79/02"},   # section A — agriculture
    ],
    "npl_resolved_lens_id": ["123-456-789", "987-654-321"],
    "jurisdictions": ["BR", "US", "EP"],   # intl family
    "families": [{"lens_id": "fam-001"}, {"lens_id": "fam-002"}],
    "patent_citation": [],
    "cited_by": {},
}

RAW_PATENT_DOMESTIC = {
    **RAW_PATENT,
    "lens_id": "pat-002",
    "jurisdictions": ["BR"],   # single jurisdiction — not international
    "npl_resolved_lens_id": [],
}


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def connector(tmp_path):
    """LensConnector with a temp cache dir and a mock API key."""
    with patch.dict("os.environ", {"LENS_API_KEY": "test-key-abc"}):
        c = LensConnector(cache_dir=str(tmp_path), max_records=500)
    return c


# ------------------------------------------------------------------
# Helper: _bare_ror
# ------------------------------------------------------------------

def test_bare_ror_strips_url():
    assert _bare_ror("https://ror.org/03q9sr818") == "03q9sr818"

def test_bare_ror_already_bare():
    assert _bare_ror("03q9sr818") == "03q9sr818"

def test_bare_ror_none():
    assert _bare_ror(None) is None


# ------------------------------------------------------------------
# normalize — scholarly
# ------------------------------------------------------------------

def test_normalize_source_id(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["source"] == "lens"

def test_normalize_source_record_id(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["source_record_id"] == "123-456-789"

def test_normalize_doi_prefixed(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["doi"] == "https://doi.org/10.1234/lens.test.2023"

def test_normalize_doi_none(connector):
    r = connector.normalize(RAW_SCHOLARLY_NO_DOI)
    assert r["doi"] is None

def test_normalize_title(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert "Amazon" in r["title"]

def test_normalize_year(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["year"] == 2023

def test_normalize_authors_count(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert len(r["authors"]) == 2

def test_normalize_author_name_display(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["authors"][0]["name"] == "Silva, Ana"

def test_normalize_author_fallback_name(connector):
    """Author without display_name falls back to last_name, first_name."""
    r = connector.normalize(RAW_SCHOLARLY)
    assert "Santos" in r["authors"][1]["name"]

def test_normalize_author_orcid(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["authors"][0]["orcid"] == "0000-0001-2345-6789"

def test_normalize_author_affiliation_ror(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    aff = r["authors"][0]["affiliations"][0]
    assert aff["ror_id"] == "03q9sr818"

def test_normalize_journal(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["journal"] == "Bioinformatics Journal"

def test_normalize_issn(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["issn"] == "1234-5678"

def test_normalize_oa_gold(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["oa_status"] == "gold"

def test_normalize_oa_green(connector):
    r = connector.normalize(RAW_SCHOLARLY_GREEN_OA)
    assert r["oa_status"] == "green"

def test_normalize_oa_closed(connector):
    r = connector.normalize(RAW_SCHOLARLY_CLOSED)
    assert r["oa_status"] == "closed"

def test_normalize_oa_url(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["oa_url"] == "https://example.com/paper.pdf"

def test_normalize_citations(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["cited_by_count"] == 12

def test_normalize_keywords(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert "machine learning" in r["keywords"]

def test_normalize_fields_of_study(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert "Ecology" in r["fields_of_study"]

def test_normalize_funders(connector):
    r = connector.normalize(RAW_SCHOLARLY)
    assert r["funders"][0]["name"] == "CNPq"
    assert r["funders"][0]["grant_id"] == "409865/2021-0"


# ------------------------------------------------------------------
# normalize_patent
# ------------------------------------------------------------------

def test_normalize_patent_record_type(connector):
    p = connector.normalize_patent(RAW_PATENT, "283", "UFPA")
    assert p["record_type"] == "patent"

def test_normalize_patent_pub_number(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert p["pub_number"] == "BR102021012345A2"

def test_normalize_patent_title_from_dict(connector):
    """Title field is a dict with 'text' key."""
    p = connector.normalize_patent(RAW_PATENT)
    assert "Biodiversidade" in p["title"]

def test_normalize_patent_ipc_codes(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert len(p["ipc_codes"]) == 2

def test_normalize_patent_ipc_sections(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert set(p["ipc_sections"]) == {"G", "A"}

def test_normalize_patent_npl_count(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert p["npl_count"] == 2

def test_normalize_patent_npl_lens_ids(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert "123-456-789" in p["npl_lens_ids"]

def test_normalize_patent_intl_family_true(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert p["intl_patent_family"] is True

def test_normalize_patent_intl_family_false(connector):
    p = connector.normalize_patent(RAW_PATENT_DOMESTIC)
    assert p["intl_patent_family"] is False

def test_normalize_patent_jurisdictions(connector):
    p = connector.normalize_patent(RAW_PATENT)
    assert "BR" in p["jurisdictions"]
    assert "US" in p["jurisdictions"]

def test_normalize_patent_applicants(connector):
    p = connector.normalize_patent(RAW_PATENT, "283", "UFPA")
    assert p["applicants"][0]["name"] == "Universidade Federal do Pará"
    assert p["applicants"][0]["ror_id"] == "03q9sr818"

def test_normalize_patent_e_mec(connector):
    p = connector.normalize_patent(RAW_PATENT, "283", "UFPA")
    assert p["e_mec_code"] == "283"
    assert p["institution_name"] == "UFPA"


# ------------------------------------------------------------------
# summarise_patents
# ------------------------------------------------------------------

def test_summarise_patents_empty(connector):
    s = connector.summarise_patents([], "283", "UFPA")
    assert s["patent_count"] == 0
    assert s["intl_patent_families"] == 0

def test_summarise_patents_count(connector):
    s = connector.summarise_patents([RAW_PATENT, RAW_PATENT_DOMESTIC], "283", "UFPA")
    assert s["patent_count"] == 2

def test_summarise_patents_intl(connector):
    s = connector.summarise_patents([RAW_PATENT, RAW_PATENT_DOMESTIC], "283", "UFPA")
    assert s["intl_patent_families"] == 1   # only RAW_PATENT is international

def test_summarise_patents_npl(connector):
    s = connector.summarise_patents([RAW_PATENT], "283", "UFPA")
    assert s["unique_npl_papers"] == 2   # 2 unique NPL lens IDs

def test_summarise_patents_ipc_sections(connector):
    s = connector.summarise_patents([RAW_PATENT], "283", "UFPA")
    assert "G" in s["ipc_sections"]
    assert "A" in s["ipc_sections"]

def test_summarise_patents_paper_links_aggregate(connector):
    """Two patents, one with 2 NPLs and one with 0 → 2 total links."""
    s = connector.summarise_patents([RAW_PATENT, RAW_PATENT_DOMESTIC], "283", "UFPA")
    assert s["paper_patent_links"] == 2


# ------------------------------------------------------------------
# fetch — no ROR ID warning
# ------------------------------------------------------------------

def test_fetch_no_ror_returns_empty(connector):
    result = connector._fetch("283", None, "UFPA", 2021, 2025)
    assert result == []
