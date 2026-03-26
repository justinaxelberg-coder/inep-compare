# tests/connectors/test_crossref.py
from __future__ import annotations
import os
import pytest
from unittest.mock import patch, MagicMock
from connectors.api.crossref import CrossrefConnector


@pytest.fixture(autouse=True)
def _set_crossref_mailto(monkeypatch):
    monkeypatch.setenv("CROSSREF_MAILTO", "test@example.com")

_SAMPLE_WORK = {
    "DOI": "10.1234/test",
    "type": "journal-article",
    "funder": [{"name": "CNPq", "DOI": "10.13039/501100003593"}],
    "license": [{"URL": "https://creativecommons.org/licenses/by/4.0/"}],
    "author": [
        {"given": "Maria", "family": "Silva",
         "affiliation": [{"name": "UFPA", "id": [{"id": "https://ror.org/03q9sr818", "id-type": "ROR"}]}]}
    ],
}

def test_init_uses_env():
    conn = CrossrefConnector()
    assert conn.email == "test@example.com"
    assert conn.source_id == "crossref"


def test_init_raises_without_env(monkeypatch):
    monkeypatch.delenv("CROSSREF_MAILTO", raising=False)
    with pytest.raises(ValueError, match="CROSSREF_MAILTO"):
        CrossrefConnector()

def test_has_funder_true():
    conn = CrossrefConnector()
    assert conn.has_funder(_SAMPLE_WORK) is True

def test_has_funder_false():
    conn = CrossrefConnector()
    assert conn.has_funder({"DOI": "10.1/x", "type": "journal-article"}) is False

def test_has_license():
    conn = CrossrefConnector()
    assert conn.has_license(_SAMPLE_WORK) is True

def test_has_ror_affiliation():
    conn = CrossrefConnector()
    assert conn.has_ror_affiliation(_SAMPLE_WORK) is True

def test_no_ror_affiliation():
    work = {**_SAMPLE_WORK, "author": [{"given": "A", "family": "B", "affiliation": [{"name": "UFPA"}]}]}
    conn = CrossrefConnector()
    assert conn.has_ror_affiliation(work) is False

def test_is_brazilian_funder():
    conn = CrossrefConnector()
    assert conn.is_brazilian_funder("CNPq") is True
    assert conn.is_brazilian_funder("NIH") is False

def test_validate_doi_returns_dict():
    conn = CrossrefConnector()
    with patch.object(conn, "_get_work", return_value=_SAMPLE_WORK):
        result = conn.validate_doi("10.1234/test")
    assert result["doi"] == "10.1234/test"
    assert result["funder_present"] is True
    assert result["license_present"] is True
    assert result["ror_affiliation_present"] is True
    assert result["brazilian_funder"] is True
    assert result["document_type"] == "journal-article"


def test_validate_doi_returns_title_year_and_type():
    conn = CrossrefConnector()
    work = {
        **_SAMPLE_WORK,
        "title": ["Observed work title"],
        "published": {"date-parts": [[2023, 1, 1]]},
    }
    with patch.object(conn, "_get_work", return_value=work):
        result = conn.validate_doi("10.1234/test")
    assert result["title"] == "Observed work title"
    assert result["published_year"] == 2023
    assert result["document_type"] == "journal-article"

def test_validate_doi_missing_returns_none():
    conn = CrossrefConnector()
    with patch.object(conn, "_get_work", return_value=None):
        assert conn.validate_doi("10.9999/missing") is None
