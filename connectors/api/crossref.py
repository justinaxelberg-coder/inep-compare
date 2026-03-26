# connectors/api/crossref.py
from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://api.crossref.org/works"

_BR_FUNDERS = {
    # Federal agencies
    "cnpq", "capes", "finep", "bndes", "embrapii", "mec",
    # State FAP agencies
    "fapesp", "fapemig", "faperj", "fapesc", "fapesb", "fapespa",
    "fapergs", "fapeal", "fapern", "fapero", "fapdf",
    # Substring matches for full names
    "fundação de amparo", "conselho nacional", "coordenação de aperfeiçoamento",
}


class CrossrefConnector:
    """Lightweight Crossref metadata validator. No API key required.

    Role: validates funder presence, license declaration, document type,
    and ROR affiliation coverage for a set of DOIs. Not a scored source.
    """

    source_id = "crossref"

    def __init__(self, email: str | None = None, rate_limit_seconds: float = 1.0) -> None:
        if email is None:
            email = os.getenv("CROSSREF_MAILTO")
            if not email:
                raise ValueError(
                    "CROSSREF_MAILTO environment variable is required for Crossref polite pool. "
                    "Set it to your contact email address."
                )
        self.email = email
        self.rate_limit = rate_limit_seconds

    def _get_work(self, doi: str) -> dict | None:
        url = f"{_BASE}/{doi}"
        try:
            r = httpx.get(url, params={"mailto": self.email}, timeout=10)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            time.sleep(self.rate_limit)
            return r.json().get("message", {})
        except Exception as exc:
            logger.warning("Crossref lookup failed for %s: %s", doi, exc)
            return None

    def has_funder(self, work: dict) -> bool:
        return bool(work.get("funder"))

    def has_license(self, work: dict) -> bool:
        return bool(work.get("license"))

    def has_ror_affiliation(self, work: dict) -> bool:
        for author in work.get("author") or []:
            for aff in author.get("affiliation") or []:
                for id_entry in aff.get("id") or []:
                    if id_entry.get("id-type") == "ROR":
                        return True
        return False

    def is_brazilian_funder(self, funder_name: str) -> bool:
        lower = funder_name.lower()
        return any(br in lower for br in _BR_FUNDERS)

    def _extract_year(self, work: dict) -> int | None:
        for field in ("published", "published-print", "issued", "published-online"):
            published = work.get(field) or {}
            date_parts = published.get("date-parts") or []
            if not date_parts or not date_parts[0]:
                continue
            year = date_parts[0][0]
            if year not in (None, ""):
                try:
                    return int(year)
                except (TypeError, ValueError):
                    continue
        return None

    def validate_doi(self, doi: str) -> dict | None:
        """Return metadata quality dict for one DOI, or None if not found."""
        work = self._get_work(doi)
        if work is None:
            return None
        titles = work.get("title") or []
        funders = work.get("funder") or []
        return {
            "doi": doi,
            "funder_present": self.has_funder(work),
            "brazilian_funder": any(
                self.is_brazilian_funder(f.get("name", "")) for f in funders
            ),
            "license_present": self.has_license(work),
            "ror_affiliation_present": self.has_ror_affiliation(work),
            "document_type": work.get("type"),
            "title": titles[0] if titles else None,
            "published_year": self._extract_year(work),
        }

    def validate_batch(self, dois: list[str]) -> list[dict]:
        """Validate a list of DOIs. Returns only successful results."""
        results = []
        for doi in dois:
            result = self.validate_doi(doi)
            if result:
                results.append(result)
        return results
