"""
Scopus connector (Elsevier).

Queries the Scopus Search API by Scopus Affiliation ID (primary) or
institution name (fallback). Institutional API key required.

Requires: SCOPUS_API_KEY env var
Optional: SCOPUS_INST_TOKEN env var (for full-text access on campus)

API docs: https://dev.elsevier.com/documentation/SCOPUSSearchAPI.wadl
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import csv

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

_CROSSWALK_PATH = "registry/crosswalk_template.csv"


def _load_scopus_crosswalk() -> dict[str, str]:
    """Return {e_mec_code: scopus_affiliation_id} for rows that have an AF-ID."""
    result: dict[str, str] = {}
    try:
        with open(_CROSSWALK_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                af_id = row.get("scopus_affiliation_id", "").strip()
                e_mec = row.get("e_mec_code", "").strip()
                if e_mec and af_id:
                    result[e_mec.lstrip("0") or "0"] = af_id
    except FileNotFoundError:
        pass
    return result

logger = logging.getLogger(__name__)

SCOPUS_BASE = "https://api.elsevier.com/content/search/scopus"
SCOPUS_AFF_BASE = "https://api.elsevier.com/content/search/affiliation"

# Scopus document type codes → common schema
DOCTYPE_MAP = {
    "ar": "article",
    "re": "review",
    "cp": "conference_paper",
    "bk": "book",
    "ch": "book_chapter",
    "ed": "editorial",
    "le": "letter",
    "no": "note",
    "sh": "short_survey",
    "er": "erratum",
}


class ScopusConnector(BaseConnector):
    """
    Scopus connector — queries by Scopus Affiliation ID or name.

    Affiliation ID lookup:
        Use get_affiliation_id(name) to resolve an institution name to
        its Scopus Affiliation ID. Store result in the crosswalk table.
        Queries by AF-ID are far more precise than name-based queries.

    Rate limits: 9 requests/second, 20,000/week (institutional key).
    """

    source_id = "scopus"
    source_name = "Scopus"

    def __init__(
        self,
        api_key: str | None = None,
        inst_token: str | None = None,
        cache_dir: str = "data/raw",
        max_records: int = 500,
        rate_limit_seconds: float = 0.15,
    ):
        super().__init__(cache_dir=cache_dir, max_records=max_records,
                         rate_limit_seconds=rate_limit_seconds)
        self.api_key = api_key or os.environ.get("SCOPUS_API_KEY", "")
        self.inst_token = inst_token or os.environ.get("SCOPUS_INST_TOKEN", "")
        if not self.api_key:
            raise ValueError(
                "Scopus requires an API key. Set SCOPUS_API_KEY env var."
            )
        self._crosswalk = _load_scopus_crosswalk()

    @property
    def _headers(self) -> dict:
        h = {"X-ELS-APIKey": self.api_key, "Accept": "application/json"}
        if self.inst_token:
            h["X-ELS-Insttoken"] = self.inst_token
        return h

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch(self, e_mec_code, ror_id, name, start_year, end_year) -> list[dict]:
        af_id = self._crosswalk.get(e_mec_code)
        if af_id:
            return self.fetch_by_affiliation_id(af_id, start_year, end_year)
        if name:
            return self._fetch_by_name(name, start_year, end_year)
        raise ValueError(f"Scopus: name or AF-ID required for {e_mec_code}")

    def fetch_by_affiliation_id(
        self, affiliation_id: str, start_year: int, end_year: int
    ) -> list[dict]:
        """Query by Scopus Affiliation ID — most precise, use when available."""
        query = f"AF-ID({affiliation_id}) AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
        return self._paginate(query)

    def _fetch_by_name(self, name: str, start_year: int, end_year: int) -> list[dict]:
        """Affiliation name search — less precise, may include false positives."""
        logger.warning(
            f"[scopus] Using name-based search for '{name}'. "
            f"Populate scopus_affiliation_id in crosswalk for precision."
        )
        query = f'AFFIL("{name}") AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}'
        return self._paginate(query)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _paginate(self, query: str) -> list[dict]:
        """Paginate through Scopus search results."""
        records: list[dict] = []
        start = 0
        count = min(25, self.max_records)   # Scopus max per page is 25

        with httpx.Client(timeout=30, headers=self._headers) as client:
            while len(records) < self.max_records:
                params = {
                    "query": query,
                    "count": count,
                    "start": start,
                    "field": (
                        "dc:identifier,doi,dc:title,prism:coverDate,"
                        "author,affiliation,prism:aggregationType,"
                        "subtypeDescription,dc:description,prism:issn,"
                        "citedby-count,openaccess,openaccessFlag,"
                        "fund-acr,fund-no,fund-sponsor"
                    ),
                }
                self._rate_limit()
                response = client.get(SCOPUS_BASE, params=params)
                response.raise_for_status()
                data = response.json()

                results = data.get("search-results", {})
                entries = results.get("entry", [])
                if not entries or entries == [{"@_fa": "true", "error": "Result set was empty"}]:
                    break

                records.extend(entries)
                total = int(results.get("opensearch:totalResults", 0))

                if len(records) >= self.max_records:
                    logger.warning(
                        f"[scopus] Ceiling hit ({self.max_records}). "
                        f"Actual corpus: {total:,}"
                    )
                    self.last_total_count = total
                    break

                self.last_total_count = total
                start += len(entries)
                if start >= total:
                    break

                logger.debug(f"[scopus] Fetched {len(records)} / {total}")

        return records

    # ------------------------------------------------------------------
    # Normalise
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        doi = raw.get("prism:doi") or raw.get("doi")
        if doi:
            doi = f"https://doi.org/{doi}" if not doi.startswith("http") else doi

        # Publication year from cover date (YYYY-MM-DD)
        cover_date = raw.get("prism:coverDate", "")
        year = int(cover_date[:4]) if cover_date else None

        # Authors
        authors_raw = raw.get("author", [])
        if isinstance(authors_raw, dict):
            authors_raw = [authors_raw]
        authors = []
        for a in authors_raw:
            afid_raw = a.get("afid")
            if isinstance(afid_raw, list):
                aff_ids = [x.get("$", "") for x in afid_raw if isinstance(x, dict)]
            elif isinstance(afid_raw, dict):
                aff_ids = [afid_raw.get("$", "")]
            else:
                aff_ids = []
            authors.append({
                "name": a.get("authname"),
                "orcid": None,
                "institutions": aff_ids,
            })

        # Affiliations
        affs_raw = raw.get("affiliation", [])
        if isinstance(affs_raw, dict):
            affs_raw = [affs_raw]
        institutions = [a.get("affilname", "") for a in affs_raw]

        # OA — Scopus has openaccess field (1=OA, 0=closed)
        oa_flag = raw.get("openaccess") or raw.get("openaccessFlag")
        oa_status = "unknown"
        if oa_flag in ("1", 1, True, "true"):
            oa_status = "gold"   # Scopus doesn't distinguish routes — enriched by Unpaywall
        elif oa_flag in ("0", 0, False, "false"):
            oa_status = "closed"

        # Funding
        funding = []
        sponsor = raw.get("fund-sponsor")
        if sponsor:
            funding.append({
                "funder": sponsor,
                "funder_id": raw.get("fund-acr"),
                "grant_number": raw.get("fund-no"),
            })

        return {
            "source": self.source_id,
            "source_record_id": raw.get("dc:identifier", ""),
            "doi": doi,
            "title": raw.get("dc:title"),
            "year": year,
            "authors": authors,
            "institutions": institutions,
            "e_mec_codes": [],
            "fields": [],           # Scopus subject areas need separate API call
            "primary_topic": None,
            "primary_field": None,
            "primary_domain": None,
            "keywords": [],
            "document_type": DOCTYPE_MAP.get(raw.get("subtype", ""), raw.get("subtypeDescription")),
            "language": None,       # not in search response
            "oa_status": oa_status,
            "oa_url": None,         # enriched by Unpaywall
            "licence": None,
            "citation_count": int(raw.get("citedby-count", 0) or 0),
            "fwci": None,           # not available in Scopus search API
            "funding": funding,
            "sdgs": [],  # Scopus does not expose SDG via standard API; flagged in source_metadata.json
            "patent_citations": [],
            "source_url": raw.get("prism:url", ""),
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Affiliation ID lookup (for crosswalk population)
    # ------------------------------------------------------------------

    def get_affiliation_id(self, name: str) -> list[dict]:
        """
        Look up Scopus Affiliation ID(s) for an institution name.
        Use this to populate scopus_affiliation_id in the crosswalk table.

        Returns list of candidates: [{"id": ..., "name": ..., "city": ...}]
        """
        with httpx.Client(timeout=15, headers=self._headers) as client:
            r = client.get(
                SCOPUS_AFF_BASE,
                params={"query": f"AFFIL({name})", "count": 5},
            )
            r.raise_for_status()
            entries = r.json().get("search-results", {}).get("entry", [])
            return [
                {
                    "id": e.get("dc:identifier", "").replace("AFFILIATION_ID:", ""),
                    "name": e.get("affiliation-name"),
                    "city": e.get("city"),
                    "country": e.get("country"),
                    "doc_count": e.get("document-count"),
                }
                for e in entries
            ]
