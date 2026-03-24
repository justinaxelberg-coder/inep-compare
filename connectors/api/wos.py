"""
Web of Science connector (Clarivate).

Queries the WoS Expanded API using Organization-Enhanced (OG) field.
Institutional API key required.

Requires: WOS_API_KEY env var

API docs: https://developer.clarivate.com/apis/wos
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

WOS_BASE = "https://api.clarivate.com/api/wos"

DOCTYPE_MAP = {
    "Article": "article",
    "Review": "review",
    "Proceedings Paper": "conference_paper",
    "Book Chapter": "book_chapter",
    "Book": "book",
    "Editorial Material": "editorial",
    "Letter": "letter",
    "Note": "note",
    "Data Paper": "data_paper",
    "Correction": "erratum",
}


class WoSConnector(BaseConnector):
    """
    Web of Science connector — queries by Organization-Enhanced (OG) name.

    WoS uses its own organisation disambiguation (OG field). Institution
    names need to match WoS's normalised organisation names — these can
    differ from official names (e.g. "Fed Univ ABC" vs full name).

    Use get_org_names(name) to discover the correct WoS OG string.
    """

    source_id = "wos"
    source_name = "Web of Science"

    def __init__(
        self,
        api_key: str | None = None,
        cache_dir: str = "data/raw",
        max_records: int = 500,
        rate_limit_seconds: float = 0.2,
    ):
        super().__init__(cache_dir=cache_dir, max_records=max_records,
                         rate_limit_seconds=rate_limit_seconds)
        self.api_key = api_key or os.environ.get("WOS_API_KEY", "")
        if not self.api_key:
            raise ValueError("WoS requires an API key. Set WOS_API_KEY env var.")

    @property
    def _headers(self) -> dict:
        return {"X-ApiKey": self.api_key, "Accept": "application/json"}

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch(self, e_mec_code, ror_id, name, start_year, end_year) -> list[dict]:
        if not name:
            raise ValueError(f"WoS: name required for {e_mec_code}")
        return self._fetch_by_org(name, start_year, end_year)

    def _fetch_by_org(self, org_name: str, start_year: int, end_year: int) -> list[dict]:
        """
        Query by Organization-Enhanced (OG) field.
        WoS OG names are normalised — may not match official institution name exactly.
        Use get_org_suggestions() to find the right string.
        """
        query = f"OG=({org_name}) AND PY={start_year}-{end_year}"
        return self._paginate(query)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _paginate(self, query: str) -> list[dict]:
        """Paginate through WoS search results."""
        records: list[dict] = []
        first_record = 1
        count = min(100, self.max_records)  # WoS max per page is 100

        with httpx.Client(timeout=30, headers=self._headers) as client:
            while len(records) < self.max_records:
                params = {
                    "databaseId": "WOS",
                    "usrQuery": query,
                    "count": count,
                    "firstRecord": first_record,
                }
                self._rate_limit()
                response = client.get(f"{WOS_BASE}", params=params)
                response.raise_for_status()
                data = response.json()

                query_result = data.get("QueryResult", {})
                total = int(query_result.get("RecordsFound", 0))
                self.last_total_count = total

                hits = data.get("Data", {}).get("Records", {}).get("records", {})
                # WoS returns records as dict with REC list or directly
                if isinstance(hits, dict):
                    rec_list = hits.get("REC", [])
                elif isinstance(hits, list):
                    rec_list = hits
                else:
                    rec_list = []

                if not rec_list:
                    break

                records.extend(rec_list if isinstance(rec_list, list) else [rec_list])

                if len(records) >= self.max_records:
                    logger.warning(
                        f"[wos] Ceiling hit ({self.max_records}). "
                        f"Actual corpus: {total:,}"
                    )
                    break

                first_record += len(rec_list)
                if first_record > total:
                    break

                logger.debug(f"[wos] Fetched {len(records)} / {total}")

        return records

    # ------------------------------------------------------------------
    # Normalise
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Map WoS record to common schema."""
        # WoS records have a complex nested structure
        static = raw.get("static_data", {})
        dynamic = raw.get("dynamic_data", {})
        summary = static.get("summary", {})

        # Title
        titles = summary.get("titles", {}).get("title", [])
        if isinstance(titles, dict):
            titles = [titles]
        title = next((t.get("content") for t in titles if t.get("type") == "item"), None)

        # DOI
        doi = None
        identifiers = dynamic.get("cluster_related", {}).get("identifiers", {}).get("identifier", [])
        if isinstance(identifiers, dict):
            identifiers = [identifiers]
        for ident in identifiers:
            if ident.get("type") == "doi":
                doi_val = ident.get("value", "")
                doi = f"https://doi.org/{doi_val}" if doi_val else None
                break

        # Year
        pub_info = summary.get("pub_info", {})
        year_str = pub_info.get("pubyear")
        year = int(year_str) if year_str else None

        # Authors
        names = summary.get("names", {}).get("name", [])
        if isinstance(names, dict):
            names = [names]
        authors = [
            {
                "name": n.get("display_name") or f"{n.get('last_name', '')}, {n.get('first_name', '')}".strip(", "),
                "orcid": n.get("orcid_id"),
                "institutions": [],
            }
            for n in names if n.get("role") == "author"
        ]

        # Institutions
        addresses = static.get("fullrecord_metadata", {}).get("addresses", {}).get("address_spec", [])
        if isinstance(addresses, dict):
            addresses = [addresses]
        institutions = [a.get("organizations", {}).get("organization", [None])[0] or ""
                        for a in addresses if a.get("organizations")]

        # Document type
        doc_types = summary.get("doctypes", {}).get("doctype", [])
        if isinstance(doc_types, str):
            doc_types = [doc_types]
        document_type = DOCTYPE_MAP.get(doc_types[0] if doc_types else "", None)

        # Citation count
        citation_count = None
        citing = dynamic.get("citation_related", {}).get("tc_list", {}).get("silo_tc", {})
        if isinstance(citing, dict):
            citation_count = int(citing.get("local_count", 0) or 0)

        # OA — WoS has OA flag in dynamic data
        oa_flag = dynamic.get("ic_related", {}).get("orc_ids", {})
        oa_status = "unknown"
        if oa_flag:
            oa_status = "gold"   # Enriched by Unpaywall for route

        # Funding
        grants = static.get("fullrecord_metadata", {}).get("fund_ack", {}).get("grants", {}).get("grant", [])
        if isinstance(grants, dict):
            grants = [grants]
        funding = [
            {
                "funder": g.get("grant_agency"),
                "funder_id": None,
                "grant_number": g.get("grant_ids", {}).get("grant_id"),
            }
            for g in grants if g.get("grant_agency")
        ]

        # UID
        uid = raw.get("UID", "")

        return {
            "source": self.source_id,
            "source_record_id": uid,
            "doi": doi,
            "title": title,
            "year": year,
            "authors": authors,
            "institutions": institutions,
            "e_mec_codes": [],
            "fields": [],
            "primary_topic": None,
            "primary_field": None,
            "primary_domain": None,
            "keywords": [],
            "document_type": document_type,
            "language": pub_info.get("pubtype"),
            "oa_status": oa_status,
            "oa_url": None,
            "licence": None,
            "citation_count": citation_count,
            "fwci": None,
            "funding": funding,
            "sdgs": [],
            "patent_citations": [],
            "source_url": f"https://www.webofscience.com/wos/woscc/full-record/{uid}",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Organisation name lookup (for crosswalk population)
    # ------------------------------------------------------------------

    def get_org_suggestions(self, name: str) -> list[str]:
        """
        Query WoS organisation suggest endpoint to find the correct OG string.
        Use this to populate wos_org_name in the crosswalk table.
        """
        with httpx.Client(timeout=15, headers=self._headers) as client:
            r = client.get(
                f"{WOS_BASE}/references/organization",
                params={"q": name, "limit": 10},
            )
            r.raise_for_status()
            return r.json()
