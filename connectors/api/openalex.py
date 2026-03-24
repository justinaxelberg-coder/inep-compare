"""
OpenAlex connector.

Queries the OpenAlex API by ROR ID (primary) or institution name (fallback).
Free, open, Barcelona Declaration aligned. No auth required.
Polite pool: include email in requests for higher rate limits.

API docs: https://docs.openalex.org
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

OPENALEX_BASE = "https://api.openalex.org"

# Map OpenAlex OA types to our schema
OA_STATUS_MAP = {
    "gold": "gold",
    "green": "green",
    "hybrid": "hybrid",
    "diamond": "diamond",
    "bronze": "bronze",         # free to read but no licence — flagged separately
    "closed": "closed",
}


class OpenAlexConnector(BaseConnector):
    """
    OpenAlex connector — works per institution by ROR ID.

    Handles pagination automatically. Falls back to name-based affiliation
    search when ROR ID is not available.
    """

    source_id = "openalex"
    source_name = "OpenAlex"

    def __init__(
        self,
        email: str | None = None,
        cache_dir: str = "data/raw",
        max_records: int | None = 500,
        rate_limit_seconds: float = 0.5,   # polite pool allows ~10 req/s
    ):
        super().__init__(cache_dir=cache_dir,
                         max_records=max_records if max_records is not None else float("inf"),
                         rate_limit_seconds=rate_limit_seconds)
        # Email enables polite pool (higher rate limits)
        self.email = email or os.environ.get("OPENALEX_EMAIL", "")
        if not self.email:
            logger.warning(
                "No email set for OpenAlex polite pool. "
                "Set OPENALEX_EMAIL env var or pass email= for higher rate limits."
            )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch(
        self,
        e_mec_code: str,
        ror_id: str | None,
        name: str | None,
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        if ror_id:
            return self._fetch_by_ror(ror_id, start_year, end_year)
        elif name:
            logger.warning(
                f"[openalex] No ROR ID for {e_mec_code} — falling back to name search. "
                f"Results may include false positives."
            )
            return self._fetch_by_name(name, start_year, end_year)
        else:
            raise ValueError(f"OpenAlex requires ror_id or name for {e_mec_code}")

    def _fetch_by_ror(self, ror_id: str, start_year: int, end_year: int) -> list[dict]:
        """Paginate through works for a given ROR ID."""
        # Strip URL prefix — OpenAlex filter expects bare ID (e.g. "028kg9j04")
        ror_bare = ror_id.replace("https://ror.org/", "").replace("http://ror.org/", "")
        filter_str = (
            f"authorships.institutions.ror:{ror_bare},"
            f"from_publication_date:{start_year}-01-01,"
            f"to_publication_date:{end_year}-12-31"
        )
        return self._paginate_works(filter_str)

    def _fetch_by_name(self, name: str, start_year: int, end_year: int) -> list[dict]:
        """Affiliation string search — lower confidence, flagged in output."""
        filter_str = (
            f"authorships.institutions.display_name.search:{name},"
            f"from_publication_date:{start_year}-01-01,"
            f"to_publication_date:{end_year}-12-31"
        )
        return self._paginate_works(filter_str)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _paginate_works(self, filter_str: str) -> list[dict]:
        """Retrieve all pages up to max_records."""
        records: list[dict] = []
        cursor = "*"
        per_page = min(200, self.max_records)  # OpenAlex max per page is 200
        self.last_total_count: int | None = None  # set on first response

        # Force gzip — Brotli responses from OpenAlex cause decompression errors
        # on some httpx/brotlicffi combinations with large payloads
        with httpx.Client(timeout=30, headers={"Accept-Encoding": "gzip"}) as client:
            while len(records) < self.max_records:
                params: dict = {
                    "filter": filter_str,
                    "per-page": per_page,
                    "cursor": cursor,
                    "select": (
                        "id,doi,title,publication_year,authorships,"
                        "primary_location,open_access,cited_by_count,"
                        "topics,primary_topic,keywords,type,language,"
                        "funders,sustainable_development_goals,fwci"
                    ),
                }
                if self.email:
                    params["mailto"] = self.email

                self._rate_limit()
                response = client.get(f"{OPENALEX_BASE}/works", params=params)
                response.raise_for_status()
                data = response.json()

                page_results = data.get("results", [])
                if not page_results:
                    break

                records.extend(page_results)

                meta = data.get("meta", {})
                if self.last_total_count is None:
                    self.last_total_count = meta.get("count")
                cursor = meta.get("next_cursor")
                if not cursor:
                    break

                logger.debug(
                    f"[openalex] Fetched {len(records)} / "
                    f"{self.last_total_count or '?'} records"
                )

        return records

    # ------------------------------------------------------------------
    # Normalise
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Map OpenAlex work to the common publication schema."""
        # Open access
        oa = raw.get("open_access", {})
        oa_status = OA_STATUS_MAP.get(oa.get("oa_status", ""), "unknown")

        # Primary location licence
        primary = raw.get("primary_location") or {}
        source_info = primary.get("source") or {}
        licence = primary.get("license")

        # Authorships
        authors = []
        e_mec_codes: list[str] = []
        for auth in raw.get("authorships", []):
            author_inst_ids = []
            for inst in auth.get("institutions", []):
                author_inst_ids.append(inst.get("display_name", ""))
                # e-MEC resolution happens in the crosswalk layer, not here
            authors.append({
                "name": auth.get("author", {}).get("display_name"),
                "orcid": auth.get("author", {}).get("orcid"),
                "institutions": author_inst_ids,
            })

        # Fields — topics (replaces deprecated concepts)
        # Use primary_topic for single best field, topics[] for full list
        primary_topic = raw.get("primary_topic") or {}
        fields = [
            t["display_name"]
            for t in raw.get("topics", [])
            if t.get("score", 0) >= 0.3
        ]

        # Keywords (author-assigned)
        keywords = [k.get("display_name", "") for k in (raw.get("keywords") or [])]

        # SDG alignment
        sdgs = [
            {"id": s.get("id"), "display_name": s.get("display_name"), "score": s.get("score")}
            for s in (raw.get("sustainable_development_goals") or [])
        ]

        # Funders — includes ROR, enables BR funder filtering (FAPESP, CNPq, CAPES)
        funding = [
            {
                "funder": f.get("display_name"),
                "funder_id": f.get("id"),
                "funder_ror": f.get("ror"),
            }
            for f in (raw.get("funders") or [])
        ]

        return {
            "source": self.source_id,
            "source_record_id": raw.get("id", ""),
            "doi": raw.get("doi"),
            "title": raw.get("title"),
            "year": raw.get("publication_year"),
            "authors": authors,
            "institutions": [
                inst.get("display_name", "")
                for auth in raw.get("authorships", [])
                for inst in auth.get("institutions", [])
            ],
            "e_mec_codes": [],          # resolved in crosswalk layer
            "fields": fields,
            "primary_topic": primary_topic.get("display_name"),
            "primary_field": (primary_topic.get("field") or {}).get("display_name"),
            "primary_domain": (primary_topic.get("domain") or {}).get("display_name"),
            "keywords": keywords,
            "document_type": raw.get("type"),
            "language": raw.get("language"),
            "oa_status": oa_status,
            "oa_url": oa.get("oa_url"),
            "licence": licence,
            "citation_count": raw.get("cited_by_count"),
            "fwci": raw.get("fwci"),    # field-weighted citation impact
            "funding": funding,
            "sdgs": sdgs,               # SDG alignment — key for social impact scoring
            "patent_citations": [],     # enriched by The Lens connector
            "source_url": f"https://openalex.org/{raw.get('id', '').split('/')[-1]}",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Institution lookup (for crosswalk validation)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Total count sidecar (persists API meta.count alongside record cache)
    # ------------------------------------------------------------------

    def _save_total_count(self, cache_key: str, count: int | None) -> None:
        if count is None:
            return
        sidecar = self.cache_dir / f"{cache_key}_meta.json"
        with sidecar.open("w") as f:
            json.dump({"total_count": count}, f)

    def _load_total_count(self, cache_key: str) -> int | None:
        sidecar = self.cache_dir / f"{cache_key}_meta.json"
        if sidecar.exists():
            with sidecar.open() as f:
                return json.load(f).get("total_count")
        return None

    def query_institution(self, e_mec_code, ror_id=None, name=None,
                          start_year=2022, end_year=2023, use_cache=True):
        """Override to persist/restore last_total_count via sidecar."""
        inst_key = self._cache_key(e_mec_code, ror_id, name, start_year, end_year)

        # Try sidecar first
        self.last_total_count = self._load_total_count(inst_key)

        records = super().query_institution(
            e_mec_code=e_mec_code, ror_id=ror_id, name=name,
            start_year=start_year, end_year=end_year, use_cache=use_cache,
        )

        # After fetch: persist if we got a count from _paginate_works
        if self.last_total_count is not None:
            self._save_total_count(inst_key, self.last_total_count)

        # Cache hit but no sidecar yet — do a lightweight count-only request
        elif use_cache and ror_id:
            self.last_total_count = self._fetch_count_only(ror_id, start_year, end_year)
            if self.last_total_count is not None:
                self._save_total_count(inst_key, self.last_total_count)

        return records

    def _fetch_count_only(self, ror_id: str, start_year: int, end_year: int) -> int | None:
        """Single lightweight API call to get total record count only (per-page=1)."""
        ror_bare = ror_id.replace("https://ror.org/", "").replace("http://ror.org/", "")
        filter_str = (
            f"authorships.institutions.ror:{ror_bare},"
            f"from_publication_date:{start_year}-01-01,"
            f"to_publication_date:{end_year}-12-31"
        )
        params = {"filter": filter_str, "per-page": 1, "select": "id"}
        if self.email:
            params["mailto"] = self.email
        try:
            self._rate_limit()
            with httpx.Client(timeout=15, headers={"Accept-Encoding": "gzip"}) as client:
                r = client.get(f"{OPENALEX_BASE}/works", params=params)
                r.raise_for_status()
                return r.json().get("meta", {}).get("count")
        except Exception as e:
            logger.warning(f"[openalex] Count-only fetch failed: {e}")
            return None

    def get_institution_by_ror(self, ror_id: str) -> dict | None:
        """
        Fetch OpenAlex institution record for a given ROR ID.
        Useful for populating the crosswalk table.
        """
        with httpx.Client(timeout=15) as client:
            params = {"filter": f"ror:{ror_id}"}
            if self.email:
                params["mailto"] = self.email
            r = client.get(f"{OPENALEX_BASE}/institutions", params=params)
            r.raise_for_status()
            results = r.json().get("results", [])
            return results[0] if results else None
