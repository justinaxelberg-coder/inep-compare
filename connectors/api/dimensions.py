"""
Dimensions connector (Digital Science).

Queries the Dimensions Analytics API using DSL (Dimensions Search Language).
Has a free API tier sufficient for research use.

Auth: username/password → JWT token (refreshed automatically).
Alternatively supports API key via DIMENSIONS_API_KEY for direct access.

API docs: https://docs.dimensions.ai/dsl/
Requires: DIMENSIONS_API_KEY env var (preferred) or
          DIMENSIONS_USERNAME + DIMENSIONS_PASSWORD
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import csv

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

DIMENSIONS_BASE = "https://app.dimensions.ai/api"
DIMENSIONS_DSL  = f"{DIMENSIONS_BASE}/dsl/v2"

_CROSSWALK_PATH = "registry/crosswalk_template.csv"


def _load_dimensions_crosswalk() -> dict[str, str]:
    """Return {e_mec_code: dimensions_grid_id} for rows that have a GRID ID."""
    result: dict[str, str] = {}
    try:
        with open(_CROSSWALK_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                grid_id = row.get("dimensions_grid_id", "").strip()
                e_mec = row.get("e_mec_code", "").strip().lstrip("0") or "0"
                if e_mec and grid_id:
                    result[e_mec] = grid_id
    except FileNotFoundError:
        pass
    return result


class DimensionsConnector(BaseConnector):
    """
    Dimensions connector — DSL queries by ROR ID (preferred) or name.

    Dimensions natively supports ROR-based institution filtering,
    making it one of the cleanest sources for Brazilian HEI queries.

    Free tier: 25,000 records/month, 30 queries/minute.
    """

    source_id = "dimensions"
    source_name = "Dimensions"

    def __init__(
        self,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        cache_dir: str = "data/raw",
        max_records: int | None = 500,
        rate_limit_seconds: float = 2.0,   # conservative: 30 req/min free tier
    ):
        super().__init__(cache_dir=cache_dir,
                         max_records=max_records if max_records is not None else float("inf"),
                         rate_limit_seconds=rate_limit_seconds)
        self.api_key = api_key or os.environ.get("DIMENSIONS_API_KEY", "")
        self.username = username or os.environ.get("DIMENSIONS_USERNAME", "")
        self.password = password or os.environ.get("DIMENSIONS_PASSWORD", "")
        self._jwt_token: str | None = None
        self._crosswalk = _load_dimensions_crosswalk()

        if not self.api_key and not (self.username and self.password):
            raise ValueError(
                "Dimensions requires DIMENSIONS_API_KEY or "
                "DIMENSIONS_USERNAME + DIMENSIONS_PASSWORD env vars."
            )

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Obtain or return cached JWT token."""
        if self._jwt_token:
            return self._jwt_token
        with httpx.Client(timeout=15) as client:
            if self.api_key:
                # API key → exchange for JWT
                r = client.post(
                    f"{DIMENSIONS_BASE}/auth.json",
                    json={"key": self.api_key},
                )
            else:
                # Username/password → exchange for JWT
                r = client.post(
                    f"{DIMENSIONS_BASE}/auth.json",
                    json={"username": self.username, "password": self.password},
                )
            r.raise_for_status()
            self._jwt_token = r.json().get("token")
        return self._jwt_token

    @property
    def _headers(self) -> dict:
        return {"Authorization": f"JWT {self._get_token()}", "Content-Type": "text/plain"}

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch(self, e_mec_code, ror_id, name, start_year, end_year) -> list[dict]:
        grid_id = self._crosswalk.get(e_mec_code)
        if grid_id:
            return self._fetch_by_grid(grid_id, start_year, end_year)
        if name:
            logger.warning(
                f"[dimensions] No GRID ID for {e_mec_code} — using name search. "
                f"Results may include false positives."
            )
            return self._fetch_by_name(name, start_year, end_year)
        raise ValueError(f"Dimensions: grid_id or name required for {e_mec_code}")

    def _fetch_by_grid(self, grid_id: str, start_year: int, end_year: int) -> list[dict]:
        """Query by GRID ID — most precise identifier in Dimensions DSL."""
        dsl = (
            f'search publications '
            f'where research_orgs.id = "{grid_id}" '
            f'and year in [{start_year}:{end_year}] '
            f'return publications'
        )
        return self._paginate(dsl)

    def _fetch_by_name(self, name: str, start_year: int, end_year: int) -> list[dict]:
        dsl = (
            f'search publications '
            f'where research_orgs.name ~ "{name}" '
            f'and year in [{start_year}:{end_year}] '
            f'return publications'
        )
        return self._paginate(dsl)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=15))
    def _paginate(self, base_dsl: str) -> list[dict]:
        """Paginate through Dimensions DSL results."""
        records: list[dict] = []
        skip = 0
        limit = min(500, self.max_records)  # Dimensions max per request is 1000

        # Fields to return — Dimensions DSL uses + separator in field lists
        fields = (
            "id+doi+title+year+authors+research_orgs"
            "+open_access+document_type"
            "+times_cited+funders+category_sdg+field_citation_ratio"
        )
        dsl_with_fields = base_dsl.replace("return publications", f"return publications[{fields}]")

        with httpx.Client(timeout=60, headers=self._headers) as client:
            while len(records) < self.max_records:
                dsl_paged = f"{dsl_with_fields} limit {limit} skip {skip}"

                self._rate_limit()
                response = client.post(DIMENSIONS_DSL, content=dsl_paged)

                if response.status_code == 401:
                    # Token expired — refresh and retry once
                    self._jwt_token = None
                    client.headers.update(self._headers)
                    response = client.post(DIMENSIONS_DSL, content=dsl_paged)

                response.raise_for_status()
                data = response.json()

                pubs = data.get("publications", [])
                if not pubs:
                    break

                records.extend(pubs)

                total = data.get("_stats", {}).get("total_count", 0)
                self.last_total_count = total

                if len(records) >= self.max_records:
                    logger.warning(
                        f"[dimensions] Ceiling hit ({self.max_records}). "
                        f"Actual corpus: {total:,}"
                    )
                    break

                skip += len(pubs)
                if skip >= total:
                    break

                logger.debug(f"[dimensions] Fetched {len(records)} / {total}")

        return records

    # ------------------------------------------------------------------
    # Normalise
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        doi_val = raw.get("doi")
        doi = f"https://doi.org/{doi_val}" if doi_val and not doi_val.startswith("http") else doi_val

        # Authors
        authors_raw = raw.get("authors", []) or []
        authors = [
            {
                "name": f"{a.get('last_name', '')}, {a.get('first_name', '')}".strip(", "),
                "orcid": a.get("orcid"),
                "institutions": [o.get("name", "") for o in (a.get("affiliations") or [])],
            }
            for a in authors_raw
        ]

        # Institutions (research_orgs)
        institutions = [
            o.get("name", "") for o in (raw.get("research_orgs") or [])
        ]

        # OA — Dimensions returns open_access as a list of category strings
        oa_raw = raw.get("open_access") or []
        if isinstance(oa_raw, list):
            oa_cats = [c.lower() for c in oa_raw]
        else:
            oa_cats = [str(oa_raw).lower()] if oa_raw else []
        oa_status = self._classify_oa(oa_cats, None)

        # Funding
        funders_raw = raw.get("funders") or []
        funding = [
            {
                "funder": f.get("name"),
                "funder_id": f.get("id"),
                "grant_number": f.get("grant_number"),
            }
            for f in funders_raw
        ]

        # SDGs
        sdgs = [
            {"id": s.get("id"), "display_name": s.get("name"), "score": None}
            for s in (raw.get("category_sdg") or [])
        ]

        return {
            "source": self.source_id,
            "source_record_id": raw.get("id", ""),
            "doi": doi,
            "title": raw.get("title"),
            "year": raw.get("year"),
            "authors": authors,
            "institutions": institutions,
            "e_mec_codes": [],
            "fields": [],
            "primary_topic": None,
            "primary_field": None,
            "primary_domain": None,
            "keywords": [],
            "document_type": raw.get("document_type") or raw.get("type"),
            "language": None,
            "oa_status": oa_status,
            "oa_url": None,         # enriched by Unpaywall
            "licence": None,
            "citation_count": raw.get("times_cited"),
            "fwci": raw.get("field_citation_ratio"),  # FCR — Dimensions equivalent of FWCI
            "funding": funding,
            "sdgs": sdgs,
            "patent_citations": [],
            "source_url": f"https://app.dimensions.ai/details/publication/{raw.get('id', '')}",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    def _classify_oa(self, oa_cats: list[str], oa_flag) -> str:
        """Map Dimensions OA categories to common schema."""
        if not oa_cats or "closed" in oa_cats:
            return "closed"
        if "gold" in oa_cats:
            return "gold"
        if "diamond" in oa_cats or "bronze" in oa_cats:
            # Dimensions uses 'bronze' for free-to-read without licence
            return "diamond" if "diamond" in oa_cats else "bronze"
        if "green" in oa_cats:
            return "green"
        if "hybrid" in oa_cats:
            return "hybrid"
        if oa_flag:
            return "gold"
        return "closed"
