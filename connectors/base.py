"""
Abstract base connector.

All connectors — API, file, scrape, manual — implement this interface.
The scoring engine and convergence engine never need to know which
access mode produced a record.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract connector. Subclasses implement _fetch() and normalize().

    Features provided by base class:
    - Local disk cache (JSON, keyed by query hash)
    - Rate limiting (min seconds between requests)
    - max_records ceiling with warning
    """

    source_id: str = ""          # must be set by subclass, matches source_registry.yaml key
    source_name: str = ""

    def __init__(
        self,
        cache_dir: str | Path = "data/raw",
        max_records: int = 500,
        rate_limit_seconds: float = 1.0,
    ):
        self.cache_dir = Path(cache_dir) / self.source_id
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_records = max_records
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def query_institution(
        self,
        e_mec_code: str,
        ror_id: str | None = None,
        name: str | None = None,
        start_year: int = 2022,
        end_year: int = 2023,
        use_cache: bool = True,
    ) -> list[dict]:
        """
        Query all records for a given institution within the temporal window.
        Returns a list of normalised record dicts conforming to the common schema.
        """
        cache_key = self._cache_key(e_mec_code, ror_id, name, start_year, end_year)
        cached = self._load_cache(cache_key) if use_cache else None

        if cached is not None:
            logger.debug(f"[{self.source_id}] Cache hit for {e_mec_code}")
            return cached

        self._rate_limit()
        logger.info(f"[{self.source_id}] Fetching {e_mec_code} ({start_year}–{end_year})")

        raw_records = self._fetch(
            e_mec_code=e_mec_code,
            ror_id=ror_id,
            name=name,
            start_year=start_year,
            end_year=end_year,
        )

        if len(raw_records) >= self.max_records:
            logger.warning(
                f"[{self.source_id}] {e_mec_code}: max_records ceiling hit "
                f"({self.max_records}). Results are truncated. "
                f"This is a data point — consider raising ceiling or narrowing query."
            )

        normalised = [self.normalize(r) for r in raw_records[: self.max_records]]
        self._save_cache(cache_key, normalised)
        return normalised

    # ------------------------------------------------------------------
    # Abstract methods — subclasses must implement
    # ------------------------------------------------------------------

    @abstractmethod
    def _fetch(
        self,
        e_mec_code: str,
        ror_id: str | None,
        name: str | None,
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        """Fetch raw records from the source. Returns raw API/scrape dicts."""
        ...

    @abstractmethod
    def normalize(self, raw: dict) -> dict:
        """
        Map a raw source record to the common publication schema:

        {
            # Identity
            "source": str,              # source_id
            "source_record_id": str,    # source's own ID
            "doi": str | None,
            "title": str | None,
            "year": int | None,

            # Authorship & affiliation
            "authors": list[dict],      # [{name, orcid, institutions: []}]
            "institutions": list[str],  # institution names as they appear in source
            "e_mec_codes": list[str],   # resolved e-MEC codes (empty if unresolved)

            # Classification
            "fields": list[str],        # subject areas / CAPES grandes áreas where known
            "document_type": str | None,  # article / book_chapter / thesis / preprint / etc.
            "language": str | None,

            # Open access
            "oa_status": str | None,    # gold / green / hybrid / diamond / closed / unknown
            "oa_url": str | None,
            "licence": str | None,

            # Metrics
            "citation_count": int | None,
            "funding": list[dict],      # [{funder, funder_id, grant_number}]

            # Innovation link
            "patent_citations": list[str],  # patent numbers citing this record

            # Provenance
            "source_url": str | None,
            "retrieved_at": str,        # ISO datetime
        }
        """
        ...

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_key(self, *args) -> str:
        payload = json.dumps(args, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json"

    def _load_cache(self, key: str) -> list[dict] | None:
        path = self._cache_path(key)
        if path.exists():
            with path.open() as f:
                return json.load(f)
        return None

    def _save_cache(self, key: str, records: list[dict]) -> None:
        with self._cache_path(key).open("w") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.time()
