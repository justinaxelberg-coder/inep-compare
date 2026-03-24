"""
Unpaywall connector.

Validates OA metadata from other sources against Unpaywall ground truth.

IMPORTANT: OpenAlex already ingests Unpaywall data and returns open_access
for every record. Unpaywall's role here is *validation*, not primary lookup.
We sample a subset of DOIs per institution rather than looking up every record.

Requests are parallelised via ThreadPoolExecutor to avoid timeout on large samples.

API docs: https://unpaywall.org/products/api
Requires: UNPAYWALL_EMAIL env var (free, mandatory for API access)
"""

from __future__ import annotations

import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

UNPAYWALL_BASE = "https://api.unpaywall.org/v2"


class UnpaywallConnector(BaseConnector):
    """
    Unpaywall connector — parallel DOI-sample OA validation.

    Primary use: validate OA metadata from OpenAlex/Scopus/WoS on a sample.
    Not for full-corpus lookup — OpenAlex already carries Unpaywall data.
    """

    source_id = "unpaywall"
    source_name = "Unpaywall"

    def __init__(
        self,
        email: str | None = None,
        cache_dir: str = "data/raw",
        max_records: int = 500,
        rate_limit_seconds: float = 0.1,
        validation_sample_size: int = 100,   # max DOIs to validate per institution
        max_workers: int = 10,               # parallel threads
    ):
        super().__init__(cache_dir=cache_dir, max_records=max_records,
                         rate_limit_seconds=rate_limit_seconds)
        self.email = email or os.environ.get("UNPAYWALL_EMAIL", "")
        if not self.email:
            raise ValueError(
                "Unpaywall requires an email address. "
                "Set UNPAYWALL_EMAIL env var or pass email= to constructor."
            )
        self.validation_sample_size = validation_sample_size
        self.max_workers = max_workers

    # ------------------------------------------------------------------
    # Validation sample lookup — main entry point
    # ------------------------------------------------------------------

    def lookup_dois(self, dois: list[str], sample: bool = True) -> dict[str, dict]:
        """
        Look up OA status for a list of DOIs.

        Args:
            dois:   list of DOIs (may include None/empty, skipped silently)
            sample: if True, randomly sample up to validation_sample_size DOIs
                    rather than looking up the full list. Sampled DOIs are
                    representative for validation purposes.

        Returns dict mapping DOI -> normalised OA record.
        """
        clean_dois = [d for d in dois if d]

        if sample and len(clean_dois) > self.validation_sample_size:
            sampled = random.sample(clean_dois, self.validation_sample_size)
            logger.info(
                f"[unpaywall] Sampling {self.validation_sample_size} / "
                f"{len(clean_dois)} DOIs for validation"
            )
        else:
            sampled = clean_dois
            logger.info(f"[unpaywall] Looking up {len(sampled)} DOIs")

        # Check cache first, collect uncached DOIs
        results: dict[str, dict] = {}
        uncached: list[str] = []

        for doi in sampled:
            cached = self._load_cache(self._cache_key(doi))
            if cached:
                results[doi] = cached[0]
            else:
                uncached.append(doi)

        logger.info(
            f"[unpaywall] {len(results)} cached, {len(uncached)} to fetch "
            f"(parallel, {self.max_workers} workers)"
        )

        # Parallel fetch for uncached DOIs
        if uncached:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._fetch_and_cache, doi): doi
                           for doi in uncached}
                for future in as_completed(futures):
                    doi = futures[future]
                    try:
                        record = future.result()
                        if record:
                            results[doi] = record
                    except Exception as e:
                        logger.warning(f"[unpaywall] Failed {doi}: {e}")

        logger.info(f"[unpaywall] Retrieved {len(results)} records")
        return results

    def _fetch_and_cache(self, doi: str) -> dict | None:
        """Fetch a single DOI and cache the result. Called from thread pool."""
        record = self._fetch_doi(doi)
        if record:
            normalised = self.normalize(record)
            self._save_cache(self._cache_key(doi), [normalised])
            return normalised
        return None

    # ------------------------------------------------------------------
    # BaseConnector interface (institution-level query — not primary use)
    # ------------------------------------------------------------------

    def _fetch(self, e_mec_code, ror_id, name, start_year, end_year) -> list[dict]:
        """
        Unpaywall doesn't support institution-level queries.
        This connector is used via lookup_dois(), not query_institution().
        """
        raise NotImplementedError(
            "Unpaywall works per DOI. Use lookup_dois() instead of query_institution()."
        )

    # ------------------------------------------------------------------
    # Per-DOI fetch
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _fetch_doi(self, doi: str) -> dict | None:
        """Fetch a single DOI from Unpaywall."""
        # Normalise DOI — strip prefix if present
        doi_clean = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")

        url = f"{UNPAYWALL_BASE}/{doi_clean}"
        with httpx.Client(timeout=15) as client:
            response = client.get(url, params={"email": self.email})

        if response.status_code == 404:
            logger.debug(f"[unpaywall] DOI not found: {doi_clean}")
            return None
        if response.status_code == 422:
            logger.debug(f"[unpaywall] Invalid DOI format: {doi_clean}")
            return None

        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Normalise
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """
        Map Unpaywall response to OA schema.
        This is a focused schema — just OA fields — not the full publication schema.
        Merged into publication records in the scoring layer.
        """
        best_oa = raw.get("best_oa_location") or {}

        # Determine OA route
        oa_status = self._classify_oa(raw)

        # All OA locations (published, accepted, submitted versions)
        locations = raw.get("oa_locations", [])
        versions = list({loc.get("version") for loc in locations if loc.get("version")})

        return {
            "source": self.source_id,
            "doi": raw.get("doi"),
            "title": raw.get("title"),
            "year": raw.get("year"),
            "oa_status": oa_status,
            "oa_url": best_oa.get("url"),
            "oa_url_for_pdf": best_oa.get("url_for_pdf"),
            "licence": best_oa.get("license"),
            "host_type": best_oa.get("host_type"),  # publisher | repository
            "version": best_oa.get("version"),       # published | accepted | submitted
            "all_versions_available": versions,
            "repository_institution": best_oa.get("repository_institution"),
            "n_oa_locations": len(locations),
            "journal_is_oa": raw.get("journal_is_oa", False),
            "journal_issns": raw.get("journal_issns", []),
            "publisher": raw.get("publisher"),
            "updated": raw.get("updated"),
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    def _classify_oa(self, raw: dict) -> str:
        """
        Classify OA status with diamond OA detection.

        Diamond OA: journal is OA AND no APC (author pays). Unpaywall doesn't
        directly flag diamond, so we proxy it: journal_is_oa=True and
        best location is publisher-hosted with no APC signals.
        SciELO URLs are a strong signal for diamond OA in the Brazilian context.
        """
        if not raw.get("is_oa"):
            return "closed"

        best = raw.get("best_oa_location") or {}
        host = best.get("host_type", "")
        url = best.get("url", "") or ""
        journal_is_oa = raw.get("journal_is_oa", False)

        # Diamond OA heuristic: publisher-hosted, journal is fully OA,
        # and URL signals known diamond infrastructure
        diamond_signals = ["scielo.org", "redalyc.org", "doaj.org"]
        if journal_is_oa and host == "publisher" and any(s in url for s in diamond_signals):
            return "diamond"

        if journal_is_oa and host == "publisher":
            return "gold"

        if host == "repository":
            return "green"

        # Hybrid: publisher hosted but journal is not fully OA
        if host == "publisher" and not journal_is_oa:
            return "hybrid"

        return "unknown"
