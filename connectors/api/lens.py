"""
The Lens connector (Cambia / Lens.org).

Queries both endpoints:
  - /scholarly/search  — papers (fits the common record schema; used in convergence)
  - /patent/search     — patents with NPL back-references to papers (innovation dimension)

Auth: Bearer token via LENS_API_KEY env var.
Fully open infrastructure — high Barcelona Declaration alignment.

API docs: https://docs.lens.org/
Free scholarly API is rate-limited; patent API requires registration.

Key innovation fields produced:
  - patent_count          (per institution, from fetch_patents())
  - intl_patent_families  (PCT/EPO designations — measures global reach)
  - paper_patent_link     (scholarly DOIs cited in patent NPLs — research→innovation bridge)
  - ipc_codes             (IPC technology classification)
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

LENS_SCHOLARLY_URL = "https://api.lens.org/scholarly/search"
LENS_PATENT_URL    = "https://api.lens.org/patent/search"
PAGE_SIZE          = 500   # max per request

SCHOLARLY_INCLUDE = [
    "lens_id", "doi", "title", "year_published", "date_published",
    "authors", "source", "open_access", "citations_count",
    "fields_of_study", "keywords", "abstract", "funding",
    "scholarly_citations_count",
]

PATENT_INCLUDE = [
    "lens_id", "pub_number", "pub_key", "title", "year_published",
    "date_published", "applicant", "inventor", "claims",
    "ipc_classifications", "cpc_classifications",
    "npl_resolved_lens_id",          # scholarly works cited by this patent
    "patent_citation",               # patents citing this patent
    "cited_by",                      # works citing this patent
    "families",                      # patent family members (intl reach)
    "jurisdictions",                 # country codes
]


def _bare_ror(ror_id: str | None) -> str | None:
    """Strip https://ror.org/ prefix → bare alphanumeric ROR ID."""
    if not ror_id:
        return None
    return ror_id.replace("https://ror.org/", "").replace("http://ror.org/", "").strip("/")


class LensConnector(BaseConnector):
    """
    The Lens connector.

    Scholarly records slot into the convergence engine exactly like OpenAlex/Scopus.
    Patent records are fetched separately via fetch_patents() and stored under
    data/raw/lens_patents/.

    Barcelona alignment: high — fully open, no proprietary lock-in, ROR-native.
    """

    source_id   = "lens"
    source_name = "The Lens"

    def __init__(self, cache_dir: str = "data/raw", max_records: int = 500,
                 rate_limit_seconds: float = 1.0):
        super().__init__(cache_dir=cache_dir, max_records=max_records,
                         rate_limit_seconds=rate_limit_seconds)
        self.api_key = os.environ.get("LENS_API_KEY", "")
        if not self.api_key:
            logger.warning("[lens] LENS_API_KEY not set — API calls will fail")
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        # Separate cache dir for patents
        self._patent_cache_dir = (
            self.cache_dir.parent / "lens_patents"
        )
        self._patent_cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Scholarly endpoint (fits common paper schema)
    # ------------------------------------------------------------------

    def _fetch(
        self,
        e_mec_code: str,
        ror_id: str | None,
        name: str | None,
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        """Fetch scholarly records from The Lens for a single institution."""
        bare_ror = _bare_ror(ror_id)
        if not bare_ror:
            logger.warning(f"[lens] No ROR ID for {e_mec_code} — cannot query scholarly")
            return []

        payload = self._build_scholarly_payload(bare_ror, start_year, end_year)
        return self._paginate(LENS_SCHOLARLY_URL, payload, self.max_records)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def _post(self, url: str, payload: dict) -> dict:
        """POST to Lens API with retry."""
        self._rate_limit()
        with httpx.Client(timeout=60) as client:
            resp = client.post(url, json=payload, headers=self._headers)
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, url: str, payload: dict, limit: int) -> list[dict]:
        """Page through Lens results up to limit."""
        results: list[dict] = []
        offset = 0
        page_size = min(PAGE_SIZE, limit)

        while True:
            payload = {**payload, "size": page_size, "from": offset}
            data = self._post(url, payload)
            hits = data.get("data") or data.get("results") or []
            if not hits:
                break
            results.extend(hits)
            if len(results) >= limit:
                results = results[:limit]
                logger.warning(f"[lens] Ceiling hit at {limit} records")
                break
            total = data.get("total", 0)
            if offset + page_size >= total:
                break
            offset += len(hits)

        return results

    def _build_scholarly_payload(
        self, bare_ror: str, start_year: int, end_year: int
    ) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"authors.affiliation.institution.ror_id": bare_ror}},
                        {"range": {"year_published": {"gte": start_year, "lte": end_year}}},
                    ]
                }
            },
            "include": SCHOLARLY_INCLUDE,
        }

    # ------------------------------------------------------------------
    # Patent endpoint
    # ------------------------------------------------------------------

    def fetch_patents(
        self,
        e_mec_code: str,
        ror_id: str | None,
        name: str | None,
        start_year: int,
        end_year: int,
        use_cache: bool = True,
    ) -> list[dict]:
        """
        Fetch patent records where the institution appears as applicant.

        Returns raw patent dicts (not normalised to the paper schema).
        Use normalize_patent() to get the canonical patent record.
        """
        bare_ror = _bare_ror(ror_id)
        if not bare_ror:
            logger.warning(f"[lens] No ROR ID for {e_mec_code} — cannot query patents")
            return []

        cache_key = self._cache_key(
            e_mec_code, ror_id, name, start_year, end_year
        ) + "_patents"
        cache_path = self._patent_cache_dir / f"{cache_key}.json"

        if use_cache and cache_path.exists():
            import json
            logger.debug(f"[lens_patents] Cache hit for {e_mec_code}")
            return json.loads(cache_path.read_text(encoding="utf-8"))

        payload = self._build_patent_payload(bare_ror, start_year, end_year)
        records = self._paginate(LENS_PATENT_URL, payload, self.max_records)

        import json
        cache_path.write_text(
            json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info(f"[lens_patents] {e_mec_code}: {len(records)} patents fetched")
        return records

    def _build_patent_payload(
        self, bare_ror: str, start_year: int, end_year: int
    ) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"applicant.ror_id": bare_ror}},
                        {"range": {"year_published": {"gte": start_year, "lte": end_year}}},
                    ]
                }
            },
            "include": PATENT_INCLUDE,
        }

    # ------------------------------------------------------------------
    # Normalisation — scholarly (common paper schema)
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Map a raw Lens scholarly record to the common schema."""
        oa_info = raw.get("open_access") or {}

        authors = []
        for a in raw.get("authors") or []:
            affiliations = []
            for aff in a.get("affiliations") or []:
                inst = aff.get("institution") or {}
                affiliations.append({
                    "name": inst.get("name") or aff.get("name"),
                    "ror_id": inst.get("ror_id"),
                    "country": aff.get("country_code"),
                })
            authors.append({
                "name": a.get("display_name") or (
                    f"{a.get('last_name', '')}, {a.get('first_name', '')}".strip(", ")
                ),
                "orcid": a.get("orcid"),
                "affiliations": affiliations,
            })

        source = raw.get("source") or {}
        doi = raw.get("doi")

        return {
            "source":             self.source_id,
            "source_record_id":   raw.get("lens_id", ""),
            "doi":                f"https://doi.org/{doi}" if doi else None,
            "title":              raw.get("title"),
            "year":               raw.get("year_published"),
            "date_published":     raw.get("date_published"),
            "authors":            authors,
            "journal":            source.get("title"),
            "issn":               (source.get("issn") or [None])[0],
            "document_type":      "article",   # Lens scholarly = peer-reviewed by default
            "language":           None,         # not returned by Lens scholarly API
            "oa_status":          self._classify_oa(oa_info),
            "oa_url":             oa_info.get("pdf_url") or oa_info.get("license_url"),
            "cited_by_count":     raw.get("citations_count") or raw.get("scholarly_citations_count"),
            "fwci":               None,         # not available in Lens scholarly
            "keywords":           raw.get("keywords") or [],
            "fields_of_study":    raw.get("fields_of_study") or [],
            "funders":            self._extract_funders(raw),
            "sdg_labels":         [],           # not available in Lens scholarly
            "institution_name":   None,         # injected by runner
            "e_mec_code":         None,         # injected by runner
        }

    def _classify_oa(self, oa_info: dict) -> str:
        """Map Lens OA info to common OA status vocabulary."""
        if not oa_info:
            return "closed"
        if oa_info.get("is_open_access"):
            colour = (oa_info.get("colour") or "").lower()
            if colour == "gold":
                return "gold"
            if colour == "green":
                return "green"
            if colour == "hybrid":
                return "hybrid"
            if colour == "bronze":
                return "bronze"
            return "open"   # unknown colour but OA
        return "closed"

    def _extract_funders(self, raw: dict) -> list[dict]:
        funders = []
        for f in raw.get("funding") or []:
            funders.append({
                "name":       f.get("org") or f.get("funder"),
                "grant_id":   f.get("funding_id") or f.get("award_id"),
                "country":    f.get("country"),
            })
        return funders

    # ------------------------------------------------------------------
    # Normalisation — patents
    # ------------------------------------------------------------------

    def normalize_patent(self, raw: dict, e_mec_code: str = "",
                         institution_name: str = "") -> dict:
        """
        Map a raw Lens patent record to the innovation schema.

        Key indicators for SINAES:
          - patent_count          (aggregate)
          - intl_patent_families  (families with ≥2 jurisdictions → global reach)
          - paper_patent_link     (NPL back-references → research→innovation bridge)
          - ipc_section           (technology domain — A–H)
        """
        applicants = []
        for ap in raw.get("applicant") or []:
            applicants.append({
                "name":   ap.get("name"),
                "ror_id": ap.get("ror_id"),
                "type":   ap.get("type"),        # "university" / "company" / "individual"
                "country": ap.get("country"),
            })

        inventors = []
        for inv in raw.get("inventor") or []:
            inventors.append({
                "name":    inv.get("name"),
                "country": inv.get("country"),
            })

        # IPC / CPC classification
        ipc_codes = [
            c.get("code", "") for c in (raw.get("ipc_classifications") or [])
        ]
        ipc_sections = list({c[0] for c in ipc_codes if c})   # single-letter section

        # NPL (non-patent literature) references → links to scholarly papers
        npl_lens_ids = raw.get("npl_resolved_lens_id") or []

        # Family members → international reach
        families = raw.get("families") or []
        jurisdictions = raw.get("jurisdictions") or []
        intl_family = len(set(jurisdictions)) >= 2

        return {
            "source":               self.source_id,
            "record_type":          "patent",
            "source_record_id":     raw.get("lens_id", ""),
            "pub_number":           raw.get("pub_number"),
            "pub_key":              raw.get("pub_key"),
            "title":                (raw.get("title") or {}).get("text")
                                    if isinstance(raw.get("title"), dict)
                                    else raw.get("title"),
            "year":                 raw.get("year_published"),
            "date_published":       raw.get("date_published"),
            "applicants":           applicants,
            "inventors":            inventors,
            "ipc_codes":            ipc_codes,
            "ipc_sections":         ipc_sections,
            "npl_lens_ids":         npl_lens_ids,   # scholarly works cited
            "npl_count":            len(npl_lens_ids),
            "jurisdictions":        jurisdictions,
            "intl_patent_family":   intl_family,
            "family_size":          len(families),
            "e_mec_code":           e_mec_code,
            "institution_name":     institution_name,
        }

    # ------------------------------------------------------------------
    # Innovation summary helper (called by runner / scorer)
    # ------------------------------------------------------------------

    def summarise_patents(
        self, patents: list[dict], e_mec_code: str = "", institution_name: str = ""
    ) -> dict:
        """
        Aggregate patent records into institution-level innovation indicators.

        Returns a dict ready for the innovation scoring dimension.
        """
        if not patents:
            return {
                "e_mec_code":          e_mec_code,
                "institution_name":    institution_name,
                "patent_count":        0,
                "intl_patent_families": 0,
                "paper_patent_links":  0,
                "unique_npl_papers":   0,
                "ipc_sections":        [],
            }

        normalised = [
            self.normalize_patent(p, e_mec_code, institution_name) for p in patents
        ]
        intl = sum(1 for p in normalised if p["intl_patent_family"])
        all_npl = [lid for p in normalised for lid in p["npl_lens_ids"]]
        unique_npl = len(set(all_npl))
        all_sections: list[str] = []
        for p in normalised:
            all_sections.extend(p["ipc_sections"])

        return {
            "e_mec_code":           e_mec_code,
            "institution_name":     institution_name,
            "patent_count":         len(normalised),
            "intl_patent_families": intl,
            "paper_patent_links":   len(all_npl),
            "unique_npl_papers":    unique_npl,
            "ipc_sections":         sorted(set(all_sections)),
        }
