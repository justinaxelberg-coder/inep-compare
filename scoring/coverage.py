"""
Coverage scorer.

Evaluates how well a source covers Brazilian HEIs along five sub-dimensions:
  - institutional_coverage: % of sampled institutions present in source
  - field_coverage: breadth across CAPES knowledge areas
  - temporal_coverage: years available and update lag
  - language_coverage: Portuguese-language content indexed
  - geographic_bias: regional distribution of indexed BR output
  - equity_representation: systematic gaps by institution category

All rates are reported with Wilson confidence intervals via scoring.confidence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from scoring.confidence import ProportionEstimate, wilson_estimate, pooled_wilson_estimate

logger = logging.getLogger(__name__)

# CAPES grandes áreas — used for field coverage calculation
CAPES_AREAS = [
    "Ciências Exatas e da Terra",
    "Ciências Biológicas",
    "Engenharias",
    "Ciências da Saúde",
    "Ciências Agrárias",
    "Ciências Sociais Aplicadas",
    "Ciências Humanas",
    "Linguística, Letras e Artes",
    "Multidisciplinar",
]


@dataclass
class CoverageResult:
    source: str
    e_mec_code: str
    institution_name: str

    # Institutional presence
    institution_present: bool = False

    # Record counts
    n_records: int = 0

    # OA breakdown (used downstream by OA scorer)
    n_oa: int = 0
    n_portuguese: int = 0

    # Field coverage
    capes_areas_found: list[str] = field(default_factory=list)
    field_coverage_estimate: ProportionEstimate | None = None

    # Language coverage
    language_coverage_estimate: ProportionEstimate | None = None

    # Equity flag
    institution_category: str | None = None
    institution_org_type: str | None = None
    institution_region: str | None = None

    # Structural gap flags
    extensao_coverage: bool = False   # always False — documented gap

    def as_dict(self) -> dict:
        result = {
            "source": self.source,
            "e_mec_code": self.e_mec_code,
            "institution_name": self.institution_name,
            "institution_present": self.institution_present,
            "n_records": self.n_records,
            "institution_category": self.institution_category,
            "institution_org_type": self.institution_org_type,
            "institution_region": self.institution_region,
            "capes_areas_found": self.capes_areas_found,
            "n_capes_areas_found": len(self.capes_areas_found),
            "extensao_coverage": self.extensao_coverage,
        }
        if self.field_coverage_estimate:
            result["field_coverage"] = self.field_coverage_estimate.as_dict()
        if self.language_coverage_estimate:
            result["language_coverage"] = self.language_coverage_estimate.as_dict()
        return result


class CoverageScorer:
    """
    Computes coverage metrics for a set of records from one source,
    for one institution.

    Usage:
        scorer = CoverageScorer(source="openalex")
        result = scorer.score(
            records=openalex_records,
            institution={"e_mec_code": "000572", "name": "UFABC", ...}
        )
    """

    def __init__(self, source: str):
        self.source = source

    def score(
        self,
        records: list[dict],
        institution: dict,
    ) -> CoverageResult:
        """
        Score coverage for a single institution × source combination.

        Args:
            records:     list of normalised publication records from the source
            institution: institution dict from registry (e-MEC, name, category, etc.)
        """
        result = CoverageResult(
            source=self.source,
            e_mec_code=institution.get("e_mec_code", ""),
            institution_name=institution.get("name", ""),
            institution_category=institution.get("category"),
            institution_org_type=institution.get("org_type"),
            institution_region=institution.get("region"),
        )

        result.institution_present = len(records) > 0
        result.n_records = len(records)

        if not records:
            logger.info(
                f"[coverage] {self.source}: {institution.get('name')} — 0 records found. "
                f"Institution absent from source or below detection threshold."
            )
            return result

        # Field coverage
        result.capes_areas_found = self._detect_capes_areas(records)
        result.field_coverage_estimate = wilson_estimate(
            k=len(result.capes_areas_found),
            n=len(CAPES_AREAS),
        )

        # Language coverage — Portuguese-language records
        n_pt = sum(
            1 for r in records
            if (r.get("language") or "").lower() in ("pt", "portuguese", "por")
        )
        result.n_portuguese = n_pt
        result.language_coverage_estimate = wilson_estimate(k=n_pt, n=len(records))

        # OA count (passed to OA scorer)
        result.n_oa = sum(
            1 for r in records
            if r.get("oa_status") not in (None, "closed", "unknown")
        )

        # Extensão — always absent, documented gap
        result.extensao_coverage = False

        return result

    def score_batch(
        self,
        records_by_institution: dict[str, list[dict]],
        institutions: list[dict],
    ) -> list[CoverageResult]:
        """
        Score coverage for multiple institutions.

        Args:
            records_by_institution: {e_mec_code: [records]}
            institutions:           list of institution dicts from registry
        """
        inst_map = {i["e_mec_code"]: i for i in institutions}
        results = []

        for e_mec_code, records in records_by_institution.items():
            institution = inst_map.get(e_mec_code, {"e_mec_code": e_mec_code})
            results.append(self.score(records, institution))

        return results

    def institutional_coverage_summary(
        self, results: list[CoverageResult]
    ) -> ProportionEstimate:
        """
        Compute overall institutional coverage: what fraction of sampled
        institutions are present in this source?
        """
        n = len(results)
        k = sum(1 for r in results if r.institution_present)
        return wilson_estimate(k=k, n=n)

    def coverage_by_category(
        self, results: list[CoverageResult]
    ) -> dict[str, ProportionEstimate]:
        """
        Break down institutional coverage by institution category.
        Uses pooled Wilson estimates for robustness with small groups.
        """
        from collections import defaultdict
        groups: dict[str, list[CoverageResult]] = defaultdict(list)
        for r in results:
            if r.institution_category:
                groups[r.institution_category].append(r)

        breakdown = {}
        for category, group_results in groups.items():
            ks = [1 if r.institution_present else 0 for r in group_results]
            ns = [1] * len(group_results)
            breakdown[category] = pooled_wilson_estimate(ks, ns)

        return breakdown

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_capes_areas(self, records: list[dict]) -> list[str]:
        """
        Detect which CAPES grandes áreas are represented in the records.
        Maps OpenAlex/source concepts to CAPES areas via keyword heuristics.
        This is approximate — a proper mapping table will be added in phase 2.
        """
        found = set()
        all_fields = " ".join(
            f.lower()
            for r in records
            for f in (r.get("fields") or [])
        )

        mappings = {
            "Ciências Exatas e da Terra": [
                "mathematics", "physics", "chemistry", "geology",
                "astronomy", "computer science", "matemática", "física", "química",
            ],
            "Ciências Biológicas": [
                "biology", "ecology", "genetics", "microbiology", "zoology",
                "botany", "biologia", "ecologia",
            ],
            "Engenharias": [
                "engineering", "materials science", "engenharia",
            ],
            "Ciências da Saúde": [
                "medicine", "nursing", "pharmacy", "dentistry", "public health",
                "saúde", "medicina", "enfermagem",
            ],
            "Ciências Agrárias": [
                "agriculture", "agronomy", "veterinary", "agrária",
            ],
            "Ciências Sociais Aplicadas": [
                "economics", "business", "law", "communication", "architecture",
                "economia", "administração", "direito",
            ],
            "Ciências Humanas": [
                "history", "philosophy", "sociology", "anthropology", "psychology",
                "education", "história", "filosofia", "psicologia", "educação",
            ],
            "Linguística, Letras e Artes": [
                "linguistics", "literature", "arts", "music",
                "linguística", "letras",
            ],
        }

        for area, keywords in mappings.items():
            if any(kw in all_fields for kw in keywords):
                found.add(area)

        return sorted(found)
