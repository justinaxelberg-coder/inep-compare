"""
Open Access scorer.

Evaluates OA coverage and documentation quality for a set of records.
Uses Unpaywall as ground truth for OA status validation.

Dimensions scored:
  - oa_percentage:        share of records with any OA version
  - oa_by_route:          gold / green / hybrid / diamond / closed breakdown
  - licence_completeness: share of OA records with a machine-readable licence
  - diamond_detection:    share of diamond OA correctly identified vs. Unpaywall
  - oa_documentation:     does the source expose OA metadata at all?
  - source_vs_unpaywall:  agreement rate between source OA claims and Unpaywall
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scoring.confidence import ProportionEstimate, wilson_estimate

logger = logging.getLogger(__name__)

OA_ROUTES = ("gold", "green", "hybrid", "diamond", "bronze", "closed", "unknown")


@dataclass
class OAResult:
    source: str
    e_mec_code: str
    institution_name: str

    n_records: int = 0
    n_with_oa_metadata: int = 0          # source provides any OA field

    # OA route counts
    route_counts: dict[str, int] = field(default_factory=dict)

    # Proportion estimates
    oa_rate: ProportionEstimate | None = None
    diamond_rate: ProportionEstimate | None = None
    licence_completeness: ProportionEstimate | None = None

    # Validation against Unpaywall
    n_validated: int = 0                 # records cross-checked with Unpaywall
    n_agreement: int = 0                 # source OA status agrees with Unpaywall
    unpaywall_agreement: ProportionEstimate | None = None

    # Documentation quality flag
    oa_metadata_present: bool = False    # source exposes any OA fields
    licence_metadata_present: bool = False

    def as_dict(self) -> dict:
        result = {
            "source": self.source,
            "e_mec_code": self.e_mec_code,
            "institution_name": self.institution_name,
            "n_records": self.n_records,
            "n_with_oa_metadata": self.n_with_oa_metadata,
            "oa_metadata_present": self.oa_metadata_present,
            "licence_metadata_present": self.licence_metadata_present,
            "route_counts": self.route_counts,
        }
        if self.oa_rate:
            result["oa_rate"] = self.oa_rate.as_dict()
        if self.diamond_rate:
            result["diamond_rate"] = self.diamond_rate.as_dict()
        if self.licence_completeness:
            result["licence_completeness"] = self.licence_completeness.as_dict()
        if self.unpaywall_agreement:
            result["unpaywall_agreement"] = self.unpaywall_agreement.as_dict()
        return result


class OAScorer:
    """
    Computes open access metrics for a set of records from one source,
    for one institution.

    Usage:
        scorer = OAScorer(source="openalex")
        result = scorer.score(
            records=openalex_records,
            unpaywall_lookup={doi: oa_record},   # optional Unpaywall validation
            institution={...}
        )
    """

    def __init__(self, source: str):
        self.source = source

    def score(
        self,
        records: list[dict],
        institution: dict,
        unpaywall_lookup: dict[str, dict] | None = None,
    ) -> OAResult:
        """
        Score OA coverage and documentation quality.

        Args:
            records:           normalised publication records from source
            institution:       institution dict from registry
            unpaywall_lookup:  {doi: unpaywall_record} for cross-validation
        """
        result = OAResult(
            source=self.source,
            e_mec_code=institution.get("e_mec_code", ""),
            institution_name=institution.get("name", ""),
            n_records=len(records),
        )

        if not records:
            return result

        # OA metadata presence
        result.n_with_oa_metadata = sum(
            1 for r in records if r.get("oa_status") is not None
        )
        result.oa_metadata_present = result.n_with_oa_metadata > 0
        result.licence_metadata_present = any(r.get("licence") for r in records)

        # Route counts
        route_counts: dict[str, int] = {route: 0 for route in OA_ROUTES}
        for r in records:
            status = r.get("oa_status") or "unknown"
            route_counts[status] = route_counts.get(status, 0) + 1
        result.route_counts = route_counts

        # OA rate (any route except closed/unknown)
        n_oa = sum(
            route_counts.get(r, 0)
            for r in ("gold", "green", "hybrid", "diamond", "bronze")
        )
        result.oa_rate = wilson_estimate(k=n_oa, n=len(records))

        # Diamond rate
        n_diamond = route_counts.get("diamond", 0)
        result.diamond_rate = wilson_estimate(k=n_diamond, n=len(records))

        # Licence completeness — among OA records, how many have a licence?
        oa_records = [r for r in records if r.get("oa_status") not in (None, "closed", "unknown")]
        n_with_licence = sum(1 for r in oa_records if r.get("licence"))
        if oa_records:
            result.licence_completeness = wilson_estimate(
                k=n_with_licence, n=len(oa_records)
            )

        # Unpaywall cross-validation
        if unpaywall_lookup:
            result = self._validate_against_unpaywall(
                result, records, unpaywall_lookup
            )

        return result

    def _validate_against_unpaywall(
        self,
        result: OAResult,
        records: list[dict],
        unpaywall_lookup: dict[str, dict],
    ) -> OAResult:
        """
        Cross-validate source OA claims against Unpaywall ground truth.
        Only records with DOIs that appear in Unpaywall are validated.
        """
        validated = 0
        agreement = 0

        for r in records:
            doi = r.get("doi")
            if not doi:
                continue
            uw = unpaywall_lookup.get(doi)
            if not uw:
                continue

            validated += 1
            source_oa = r.get("oa_status") or "unknown"
            uw_oa = uw.get("oa_status") or "unknown"

            # Agreement: both open or both closed (exact route match too strict)
            source_is_open = source_oa not in ("closed", "unknown")
            uw_is_open = uw_oa not in ("closed", "unknown")

            if source_is_open == uw_is_open:
                agreement += 1

        result.n_validated = validated
        result.n_agreement = agreement

        if validated > 0:
            result.unpaywall_agreement = wilson_estimate(k=agreement, n=validated)
            logger.info(
                f"[oa_scorer] {self.source} × {result.e_mec_code}: "
                f"Unpaywall agreement {agreement}/{validated} "
                f"({result.unpaywall_agreement.estimate:.1%})"
            )

        return result

    def summarise_by_route(self, results: list[OAResult]) -> dict[str, ProportionEstimate]:
        """
        Aggregate OA route breakdown across multiple institutions.
        Returns pooled Wilson estimates per route.
        """
        from scoring.confidence import pooled_wilson_estimate

        totals: dict[str, list] = {route: [] for route in OA_ROUTES}
        total_n = []

        for r in results:
            total_n.append(r.n_records)
            for route in OA_ROUTES:
                totals[route].append(r.route_counts.get(route, 0))

        return {
            route: pooled_wilson_estimate(ks=totals[route], ns=total_n)
            for route in OA_ROUTES
            if sum(totals[route]) > 0
        }
