"""
Source fitness scoring.

Aggregates per-dimension evidence into a FitnessMatrix:
  sources × institution_types → weighted composite score

Dimensions:
  1. coverage        — from CoverageResult (dynamic, per institution)
  2. data_quality    — from CoverageResult fields + convergence overlap
  3. reliability     — from convergence inter-source agreement + static scores
  4. accessibility   — static scores (Barcelona alignment, licensing, cost)
  5. social_impact   — from OAResult + static scores
  6. governance      — static scores
  7. innovation_link — from LensConnector patent summary (if available)

Static scores loaded from: scoring/static_scores.yaml
Weights loaded from:       config/scoring_weights.yaml
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

_STATIC_PATH  = Path(__file__).parent / "static_scores.yaml"
_WEIGHTS_PATH = Path(__file__).parents[1] / "config" / "scoring_weights.yaml"


def _load_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class FitnessProfile:
    """Scores for one source × one institution type."""
    source: str
    inst_type: str
    coverage: float
    data_quality: float
    reliability: float
    accessibility: float
    social_impact: float
    governance: float
    innovation_link: float
    composite: float
    evidence: dict = field(default_factory=dict)

    def keys(self):
        return {"coverage", "data_quality", "reliability", "accessibility",
                "social_impact", "governance", "innovation_link", "composite"}

    def __getitem__(self, key):
        return getattr(self, key)


@dataclass
class FitnessMatrix:
    """All profiles for all source × institution type combinations."""
    rows: list[FitnessProfile] = field(default_factory=list)

    def rank_by_inst_type(self, inst_type: str) -> list[dict]:
        matching = [r for r in self.rows if r.inst_type == inst_type]
        return sorted(
            [{"source": r.source, "composite": r.composite,
              "coverage": r.coverage, "accessibility": r.accessibility} for r in matching],
            key=lambda x: -x["composite"],
        )

    def to_records(self) -> list[dict]:
        return [
            {
                "source": r.source, "inst_type": r.inst_type,
                "coverage": round(r.coverage, 3),
                "data_quality": round(r.data_quality, 3),
                "reliability": round(r.reliability, 3),
                "accessibility": round(r.accessibility, 3),
                "social_impact": round(r.social_impact, 3),
                "governance": round(r.governance, 3),
                "innovation_link": round(r.innovation_link, 3),
                "composite": round(r.composite, 3),
            }
            for r in self.rows
        ]


class FitnessScorer:
    """
    Aggregates evidence into fitness profiles.

    Usage:
        scorer = FitnessScorer()
        matrix = scorer.build_matrix(
            coverage_by_source_type,
            oa_by_source_type,
            convergence,
            patent_by_source_type,   # optional
        )
    """

    def __init__(
        self,
        static_path: Path = _STATIC_PATH,
        weights_path: Path = _WEIGHTS_PATH,
    ):
        self.static  = _load_yaml(static_path).get("sources", {})
        raw_w        = _load_yaml(weights_path)
        self.dim_w   = raw_w.get("dimension_weights", {})
        _DIMENSION_KEYS = {"coverage", "data_quality", "reliability", "accessibility",
                           "social_impact", "governance", "innovation_link"}
        self.sub_w = {k: raw_w[k] for k in raw_w if k in _DIMENSION_KEYS}

    def build_profile(
        self,
        source_id: str,
        inst_type: str,
        coverage: dict,
        oa: dict,
        convergence: dict,
        patents: dict | None = None,
    ) -> FitnessProfile:
        static = self.static.get(source_id, {})

        cov_score  = self._score_coverage(coverage)
        dq_score   = self._score_data_quality(coverage, convergence, source_id)
        rel_score  = self._score_reliability(convergence, source_id, static)
        acc_score  = self._score_accessibility(static)
        si_score   = self._score_social_impact(oa, static)
        gov_score  = self._score_governance(static)
        inn_score  = self._score_innovation_link(patents)

        weights   = self.dim_w
        total_w   = sum(weights.values()) or 1.0
        composite = (
            weights.get("coverage", 0.20)        * cov_score +
            weights.get("data_quality", 0.20)    * dq_score +
            weights.get("reliability", 0.15)     * rel_score +
            weights.get("accessibility", 0.20)   * acc_score +
            weights.get("social_impact", 0.10)   * si_score +
            weights.get("governance", 0.10)      * gov_score +
            weights.get("innovation_link", 0.05) * inn_score
        ) / total_w

        return FitnessProfile(
            source=source_id, inst_type=inst_type,
            coverage=cov_score, data_quality=dq_score, reliability=rel_score,
            accessibility=acc_score, social_impact=si_score,
            governance=gov_score, innovation_link=inn_score,
            composite=min(1.0, max(0.0, composite)),
            evidence={"coverage_input": coverage, "oa_input": oa, "patents_input": patents},
        )

    def build_matrix(
        self,
        coverage_by_source_type: dict[str, dict[str, dict]],
        oa_by_source_type: dict[str, dict[str, dict]],
        convergence: dict,
        patent_by_source_type: dict | None = None,
    ) -> FitnessMatrix:
        rows: list[FitnessProfile] = []
        for source_id, type_map in coverage_by_source_type.items():
            for inst_type, cov in type_map.items():
                oa      = (oa_by_source_type.get(source_id) or {}).get(inst_type, {})
                patents = ((patent_by_source_type or {}).get(source_id) or {}).get(inst_type)
                profile = self.build_profile(
                    source_id=source_id, inst_type=inst_type,
                    coverage=cov, oa=oa, convergence=convergence, patents=patents,
                )
                rows.append(profile)
        return FitnessMatrix(rows=rows)

    def _score_coverage(self, cov: dict) -> float:
        # NOTE: geographic_bias and equity_representation are declared in scoring_weights.yaml
        # but not yet computable from API data alone. Excluded from total_w intentionally.
        # TODO: add when INEP Microdados geographic data is integrated.
        w = self.sub_w.get("coverage", {})
        keys = ("institutional_coverage", "field_coverage", "temporal_coverage", "language_coverage")
        total_w = sum(w.get(k, 0.25) for k in keys) or 1.0
        score = sum(w.get(k, 0.25) * float(cov.get(k, 0)) for k in keys)
        return min(1.0, score / total_w)

    def _score_data_quality(self, cov: dict, convergence: dict, source_id: str) -> float:
        doi_rate     = float(cov.get("doi_coverage_rate", 0.5))
        overlap_vals = [v.get("overlap_pct", 0) for k, v in convergence.items()
                        if source_id in k]
        avg_overlap  = sum(overlap_vals) / len(overlap_vals) if overlap_vals else 0.5
        # timeliness = 1.0 because all queries use the same temporal window.
        # TODO: derive from date_published distribution when longitudinal data available.
        timeliness   = 1.0
        w = self.sub_w.get("data_quality", {})
        total_w = (w.get("completeness", 0.30) + w.get("disambiguation_quality", 0.30) +
                   w.get("timeliness", 0.20)) or 1.0
        return min(1.0, (
            w.get("completeness", 0.30)           * doi_rate +
            w.get("disambiguation_quality", 0.30) * avg_overlap +
            w.get("timeliness", 0.20)             * timeliness
        ) / total_w)

    def _score_reliability(self, convergence: dict, source_id: str, static: dict) -> float:
        # NOTE: temporal_stability (weight 0.25) requires multi-year comparison runs.
        # Excluded from scoring until longitudinal data is available.
        overlap_vals = [v.get("overlap_pct", 0) for k, v in convergence.items()
                        if source_id in k]
        inter_src    = sum(overlap_vals) / len(overlap_vals) if overlap_vals else 0.5
        rel_static   = static.get("reliability", {})
        meth_transp  = float(rel_static.get("methodological_transparency", 0.5))
        repro        = float(rel_static.get("reproducibility", 0.5))
        w = self.sub_w.get("reliability", {})
        total_w = (w.get("inter_source_agreement", 0.30) +
                   w.get("methodological_transparency", 0.25) +
                   w.get("reproducibility", 0.20)) or 1.0
        return min(1.0, (
            w.get("inter_source_agreement", 0.30)      * inter_src +
            w.get("methodological_transparency", 0.25) * meth_transp +
            w.get("reproducibility", 0.20)             * repro
        ) / total_w)

    def _score_accessibility(self, static: dict) -> float:
        acc = static.get("accessibility", {})
        w   = self.sub_w.get("accessibility", {})
        keys = ("barcelona_alignment", "api_availability", "licensing_model",
                "cost_to_br_hei", "practical_usability")
        total_w = sum(w.get(k, 0.2) for k in keys) or 1.0
        return min(1.0, sum(w.get(k, 0.2) * float(acc.get(k, 0.5)) for k in keys) / total_w)

    def _score_social_impact(self, oa: dict, static: dict) -> float:
        # NOTE: sdg_coverage (weight 0.25) is declared in YAML but excluded here —
        # SDG tagging is available in OpenAlex/Dimensions but not yet aggregated into
        # the social_impact input dict. TODO: add sdg_rate field to OA results.
        oa_rate   = float(oa.get("oa_rate", 0))
        si_static = static.get("social_impact", {})
        policy    = float(si_static.get("policy_citations", 0.2))
        engage    = float(si_static.get("public_engagement", 0.2))
        geo       = float(si_static.get("geographic_social_context", 0.5))
        w = self.sub_w.get("social_impact", {})
        total_w = (w.get("oa_percentage", 0.20) + w.get("policy_citations", 0.20) +
                   w.get("public_engagement", 0.20) + w.get("geographic_social_context", 0.15)) or 1.0
        return min(1.0, (
            w.get("oa_percentage", 0.20)             * oa_rate +
            w.get("policy_citations", 0.20)          * policy +
            w.get("public_engagement", 0.20)         * engage +
            w.get("geographic_social_context", 0.15) * geo
        ) / total_w)

    def _score_governance(self, static: dict) -> float:
        gov = static.get("governance", {})
        w   = self.sub_w.get("governance", {})
        keys = ("ownership_transparency", "sustainability_risk",
                "community_governance", "data_portability")
        total_w = sum(w.get(k, 0.25) for k in keys) or 1.0
        return min(1.0, sum(w.get(k, 0.25) * float(gov.get(k, 0.5)) for k in keys) / total_w)

    def _score_innovation_link(self, patents: dict | None) -> float:
        if not patents:
            return 0.0
        w          = self.sub_w.get("innovation_link", {})
        total      = patents.get("patent_count", 0)
        intl       = patents.get("intl_patent_families", 0)
        links      = patents.get("unique_npl_papers", 0)
        pat_score  = min(1.0, total / 50)
        intl_score = min(1.0, intl  / 20)
        link_score = min(1.0, links / 20)
        total_w = (w.get("npl_link_rate", 0.40) +
                   w.get("patent_count_score", 0.30) +
                   w.get("intl_family_score", 0.30)) or 1.0
        return min(1.0, (
            w.get("npl_link_rate", 0.40)      * link_score +
            w.get("patent_count_score", 0.30) * pat_score +
            w.get("intl_family_score", 0.30)  * intl_score
        ) / total_w)
