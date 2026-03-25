# enrichment/geographic.py
from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def compute_coverage_gap(registry: pd.DataFrame, indexed: set[str]) -> dict[str, float]:
    """Return {region: observed_rate - expected_rate}. Negative = under-indexed."""
    total = len(registry)
    total_indexed = len(indexed)
    if total == 0 or total_indexed == 0:
        return {}
    result = {}
    for region, grp in registry.groupby("region"):
        expected = len(grp) / total
        observed = len([c for c in grp["e_mec_code"].astype(str) if c in indexed]) / total_indexed
        result[str(region)] = observed - expected
    return result


def compute_output_gap(registry: pd.DataFrame,
                       pub_counts: dict[str, int]) -> dict[str, float]:
    """Return {region: mean(pubs/faculty_with_phd)} excluding zero-faculty rows."""
    result = {}
    valid = registry[registry["faculty_with_phd"].fillna(0) > 0].copy()
    valid["e_mec_str"] = valid["e_mec_code"].astype(str)
    valid["pubs"] = valid["e_mec_str"].map(pub_counts).fillna(0)
    valid["rate"] = valid["pubs"] / valid["faculty_with_phd"]
    for region, grp in valid.groupby("region"):
        result[str(region)] = float(grp["rate"].mean())
    return result


def compute_geographic_bias_score(coverage_gaps: dict[str, float]) -> float:
    """Score 0-1: 1.0 = perfectly proportional, lower = more biased."""
    if not coverage_gaps:
        return 0.0
    raw = 1.0 - sum(abs(v) for v in coverage_gaps.values()) / len(coverage_gaps)
    return max(0.0, min(1.0, raw))


def compute_coverage_gap_stratified(
    registry: pd.DataFrame,
    indexed: set[str],
    source: str,
) -> list[dict]:
    """Return stratified rows for (source × inst_type × region) coverage gap.

    Args:
        registry: DataFrame with columns e_mec_code, region, inst_type, faculty_with_phd
        indexed: set of e_mec_code strings that appear in the source's coverage
        source: source identifier string (e.g. "openalex", "scopus")

    Returns list of dicts matching stratified schema (source, inst_type, region,
    sub_dimension, value, n_papers, confidence_tier).

    Value: 1.0 = perfectly proportional, 0.0 = maximally biased.
    Negative gap = under-indexed for this stratum.
    """
    from enrichment.stratified import make_stratum_row
    rows = []
    total = len(registry)
    total_indexed = len(indexed)
    if total == 0 or total_indexed == 0:
        return rows

    for (inst_type, region), grp in registry.groupby(["inst_type", "region"]):
        expected = len(grp) / total
        grp_codes = set(grp["e_mec_code"].astype(str))
        observed = len(grp_codes & indexed) / total_indexed
        gap = observed - expected  # negative = under-indexed
        bias_score = max(0.0, min(1.0, 1.0 - abs(gap) * 2))
        rows.append(make_stratum_row(
            source=source,
            inst_type=str(inst_type),
            region=str(region),
            sub_dimension="geographic_coverage_gap",
            value=bias_score,
            n_papers=len(grp),
        ))
    return rows


def load_and_compute(registry_path: str, pub_counts: dict[str, int],
                     source: str) -> dict | None:
    """Load registry and return bias metrics. Returns None if registry absent."""
    path = Path(registry_path)
    if not path.exists():
        logger.warning("Registry not found at %s — geographic_bias skipped for %s", path, source)
        return None
    registry = pd.read_csv(path)
    indexed = set(str(k) for k in pub_counts.keys())
    coverage_gaps = compute_coverage_gap(registry, indexed)
    output_gaps = compute_output_gap(registry, pub_counts)
    bias_score = compute_geographic_bias_score(coverage_gaps)
    return {
        "coverage_gaps": coverage_gaps,
        "output_gaps": output_gaps,
        "geographic_bias_score": bias_score,
    }
