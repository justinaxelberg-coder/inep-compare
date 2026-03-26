# enrichment/geographic.py
from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

GEOGRAPHIC_COLUMNS: list[str] = [
    "source",
    "inst_type",
    "region",
    "n_records",
    "source_publication_share",
    "peer_mean_share",
    "comparative_skew",
    "cohort_institution_share",
    "cohort_phd_faculty_share",
    "delta_vs_cohort_institution_share",
    "delta_vs_cohort_phd_faculty_share",
    "cohort_institutions",
]


def _normalize_e_mec_codes(series: pd.Series) -> pd.Series:
    values = series.astype(str).str.strip().str.lstrip("0")
    return values.mask(values == "", "0")


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


def build_geographic_comparison(
    coverage_df: pd.DataFrame,
    cohort_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build per-source comparative publication skew by institution type and region.

    The cohort is the evaluated crosswalk cohort, not the full national registry.
    Geography is descriptive evidence only: raw source publication shares are compared
    against peer mean shares, with cohort composition retained only as context.
    """
    if coverage_df.empty or cohort_df.empty:
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)

    cohort = cohort_df.copy()
    coverage = coverage_df.copy()

    if "sinaes_type" in cohort.columns and "inst_type" not in cohort.columns:
        cohort = cohort.rename(columns={"sinaes_type": "inst_type"})

    required_cohort = {"e_mec_code", "inst_type", "region"}
    required_cov = {"source", "e_mec_code", "n_records"}
    if not required_cohort.issubset(cohort.columns) or not required_cov.issubset(coverage.columns):
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)

    cohort["e_mec_code"] = _normalize_e_mec_codes(cohort["e_mec_code"])
    coverage["e_mec_code"] = _normalize_e_mec_codes(coverage["e_mec_code"])
    cohort["faculty_with_phd"] = pd.to_numeric(
        cohort.get("faculty_with_phd", 0), errors="coerce"
    ).fillna(0.0)
    coverage["n_records"] = pd.to_numeric(coverage["n_records"], errors="coerce").fillna(0.0)

    cohort = cohort[["e_mec_code", "inst_type", "region", "faculty_with_phd"]].drop_duplicates()
    coverage = coverage[["source", "e_mec_code", "n_records"]]

    strata = (
        cohort.groupby(["inst_type", "region"], dropna=False)
        .agg(
            cohort_institutions=("e_mec_code", "nunique"),
            cohort_phd_faculty_total=("faculty_with_phd", "sum"),
        )
        .reset_index()
    )
    if strata.empty:
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)

    total_institutions = float(strata["cohort_institutions"].sum())
    total_phd = float(strata["cohort_phd_faculty_total"].sum())
    strata["cohort_institution_share"] = (
        strata["cohort_institutions"] / total_institutions if total_institutions else 0.0
    )
    strata["cohort_phd_faculty_share"] = (
        strata["cohort_phd_faculty_total"] / total_phd if total_phd else 0.0
    )

    merged = coverage.merge(
        cohort[["e_mec_code", "inst_type", "region"]],
        on="e_mec_code",
        how="inner",
    )
    sources = sorted(coverage["source"].dropna().astype(str).unique().tolist())
    if not sources:
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)

    per_source = (
        merged.groupby(["source", "inst_type", "region"], dropna=False)["n_records"]
        .sum()
        .reset_index()
    )
    source_totals = (
        merged.groupby("source", dropna=False)["n_records"].sum().rename("source_total").reset_index()
    )

    stratum_pairs = strata[["inst_type", "region"]].drop_duplicates().copy()
    grid = (
        pd.DataFrame({"source": sources})
        .assign(_join_key=1)
        .merge(stratum_pairs.assign(_join_key=1), on="_join_key", how="inner")
        .drop(columns="_join_key")
    )

    result = (
        grid.merge(per_source, on=["source", "inst_type", "region"], how="left")
        .merge(source_totals, on="source", how="left")
        .merge(
            strata[
                [
                    "inst_type",
                    "region",
                    "cohort_institutions",
                    "cohort_institution_share",
                    "cohort_phd_faculty_share",
                ]
            ],
            on=["inst_type", "region"],
            how="left",
        )
    )
    result["n_records"] = result["n_records"].fillna(0.0)
    result["source_total"] = result["source_total"].fillna(0.0)
    result["source_publication_share"] = result.apply(
        lambda r: float(r["n_records"]) / float(r["source_total"]) if float(r["source_total"]) > 0 else 0.0,
        axis=1,
    )
    result["peer_mean_share"] = (
        result.groupby(["inst_type", "region"], dropna=False)["source_publication_share"]
        .transform("mean")
    )
    result["comparative_skew"] = result["source_publication_share"] - result["peer_mean_share"]
    result["delta_vs_cohort_institution_share"] = (
        result["source_publication_share"] - result["cohort_institution_share"]
    )
    result["delta_vs_cohort_phd_faculty_share"] = (
        result["source_publication_share"] - result["cohort_phd_faculty_share"]
    )

    result = result[GEOGRAPHIC_COLUMNS].copy()
    result["n_records"] = result["n_records"].astype(int)
    return result.sort_values(["source", "inst_type", "region"]).reset_index(drop=True)
