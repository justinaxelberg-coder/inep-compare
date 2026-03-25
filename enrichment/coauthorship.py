# enrichment/coauthorship.py
from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

NON_ACADEMIC_TYPES = {"company", "government", "nonprofit", "facility", "healthcare", "other"}


def is_nonacademic(affil_types: list[str]) -> bool:
    return bool(set(affil_types) & NON_ACADEMIC_TYPES)


def compute_coauth_metrics(papers: list[dict]) -> dict[str, float]:
    """
    Each paper dict:
      affiliation_types: list[list[str]] | None  (one list of types per author)
      ror_resolved: bool
    Returns: detectability, volume_rate, quality_score, nonacademic_coauth_score
    """
    if not papers:
        return {"detectability": 0.0, "volume_rate": 0.0,
                "quality_score": 0.0, "nonacademic_coauth_score": 0.0}
    n = len(papers)
    n_with_types = sum(1 for p in papers if p.get("affiliation_types"))
    n_nonacademic = sum(
        1 for p in papers
        if p.get("affiliation_types") and
        any(is_nonacademic(a) for a in p["affiliation_types"])
    )
    n_ror_resolved_nonacademic = sum(
        1 for p in papers
        if p.get("affiliation_types") and
        any(is_nonacademic(a) for a in p["affiliation_types"]) and
        p.get("ror_resolved", False)
    )
    detectability = n_with_types / n if n else 0.0
    volume_rate = n_nonacademic / n if n else 0.0
    quality_score = n_ror_resolved_nonacademic / n_nonacademic if n_nonacademic else 0.0
    composite = 0.4 * detectability + 0.3 * volume_rate + 0.3 * quality_score
    return {
        "detectability": detectability,
        "volume_rate": volume_rate,
        "quality_score": quality_score,
        "nonacademic_coauth_score": composite,
    }


def compute_coauth_stratified(
    papers_df,
    crosswalk_df,
) -> list[dict]:
    """Stratified non-academic coauthorship rows (source × inst_type × region)."""
    import pandas as pd
    from enrichment.stratified import make_stratum_row
    if papers_df.empty:
        return []
    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    rows = []
    for (source, inst_type, region), grp in merged.groupby(["source", "inst_type", "region"]):
        papers_list = grp.to_dict("records")
        metrics = compute_coauth_metrics(papers_list)
        rows.append(make_stratum_row(
            source=str(source), inst_type=str(inst_type), region=str(region),
            sub_dimension="nonacademic_coauth",
            value=metrics["nonacademic_coauth_score"],
            n_papers=len(papers_list),
        ))
    return rows
