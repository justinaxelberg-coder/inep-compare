# enrichment/patents.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

# NOTE: Patent link rate derived from `patent_citations` field populated by
# The Lens connector (requires LENS_API_KEY). Crossref reference deposit
# provides partial signal when Lens is absent. Derwent Innovation
# cross-reference pending when access is available.


def has_patent_link(patent_citations) -> bool:
    if not patent_citations:
        return False
    return len(patent_citations) > 0


def compute_patent_link_rate(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, patent_citations (list or None)
    Returns: source, inst_type, region, patent_rate, n_papers
    """
    if papers_df.empty:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["has_patent"] = df["patent_citations"].apply(has_patent_link)

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (
        merged
        .groupby(["source", "inst_type", "region"])
        .agg(patent_rate=("has_patent", "mean"),
             n_papers=("has_patent", "count"))
        .reset_index()
    )


def build_patent_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="patent_link_rate",
            value=float(r["patent_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
