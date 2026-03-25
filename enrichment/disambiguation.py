# enrichment/disambiguation.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)


def compute_disambiguation_rate(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, ror_resolved (bool)
    crosswalk_df: e_mec_code, inst_type, region
    Returns: source, inst_type, region, ror_rate, n_papers
    """
    if papers_df.empty or "source" not in papers_df.columns:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["ror_resolved"] = (
        df["ror_resolved"].fillna(False).astype(bool)
        if "ror_resolved" in df.columns
        else pd.Series(False, index=df.index)
    )

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)

    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (
        merged
        .groupby(["source", "inst_type", "region"])
        .agg(ror_rate=("ror_resolved", "mean"),
             n_papers=("ror_resolved", "count"))
        .reset_index()
    )


def build_disambiguation_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="disambiguation_quality",
            value=float(r["ror_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
