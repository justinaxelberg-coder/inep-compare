# enrichment/funder.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

BR_FUNDER_KEYWORDS = {
    # Federal agencies
    "cnpq", "capes", "finep", "bndes", "embrapii", "mec",
    # State FAP agencies
    "fapesp", "faperj", "fapemig", "fapesc", "fapesb", "fapespa",
    "fapergs", "fapeal", "fapern", "fapero", "fapdf",
    # Substring matches for full names
    "fundação de amparo", "conselho nacional de desenvolvimento",
    "coordenação de aperfeiçoamento",
}


def is_brazilian_funder(name: str) -> bool:
    lower = name.lower()
    return any(kw in lower for kw in BR_FUNDER_KEYWORDS)


def compute_funder_rates(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, funding (list of dicts with 'funder' key)
    Returns: source, inst_type, region, funder_rate, br_funder_rate, n_papers
    """
    if papers_df.empty:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)

    def _has_funder(funding) -> bool:
        return isinstance(funding, list) and len(funding) > 0

    def _has_br_funder(funding) -> bool:
        if not isinstance(funding, list):
            return False
        return any(is_brazilian_funder(f.get("funder", "")) for f in funding)

    df["has_funder"] = df["funding"].apply(_has_funder)
    df["has_br_funder"] = df["funding"].apply(_has_br_funder)

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (
        merged
        .groupby(["source", "inst_type", "region"])
        .agg(
            funder_rate=("has_funder", "mean"),
            br_funder_rate=("has_br_funder", "mean"),
            n_papers=("has_funder", "count"),
        )
        .reset_index()
    )


def build_funder_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        for sub_dim, col in [("funder_metadata_rate", "funder_rate"),
                              ("br_funder_rate", "br_funder_rate")]:
            rows.append(make_stratum_row(
                source=r["source"],
                inst_type=r["inst_type"],
                region=r["region"],
                sub_dimension=sub_dim,
                value=float(r[col]),
                n_papers=int(r["n_papers"]),
            ))
    return rows
