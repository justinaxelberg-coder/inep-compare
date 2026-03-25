# enrichment/sensitivity.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)


def compute_sensitivity(
    coverage_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute per-(source × e_mec_code) sensitivity using OpenAlex as denominator.

    coverage_df columns: source, e_mec_code, n_records
    crosswalk_df columns: e_mec_code, inst_type, region

    Returns DataFrame with columns:
        source, e_mec_code, inst_type, region, n_source, n_openalex, sensitivity
    """
    if "openalex" not in coverage_df["source"].values:
        logger.warning("OpenAlex not in coverage data — sensitivity cannot be computed")
        return pd.DataFrame()

    oa = (
        coverage_df[coverage_df["source"] == "openalex"][["e_mec_code", "n_records"]]
        .rename(columns={"n_records": "n_openalex"})
        .copy()
    )

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    oa["e_mec_code"] = oa["e_mec_code"].astype(str)

    base = oa.merge(xw, on="e_mec_code", how="inner")

    chunks = []
    for source in coverage_df["source"].unique():
        src = (
            coverage_df[coverage_df["source"] == source][["e_mec_code", "n_records"]]
            .copy()
        )
        src["e_mec_code"] = src["e_mec_code"].astype(str)
        merged = base.merge(src, on="e_mec_code", how="left")
        merged["n_source"] = merged["n_records"].fillna(0).astype(int)
        merged["sensitivity"] = merged.apply(
            lambda r: 1.0 if source == "openalex" else (
                min(1.0, r["n_source"] / r["n_openalex"]) if r["n_openalex"] > 0 else 0.0
            ),
            axis=1,
        )
        merged["source"] = source
        chunks.append(
            merged[["source", "e_mec_code", "inst_type", "region",
                     "n_source", "n_openalex", "sensitivity"]]
        )

    return pd.concat(chunks, ignore_index=True)


def aggregate_by_stratum(sensitivity_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-institution sensitivity into (source × inst_type × region) means."""
    if sensitivity_df.empty:
        return pd.DataFrame()
    return (
        sensitivity_df
        .groupby(["source", "inst_type", "region"])
        .agg(
            sensitivity=("sensitivity", "mean"),
            n_papers=("n_openalex", "sum"),
        )
        .reset_index()
    )


def build_sensitivity_rows(agg_df: pd.DataFrame) -> list[dict]:
    """Convert aggregated sensitivity DataFrame to stratified schema rows."""
    rows = []
    for _, r in agg_df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="sensitivity",
            value=float(r["sensitivity"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
