# enrichment/policy_docs.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

# Dimensions document types classified as policy-relevant.
# Overton cross-reference pending when access is available.
POLICY_DOC_TYPES = {
    "policy_report", "policy_brief", "working_paper",
    "government_document", "report", "legislation",
    "clinical_guideline", "standard",
}


def is_policy_document(doc_type: str | None) -> bool:
    if not doc_type:
        return False
    return doc_type.lower().replace(" ", "_") in POLICY_DOC_TYPES


def compute_policy_rates(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, document_type
    Returns: source, inst_type, region, policy_rate, n_papers
    """
    if papers_df.empty:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["is_policy"] = df["document_type"].apply(is_policy_document)

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (
        merged
        .groupby(["source", "inst_type", "region"])
        .agg(policy_rate=("is_policy", "mean"),
             n_papers=("is_policy", "count"))
        .reset_index()
    )


def build_policy_rows(df: pd.DataFrame) -> list[dict]:
    # NOTE: Overton cross-reference pending — policy_document_rate uses
    # source-declared document_type only. Validate against Overton when available.
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="policy_document_rate",
            value=float(r["policy_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
