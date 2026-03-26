from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


_SUMMARY_COLUMNS = [
    "source",
    "canonical_works",
    "usable_coverage_share",
    "disputed_coverage_share",
    "verification_risk_share",
    "high_confidence_share",
    "medium_confidence_share",
    "low_confidence_share",
    "external_corroboration_share",
    "major_conflict_share",
    "missing_critical_verifiability_fields_share",
    "doi_expected_missing_share",
]

_DETAIL_COLUMNS = [
    "source",
    "record_type",
    "canonical_works",
    "usable_coverage_share",
    "disputed_coverage_share",
    "verification_risk_share",
    "high_confidence_share",
    "medium_confidence_share",
    "low_confidence_share",
    "external_corroboration_share",
    "major_conflict_share",
    "missing_critical_verifiability_fields_share",
    "doi_expected_missing_share",
]

_REASON_COLUMNS = ["record_type", "flag", "n_works", "denominator", "share"]


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _summary_with_missing_critical(summary_df: pd.DataFrame, flags_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()

    missing = (
        flags_df[flags_df["flag"] == "missing_critical_verifiability_fields"][
            ["source", "record_type", "share"]
        ]
        .rename(columns={"share": "missing_critical_verifiability_fields_share"})
    )
    if missing.empty:
        summary = summary_df.copy()
        summary["missing_critical_verifiability_fields_share"] = 0.0
        return summary

    summary = summary_df.merge(missing, on=["source", "record_type"], how="left")
    summary["missing_critical_verifiability_fields_share"] = (
        summary["missing_critical_verifiability_fields_share"].fillna(0.0)
    )
    return summary


def _overall_summary_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return _empty(_SUMMARY_COLUMNS)

    overall = summary_df[summary_df["record_type"] == "__all__"].copy()
    if overall.empty:
        return _empty(_SUMMARY_COLUMNS)

    overall["usable_coverage_share"] = overall["integration_ready_share"]
    overall["disputed_coverage_share"] = overall["reviewable_disputed_share"]
    overall["verification_risk_share"] = overall["not_integration_ready_share"]
    return overall[_SUMMARY_COLUMNS].sort_values(["source"], kind="stable")


def _record_type_summary_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return _empty(_DETAIL_COLUMNS)

    detail = summary_df[summary_df["record_type"] != "__all__"].copy()
    if detail.empty:
        return _empty(_DETAIL_COLUMNS)

    detail["usable_coverage_share"] = detail["integration_ready_share"]
    detail["disputed_coverage_share"] = detail["reviewable_disputed_share"]
    detail["verification_risk_share"] = detail["not_integration_ready_share"]
    return detail[_DETAIL_COLUMNS].sort_values(["source", "record_type"], kind="stable")


def _top_downgrade_reasons_for_source(flags_df: pd.DataFrame, source: str) -> pd.DataFrame:
    if flags_df.empty:
        return _empty(_REASON_COLUMNS)

    source_flags = flags_df[flags_df["source"] == source].copy()
    if source_flags.empty:
        return _empty(_REASON_COLUMNS)

    rows: list[pd.DataFrame] = []
    for record_type, subset in source_flags.groupby("record_type", sort=True):
        top = subset.sort_values(
            ["share", "n_works", "flag"],
            ascending=[False, False, True],
            kind="stable",
        ).head(3)
        rows.append(top[_REASON_COLUMNS])

    if not rows:
        return _empty(_REASON_COLUMNS)

    return pd.concat(rows, ignore_index=True).sort_values(
        ["record_type", "share", "flag"], kind="stable"
    )


def build_reliability_report(summary_df: pd.DataFrame, flags_df: pd.DataFrame, run_id: str) -> str:
    summary_with_missing = _summary_with_missing_critical(summary_df, flags_df)
    overview = _overall_summary_table(summary_with_missing)
    detail = _record_type_summary_table(summary_with_missing)

    sources = sorted(
        set(summary_with_missing["source"].dropna().astype(str))
        | set(flags_df["source"].dropna().astype(str))
    )

    lines = [
        "# INEP Bibliometric Tool - Record Reliability Report",
        "",
        f"**Run:** `{run_id}`  ",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "> Reliability is reported as usable coverage, disputed coverage, and verification-risk shares. It is descriptive evidence and does not overwrite the composite fitness matrix in V1.",
        "",
        "## Source Summary",
        "",
        overview.to_markdown(index=False) if not overview.empty else "_No reliability summary available._",
        "",
    ]

    if not detail.empty:
        lines += [
            "## Record-Type Breakdown",
            "",
            detail.to_markdown(index=False),
            "",
        ]

    lines += ["## Top Downgrade Reasons", ""]
    if not sources:
        lines += ["_No downgrade reasons available._", ""]
        return "\n".join(lines)

    for source in sources:
        reasons = _top_downgrade_reasons_for_source(flags_df, source)
        lines += [
            f"## Source: {source}",
            "",
            "> Top downgrade reasons are shown overall and by `record_type`.",
            "",
            reasons.to_markdown(index=False) if not reasons.empty else "_No downgrade reasons available._",
            "",
        ]

    return "\n".join(lines)
