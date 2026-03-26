from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def _overall_summary_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(
            columns=[
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
                "doi_expected_missing_share",
            ]
        )

    overall = summary_df[summary_df["record_type"] == "__all__"].copy()
    if overall.empty:
        return pd.DataFrame(
            columns=[
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
                "doi_expected_missing_share",
            ]
        )

    overall["usable_coverage_share"] = overall["integration_ready_share"]
    overall["disputed_coverage_share"] = overall["reviewable_disputed_share"]
    overall["verification_risk_share"] = overall["not_integration_ready_share"]
    return overall[
        [
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
            "doi_expected_missing_share",
        ]
    ].sort_values(["source"], kind="stable")


def _record_type_summary_table(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()
    detail = summary_df[summary_df["record_type"] != "__all__"].copy()
    if detail.empty:
        return pd.DataFrame()
    detail["usable_coverage_share"] = detail["integration_ready_share"]
    detail["disputed_coverage_share"] = detail["reviewable_disputed_share"]
    detail["verification_risk_share"] = detail["not_integration_ready_share"]
    return detail[
        [
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
            "doi_expected_missing_share",
        ]
    ].sort_values(["source", "record_type"], kind="stable")


def _top_downgrade_reasons(flags_df: pd.DataFrame) -> pd.DataFrame:
    if flags_df.empty:
        return pd.DataFrame(columns=["source", "record_type", "flag", "n_works", "denominator", "share"])

    overall = flags_df[flags_df["record_type"] == "__all__"].copy()
    if overall.empty:
        overall = flags_df.copy()

    return overall.sort_values(
        ["share", "n_works", "source", "record_type", "flag"],
        ascending=[False, False, True, True, True],
        kind="stable",
    ).head(10)


def build_reliability_report(summary_df: pd.DataFrame, flags_df: pd.DataFrame, run_id: str) -> str:
    overview = _overall_summary_table(summary_df)
    detail = _record_type_summary_table(summary_df)
    reasons = _top_downgrade_reasons(flags_df)

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

    lines += [
        "## Top Downgrade Reasons",
        "",
        reasons.to_markdown(index=False) if not reasons.empty else "_No downgrade reasons available._",
        "",
    ]

    return "\n".join(lines)
