from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from dash import dash_table, dcc, html


_DARK_LAYOUT = {
    "paper_bgcolor": "#303030",
    "plot_bgcolor": "#303030",
    "font": {"color": "#ffffff"},
}


def layout(summary_df: pd.DataFrame, flags_df: pd.DataFrame) -> html.Div:
    if summary_df.empty:
        return html.Div([
            html.H4("Reliability", className="mt-3 mb-3"),
            html.P(
                "No reliability outputs found. Run `python run_reliability.py` first.",
                className="text-muted",
            ),
        ])

    return html.Div([
        html.H4("Reliability — Usable Coverage and Verification Risk", className="mt-3 mb-2"),
        html.P(
            "Overall shares use all canonical works surfaced by the source. "
            "Record-type charts use within-type denominators.",
            className="text-muted",
        ),
        dcc.Graph(
            id="reliability-overall-chart",
            figure=_outcome_share_figure(summary_df),
        ),
        dcc.Graph(
            id="reliability-record-type-chart",
            figure=_record_type_figure(summary_df, metric="integration_ready_share"),
        ),
        dcc.Graph(
            id="reliability-doi-chart",
            figure=_record_type_figure(summary_df, metric="doi_expected_missing_share"),
        ),
        html.H5("Flag / Reason View", className="mt-4 mb-2"),
        _flag_table(flags_df),
    ])


def register_callbacks(app) -> None:
    return None


def _outcome_share_figure(summary_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    required = {
        "source",
        "record_type",
        "integration_ready_share",
        "reviewable_disputed_share",
        "not_integration_ready_share",
    }
    if summary_df.empty or not required.issubset(summary_df.columns):
        fig.update_layout(title="No reliability summary data", **_DARK_LAYOUT)
        return fig

    overall = summary_df[summary_df["record_type"] == "__all__"].copy()
    if overall.empty:
        overall = summary_df.copy()

    overall = _coerce_percent_columns(
        overall,
        [
            "integration_ready_share",
            "reviewable_disputed_share",
            "not_integration_ready_share",
        ],
    )
    overall = overall.sort_values("source", kind="stable")

    fig.add_bar(
        name="integration_ready",
        x=overall["source"].tolist(),
        y=overall["integration_ready_share"].tolist(),
    )
    fig.add_bar(
        name="reviewable_disputed",
        x=overall["source"].tolist(),
        y=overall["reviewable_disputed_share"].tolist(),
    )
    fig.add_bar(
        name="not_integration_ready",
        x=overall["source"].tolist(),
        y=overall["not_integration_ready_share"].tolist(),
    )
    fig.update_layout(
        barmode="stack",
        title="Outcome state share by source",
        yaxis={"tickformat": ".0%"},
        margin={"t": 50, "b": 70, "l": 60, "r": 20},
        **_DARK_LAYOUT,
    )
    return fig


def _record_type_figure(summary_df: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    required = {"source", "record_type", metric}
    if summary_df.empty or not required.issubset(summary_df.columns):
        fig.update_layout(title="No record-type reliability data", **_DARK_LAYOUT)
        return fig

    detail = summary_df[summary_df["record_type"] != "__all__"].copy()
    if detail.empty:
        fig.update_layout(title="No record-type reliability data", **_DARK_LAYOUT)
        return fig

    detail = _coerce_percent_columns(detail, [metric])
    for source, group in detail.groupby("source", sort=True):
        group = group.sort_values("record_type", kind="stable")
        fig.add_bar(name=str(source), x=group["record_type"].tolist(), y=group[metric].tolist())

    title = metric.replace("_", " ").capitalize()
    fig.update_layout(
        barmode="group",
        title=f"{title} by source and record type",
        yaxis={"tickformat": ".0%"},
        xaxis={"tickangle": -30},
        margin={"t": 50, "b": 100, "l": 60, "r": 20},
        **_DARK_LAYOUT,
    )
    return fig


def _flag_table(flags_df: pd.DataFrame):
    if flags_df.empty:
        return html.P("No reliability flags available.", className="text-muted")

    display = flags_df.copy()
    for col in ("n_works", "denominator", "share"):
        if col in display.columns:
            display[col] = pd.to_numeric(display[col], errors="coerce")
    display = display.sort_values(["source", "record_type", "share"], ascending=[True, True, False], kind="stable")
    rows = display[["source", "record_type", "flag", "n_works", "denominator", "share"]].copy()
    rows["share"] = rows["share"].map(lambda value: f"{float(value):.1%}" if pd.notna(value) else "")

    return dash_table.DataTable(
        id="reliability-flag-table",
        data=rows.to_dict("records"),
        columns=[
            {"name": "Source", "id": "source"},
            {"name": "Record Type", "id": "record_type"},
            {"name": "Flag", "id": "flag"},
            {"name": "Works", "id": "n_works"},
            {"name": "Denominator", "id": "denominator"},
            {"name": "Share", "id": "share"},
        ],
        page_size=12,
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={
            "backgroundColor": "#3a3a3a",
            "color": "#ffffff",
            "border": "1px solid #555",
            "textAlign": "left",
            "padding": "6px",
            "fontSize": "0.85rem",
        },
        style_header={"backgroundColor": "#222", "fontWeight": "bold", "color": "#fff"},
    )


def _coerce_percent_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    frame = df.copy()
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    return frame
