# dashboard/tabs/enrichment.py
from __future__ import annotations

import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html

logger = logging.getLogger(__name__)

_REGION_COLOURS = {
    "Norte":       "#d62728",
    "Nordeste":    "#ff7f0e",
    "Centro-Oeste":"#bcbd22",
    "Sul":         "#2ca02c",
    "Sudeste":     "#1f77b4",
}
_SOURCE_COLOURS = {
    "openalex":  "#1f77b4",
    "scopus":    "#ff7f0e",
    "dimensions":"#2ca02c",
}


def layout(geo_df: pd.DataFrame, sdg_df: pd.DataFrame, metadata: dict) -> html.Div:
    sources = sorted(geo_df["source"].unique().tolist()) if not geo_df.empty else []
    default_source = sources[0] if sources else None

    scopus_sdg_warning = []
    if not metadata.get("scopus", {}).get("sdg_available", True):
        scopus_sdg_warning = [html.Div(
            "⚠ Scopus SDG data not available via standard API (requires SciVal). "
            "sdg_coverage scored as 0.0 for Scopus.",
            style={"backgroundColor": "#4a3000", "color": "#ffcc00",
                   "padding": "10px 16px", "borderRadius": "6px",
                   "marginBottom": "16px", "fontSize": "0.9rem"},
        )]

    return html.Div([
        html.H4("Enrichment Indicators", className="mt-3 mb-3"),
        *scopus_sdg_warning,

        # Geographic bias section
        html.H5("Geographic Coverage Bias", className="mt-2 mb-1"),
        html.P(
            "Coverage gap = source's observed regional share minus INEP baseline share. "
            "Negative = under-indexed relative to population.",
            className="text-muted", style={"fontSize": "0.85rem"},
        ),
        html.Div([
            html.Label("Source:"),
            dcc.Dropdown(
                id="enrichment-source-dropdown",
                options=[{"label": s, "value": s} for s in sources],
                value=default_source,
                clearable=False,
                style={"color": "#000", "maxWidth": "300px"},
            ),
        ], className="mb-3"),
        dcc.Graph(id="enrichment-geo-chart",
                  figure=_geo_bar_figure(geo_df, default_source)),

        # Diamond OA section
        html.H5("Diamond OA Breakdown", className="mt-4 mb-1"),
        html.P(
            "Diamond OA: no APC, no subscription (Scielo, Redalyc, DOAJ, OJS). "
            "Run the uncapped phase 2 to populate this chart.",
            className="text-muted", style={"fontSize": "0.85rem"},
        ),
        dcc.Graph(id="enrichment-oa-chart",
                  figure=_oa_type_figure(pd.DataFrame())),

        # SDG section
        html.H5("SDG Coverage by Source", className="mt-4 mb-1"),
        html.Div(id="enrichment-sdg-content",
                 children=_sdg_content(sdg_df)),

        # Stores
        dcc.Store(id="enrichment-geo-store",
                  data=geo_df.to_json(orient="records") if not geo_df.empty else ""),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("enrichment-geo-chart", "figure"),
        Input("enrichment-source-dropdown", "value"),
        State("enrichment-geo-store", "data"),
    )
    def update_geo(source, data_json):
        if not source or not data_json:
            return _empty_figure("Select a source")
        df = pd.read_json(data_json, orient="records")
        return _geo_bar_figure(df, source)


def _geo_bar_figure(df: pd.DataFrame, source: str | None) -> go.Figure:
    if df.empty or not source or "source" not in df.columns:
        return _empty_figure("No geographic data — run run_enrichment.py first")
    sub = df[df["source"] == source].copy()
    if sub.empty:
        return _empty_figure(f"No data for {source}")
    colours = [_REGION_COLOURS.get(r, "#aaa") for r in sub["region"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=sub["region"].tolist(),
        y=sub["coverage_gap"].tolist(),
        marker_color=colours,
        name="Coverage gap",
        hovertemplate="%{x}<br>Gap: %{y:+.3f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="#ffffff", opacity=0.4)
    fig.update_layout(
        title=f"Regional coverage gap — {source}",
        yaxis_title="Observed share − Expected share",
        paper_bgcolor="#303030", plot_bgcolor="#303030",
        font={"color": "#ffffff"},
        margin={"t": 50, "b": 60, "l": 80, "r": 20},
    )
    return fig


def _oa_type_figure(oa_df: pd.DataFrame) -> go.Figure:
    """Bar chart of oa_type distribution across sources. Empty state if no data."""
    _OA_COLOURS = {
        "diamond": "#9467bd", "gold": "#ffbf00",
        "green": "#2ca02c",   "hybrid": "#17becf",
        "closed": "#7f7f7f",  "unknown": "#aaa",
    }
    if oa_df.empty or "oa_type" not in oa_df.columns:
        return _empty_figure("Diamond OA data not yet available — run uncapped phase 2")
    # Count oa_type per source
    counts = oa_df.groupby(["source","oa_type"]).size().reset_index(name="n")
    fig = go.Figure()
    for oa_type, grp in counts.groupby("oa_type"):
        fig.add_trace(go.Bar(
            name=oa_type,
            x=grp["source"].tolist(),
            y=grp["n"].tolist(),
            marker_color=_OA_COLOURS.get(str(oa_type), "#aaa"),
            hovertemplate="%{x} — " + str(oa_type) + ": %{y}<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack", title="OA type distribution by source",
        paper_bgcolor="#303030", plot_bgcolor="#303030",
        font={"color": "#ffffff"},
        margin={"t": 50, "b": 60, "l": 60, "r": 20},
    )
    return fig


def _sdg_content(sdg_df: pd.DataFrame) -> list:
    if sdg_df.empty:
        return [html.P(
            "SDG data not yet available. Run: python run_enrichment.py (requires OpenAlex + Dimensions API access).",
            className="text-muted",
        )]
    sources = sdg_df["source"].unique().tolist()
    rows = []
    for source in sources:
        sub = sdg_df[sdg_df["source"] == source].sort_values("sdg_goal")
        fig = go.Figure(go.Bar(
            x=sub["sdg_label"].tolist(),
            y=sub["rate"].tolist(),
            marker_color=_SOURCE_COLOURS.get(source, "#aaa"),
            hovertemplate="%{x}<br>Rate: %{y:.1%}<extra></extra>",
        ))
        fig.update_layout(
            title=f"SDG coverage — {source}",
            yaxis={"tickformat": ".0%"},
            paper_bgcolor="#303030", plot_bgcolor="#303030",
            font={"color": "#ffffff"},
            margin={"t": 50, "b": 100, "l": 60, "r": 20},
            xaxis={"tickangle": -45},
        )
        rows.append(dcc.Graph(figure=fig))
    return rows


def _empty_figure(msg: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=msg, paper_bgcolor="#303030",
        plot_bgcolor="#303030", font={"color": "#ffffff"},
    )
    return fig
