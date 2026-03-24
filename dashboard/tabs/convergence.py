# dashboard/tabs/convergence.py
from __future__ import annotations

import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, dcc, html

logger = logging.getLogger(__name__)

_BADGE_DIVERGENT = {"backgroundColor": "#d62728", "color": "#fff",
                    "borderRadius": "4px", "padding": "2px 8px",
                    "marginRight": "6px", "display": "inline-block"}
_BADGE_CEILING   = {"backgroundColor": "#ff7f0e", "color": "#fff",
                    "borderRadius": "4px", "padding": "2px 8px",
                    "marginRight": "6px", "display": "inline-block"}
_BADGE_OK        = {"backgroundColor": "#2ca02c", "color": "#fff",
                    "borderRadius": "4px", "padding": "2px 8px",
                    "marginRight": "6px", "display": "inline-block"}
_CEILING_THRESHOLD = 0.80


def layout(overlap_df: pd.DataFrame, divs_df: pd.DataFrame) -> html.Div:
    institutions = _institution_options(overlap_df)
    default_inst = institutions[0]["value"] if institutions else None

    return html.Div([
        html.H4("Convergence Explorer", className="mt-3 mb-3"),
        html.Div([
            html.Label("Select institution (e-MEC code):"),
            dcc.Dropdown(
                id="convergence-institution-dropdown",
                options=institutions,
                value=default_inst,
                clearable=False,
                style={"color": "#000", "maxWidth": "500px"},
            ),
        ], className="mb-3"),
        html.Div(id="convergence-flag-badges", className="mb-3"),
        html.Div(id="convergence-summary-stats", className="mb-3"),
        dcc.Graph(id="convergence-bar-chart"),
        dcc.Store(id="convergence-overlap-store",
                  data=overlap_df.to_json(orient="records")),
        dcc.Store(id="convergence-divs-store",
                  data=divs_df.to_json(orient="records")),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("convergence-bar-chart", "figure"),
        Output("convergence-flag-badges", "children"),
        Output("convergence-summary-stats", "children"),
        Input("convergence-institution-dropdown", "value"),
        Input("convergence-overlap-store", "data"),
        Input("convergence-divs-store", "data"),
    )
    def update_convergence(inst_code, overlap_json, divs_json):
        empty_fig = _empty_bar()
        if not inst_code or not overlap_json:
            return empty_fig, [], _summary_stats(pd.DataFrame(), pd.DataFrame())

        overlap_df = pd.read_json(overlap_json, orient="records")
        divs_df = pd.read_json(divs_json, orient="records") if divs_json else pd.DataFrame()

        overlap_df["e_mec_code"] = overlap_df["e_mec_code"].astype(str)
        inst_overlap = overlap_df[overlap_df["e_mec_code"] == str(inst_code)]

        if inst_overlap.empty:
            return empty_fig, _badges(pd.DataFrame(), pd.DataFrame()), \
                   _summary_stats(pd.DataFrame(), pd.DataFrame())

        inst_divs = pd.DataFrame()
        if not divs_df.empty and "e_mec_code" in divs_df.columns:
            divs_df["e_mec_code"] = divs_df["e_mec_code"].astype(str)
            inst_divs = divs_df[divs_df["e_mec_code"] == str(inst_code)]

        return _bar_figure(inst_overlap), _badges(inst_overlap, inst_divs), \
               _summary_stats(inst_overlap, inst_divs)


def _institution_options(df: pd.DataFrame) -> list[dict]:
    if df.empty or "e_mec_code" not in df.columns:
        return []
    codes = sorted(df["e_mec_code"].astype(str).unique())
    return [{"label": c, "value": c} for c in codes]


def _bar_figure(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    df["pair"] = df["source_a"] + " / " + df["source_b"]
    pct = df["overlap_pct_min"].fillna(0).tolist()
    colours = ["#d62728" if v < 0.20 else ("#ffff00" if v < 0.50 else "#2ca02c") for v in pct]
    fig = go.Figure(go.Bar(
        x=df["pair"].tolist(), y=pct, marker_color=colours,
        hovertemplate="%{x}<br>Overlap: %{y:.1%}<extra></extra>",
    ))
    fig.update_layout(
        title="Source-pair overlap % (min)",
        yaxis={"range": [0, 1], "tickformat": ".0%"},
        paper_bgcolor="#303030", plot_bgcolor="#303030",
        font={"color": "#ffffff"},
        margin={"t": 50, "b": 80, "l": 60, "r": 20},
        xaxis={"tickangle": -30},
    )
    return fig


def _empty_bar() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(title="Select an institution", paper_bgcolor="#303030",
                      plot_bgcolor="#303030", font={"color": "#ffffff"})
    return fig


def _badges(overlap_df: pd.DataFrame, divs_df: pd.DataFrame) -> list:
    badges = []
    if not divs_df.empty:
        for _, row in divs_df.iterrows():
            label = (f"Divergent: {row.get('source_a','')} vs {row.get('source_b','')} "
                     f"({float(row.get('discrepancy_pct', 0)):.0%})")
            badges.append(html.Span(f"🔴 {label}", style=_BADGE_DIVERGENT))
    if not overlap_df.empty and "overlap_pct_min" in overlap_df.columns:
        for _, row in overlap_df.iterrows():
            if float(row.get("overlap_pct_min", 0) or 0) >= _CEILING_THRESHOLD:
                label = (f"Ceiling: {row.get('source_a','')} vs {row.get('source_b','')} "
                         f"({float(row['overlap_pct_min']):.0%})")
                badges.append(html.Span(f"🟡 {label}", style=_BADGE_CEILING))
    if not badges:
        badges = [html.Span("✅ No warnings for this institution", style=_BADGE_OK)]
    return badges


def _summary_stats(overlap_df: pd.DataFrame, divs_df: pd.DataFrame) -> html.Div:
    if overlap_df.empty or "overlap_pct_min" not in overlap_df.columns:
        return html.P("No overlap data for this institution.", className="text-muted")
    mean_overlap = overlap_df["overlap_pct_min"].mean()
    n_divergent  = len(divs_df) if not divs_df.empty else 0
    return html.Div([
        html.Span(f"Mean overlap: {mean_overlap:.1%}",
                  style={"marginRight": "20px", "fontWeight": "bold"}),
        html.Span(f"Divergent pairs: {n_divergent}",
                  style={"color": "#d62728" if n_divergent > 0 else "#2ca02c",
                         "fontWeight": "bold"}),
    ])
