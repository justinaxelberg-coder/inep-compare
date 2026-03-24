# dashboard/tabs/registry.py
from __future__ import annotations

import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, dash_table, dcc, html

logger = logging.getLogger(__name__)

_SINAES_LABELS = {
    "federal_university":   "Federal University",
    "state_university":     "State University",
    "private_university":   "Private University",
    "federal_institute":    "Federal Institute",
    "community_university": "Community University",
    "isolated_faculty":     "Isolated Faculty",
}


def layout(registry_df: pd.DataFrame) -> html.Div:
    return html.Div([
        html.H4("Institution Registry", className="mt-3 mb-3"),
        html.Div(id="registry-summary-cards", children=_summary_cards(registry_df),
                 className="mb-4"),
        html.Div([
            html.Div([
                html.Label("Region:"),
                dcc.Dropdown(id="registry-region-filter",
                             options=_options(registry_df, "region"),
                             multi=True, placeholder="All regions",
                             style={"color": "#000"}),
            ], style={"width": "32%", "display": "inline-block", "paddingRight": "1%"}),
            html.Div([
                html.Label("SINAES Type:"),
                dcc.Dropdown(id="registry-type-filter",
                             options=_options(registry_df, "sinaes_type"),
                             multi=True, placeholder="All types",
                             style={"color": "#000"}),
            ], style={"width": "32%", "display": "inline-block", "paddingRight": "1%"}),
            html.Div([
                html.Label("State (UF):"),
                dcc.Dropdown(id="registry-state-filter",
                             options=_options(registry_df, "state"),
                             multi=True, placeholder="All states",
                             style={"color": "#000"}),
            ], style={"width": "32%", "display": "inline-block"}),
        ], className="mb-3"),
        html.H5("Top 20 Institutions by PhD Faculty", className="mt-2"),
        dcc.Graph(id="registry-phd-chart", figure=_phd_bar_figure(registry_df)),
        html.H5("Full Registry", className="mt-4"),
        html.Div(id="registry-table-container", children=_data_table(registry_df)),
        dcc.Store(id="registry-data-store", data=registry_df.to_json(orient="records")),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("registry-table-container", "children"),
        Output("registry-phd-chart", "figure"),
        Output("registry-summary-cards", "children"),
        Input("registry-region-filter", "value"),
        Input("registry-type-filter",   "value"),
        Input("registry-state-filter",  "value"),
        Input("registry-data-store",    "data"),
    )
    def update_registry(regions, types, states, data_json):
        if not data_json:
            empty = pd.DataFrame()
            return _data_table(empty), _phd_bar_figure(empty), _summary_cards(empty)
        df = pd.read_json(data_json, orient="records")
        # Coerce numeric columns after JSON round-trip (dtypes are lost)
        for col in ("faculty_with_phd", "faculty_total", "phd_faculty_share"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = _apply_filters(df, regions, types, states)
        return _data_table(df), _phd_bar_figure(df), _summary_cards(df)


def _options(df: pd.DataFrame, col: str) -> list[dict]:
    if df.empty or col not in df.columns:
        return []
    return [{"label": str(v), "value": str(v)} for v in sorted(df[col].dropna().unique())]


def _apply_filters(df: pd.DataFrame, regions, types, states) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if regions:
        df = df[df["region"].isin(regions)]
    if types:
        df = df[df["sinaes_type"].isin(types)]
    if states:
        df = df[df["state"].isin(states)]
    return df


def _summary_cards(df: pd.DataFrame) -> list:
    if df.empty or "sinaes_type" not in df.columns:
        return [html.P("No registry data.", className="text-muted")]
    counts = df["sinaes_type"].value_counts().to_dict()
    cards = []
    for key, label in _SINAES_LABELS.items():
        n = counts.get(key, 0)
        cards.append(html.Div([
            html.H6(label, style={"margin": "0", "fontSize": "0.75rem", "color": "#aaa"}),
            html.H4(str(n), style={"margin": "0", "fontWeight": "bold"}),
        ], style={"display": "inline-block", "backgroundColor": "#3a3a3a",
                  "borderRadius": "6px", "padding": "10px 16px",
                  "marginRight": "10px", "minWidth": "120px", "textAlign": "center",
                  "verticalAlign": "top"}))
    cards.append(html.Div([
        html.H6("Total", style={"margin": "0", "fontSize": "0.75rem", "color": "#aaa"}),
        html.H4(str(len(df)), style={"margin": "0", "fontWeight": "bold"}),
    ], style={"display": "inline-block", "backgroundColor": "#2a2a5a",
              "borderRadius": "6px", "padding": "10px 16px",
              "marginRight": "10px", "minWidth": "100px", "textAlign": "center"}))
    return cards


def _phd_bar_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty or "faculty_with_phd" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="No registry data", paper_bgcolor="#303030",
                          plot_bgcolor="#303030", font={"color": "#ffffff"})
        return fig
    top = (df[["name", "faculty_with_phd", "sinaes_type"]]
           .dropna(subset=["faculty_with_phd"])
           .sort_values("faculty_with_phd", ascending=False)
           .head(20))
    colours = {
        "federal_university": "#1f77b4", "state_university": "#ff7f0e",
        "private_university": "#2ca02c", "federal_institute": "#d62728",
        "community_university": "#9467bd", "isolated_faculty": "#8c564b",
    }
    bar_colours = [colours.get(t, "#aec7e8") for t in top["sinaes_type"]]
    labels = [n[:30] + "\u2026" if len(str(n)) > 30 else str(n) for n in top["name"]]
    fig = go.Figure(go.Bar(
        x=labels, y=top["faculty_with_phd"].tolist(), marker_color=bar_colours,
        hovertemplate="%{x}<br>PhD Faculty: %{y:,}<extra></extra>",
    ))
    fig.update_layout(
        title="Top 20 Institutions by PhD Faculty Count",
        paper_bgcolor="#303030", plot_bgcolor="#303030",
        font={"color": "#ffffff"},
        margin={"t": 50, "b": 140, "l": 60, "r": 20},
        xaxis={"tickangle": -45},
        yaxis={"title": "Faculty with PhD"},
    )
    return fig


def _data_table(df: pd.DataFrame) -> dash_table.DataTable:
    display_cols = ["e_mec_code", "name", "abbreviation", "city", "state",
                    "region", "sinaes_type", "faculty_with_phd", "phd_faculty_share"]
    if df.empty:
        records, cols = [], display_cols
    else:
        available = [c for c in display_cols if c in df.columns]
        records, cols = df[available].to_dict("records"), available
    return dash_table.DataTable(
        data=records,
        columns=[{"name": c, "id": c} for c in cols],
        filter_action="native", sort_action="native",
        page_action="native", page_size=25,
        style_table={"overflowX": "auto"},
        style_cell={"backgroundColor": "#3a3a3a", "color": "#ffffff",
                    "border": "1px solid #555", "textAlign": "left",
                    "padding": "5px", "fontSize": "0.85rem",
                    "maxWidth": "200px", "overflow": "hidden",
                    "textOverflow": "ellipsis"},
        style_header={"backgroundColor": "#222", "fontWeight": "bold", "color": "#fff"},
        tooltip_data=[{col: {"value": str(row.get(col, "")), "type": "markdown"}
                       for col in cols} for row in records],
        tooltip_duration=None,
    )
