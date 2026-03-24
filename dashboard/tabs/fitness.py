# dashboard/tabs/fitness.py
from __future__ import annotations

import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, dash_table, dcc, html

logger = logging.getLogger(__name__)

DIMENSIONS = [
    "coverage", "data_quality", "reliability", "accessibility",
    "social_impact", "governance", "innovation_link",
]

_COLORSCALE = [
    [0.0, "#d62728"],
    [0.5, "#ffff00"],
    [1.0, "#2ca02c"],
]


def layout(fitness_df: pd.DataFrame) -> html.Div:
    sources = sorted(fitness_df["source"].unique()) if not fitness_df.empty else []
    source_options = [{"label": s, "value": s} for s in sources]
    default_source = sources[0] if sources else None

    return html.Div([
        html.H4("Source Fitness Matrix", className="mt-3 mb-3"),
        html.Div([
            html.Div([
                dcc.Graph(id="fitness-heatmap", figure=_heatmap_figure(fitness_df)),
            ], style={"width": "65%", "display": "inline-block", "verticalAlign": "top"}),
            html.Div([
                html.Label("Select source for detail:"),
                dcc.Dropdown(
                    id="fitness-source-dropdown",
                    options=source_options,
                    value=default_source,
                    clearable=False,
                    style={"color": "#000"},
                ),
                dcc.Graph(id="fitness-radar"),
            ], style={"width": "33%", "display": "inline-block",
                      "verticalAlign": "top", "paddingLeft": "1%"}),
        ]),
        html.H5("Top 3 Recommended Sources per Institution Type", className="mt-4"),
        html.Div(id="fitness-rankings-table"),
        dcc.Store(id="fitness-data-store", data=fitness_df.to_json(orient="records")),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("fitness-radar", "figure"),
        Input("fitness-source-dropdown", "value"),
        Input("fitness-data-store", "data"),
    )
    def update_radar(source: str, data_json: str) -> go.Figure:
        if not source or not data_json:
            return _empty_radar()
        df = pd.read_json(data_json, orient="records")
        rows = df[df["source"] == source]
        if rows.empty:
            return _empty_radar()
        means = rows[DIMENSIONS].mean()
        return _radar_figure(source, means)

    @app.callback(
        Output("fitness-rankings-table", "children"),
        Input("fitness-data-store", "data"),
    )
    def update_rankings(data_json: str) -> html.Div:
        if not data_json:
            return html.P("No fitness data available.", className="text-muted")
        df = pd.read_json(data_json, orient="records")
        if df.empty:
            return html.P("No fitness data available.", className="text-muted")
        return _rankings_table(df)


def _heatmap_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No fitness data", paper_bgcolor="#303030",
                          plot_bgcolor="#303030", font={"color": "#ffffff"})
        return fig
    pivot = df.pivot_table(index="source", columns="inst_type",
                           values="composite", aggfunc="mean")
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=_COLORSCALE,
        zmin=0, zmax=1,
        text=[[f"{v:.2f}" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        hovertemplate="Source: %{y}<br>Type: %{x}<br>Composite: %{z:.3f}<extra></extra>",
    ))
    fig.update_layout(
        title="Composite Fitness Score (source × institution type)",
        paper_bgcolor="#303030", plot_bgcolor="#303030",
        font={"color": "#ffffff"},
        margin={"t": 50, "b": 60, "l": 120, "r": 20},
        xaxis={"tickangle": -30},
    )
    return fig


def _radar_figure(source: str, means: pd.Series) -> go.Figure:
    labels = DIMENSIONS + [DIMENSIONS[0]]
    values = [float(means.get(d, 0)) for d in DIMENSIONS] + [float(means.get(DIMENSIONS[0], 0))]
    fig = go.Figure(go.Scatterpolar(
        r=values, theta=labels, fill="toself", name=source, line_color="#1f77b4",
    ))
    fig.update_layout(
        polar={"radialaxis": {"visible": True, "range": [0, 1]}, "bgcolor": "#303030"},
        paper_bgcolor="#303030", font={"color": "#ffffff"},
        title=f"Dimension breakdown: {source}",
        margin={"t": 50, "b": 20, "l": 20, "r": 20},
    )
    return fig


def _empty_radar() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(paper_bgcolor="#303030", plot_bgcolor="#303030",
                      font={"color": "#ffffff"}, title="Select a source")
    return fig


def _rankings_table(df: pd.DataFrame) -> html.Div:
    rows = []
    for it in sorted(df["inst_type"].unique()):
        sub = df[df["inst_type"] == it].sort_values("composite", ascending=False).head(3)
        for rank, (_, row) in enumerate(sub.iterrows(), 1):
            rows.append({
                "Rank": rank, "Institution Type": it, "Source": row["source"],
                "Composite": f"{row['composite']:.3f}",
                "Coverage": f"{row['coverage']:.3f}",
                "Accessibility": f"{row['accessibility']:.3f}",
            })
    return dash_table.DataTable(
        data=rows,
        columns=[{"name": c, "id": c} for c in
                 ["Rank", "Institution Type", "Source", "Composite", "Coverage", "Accessibility"]],
        style_table={"overflowX": "auto"},
        style_cell={"backgroundColor": "#3a3a3a", "color": "#ffffff",
                    "border": "1px solid #555", "textAlign": "left", "padding": "6px"},
        style_header={"backgroundColor": "#222", "fontWeight": "bold", "color": "#fff"},
        style_data_conditional=[
            {"if": {"filter_query": "{Rank} = 1"},
             "backgroundColor": "#1a4a1a", "color": "#90ee90"},
        ],
        page_size=20,
    )
