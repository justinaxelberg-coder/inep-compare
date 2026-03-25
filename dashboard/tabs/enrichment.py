# dashboard/tabs/enrichment.py
from __future__ import annotations

import logging

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html

logger = logging.getLogger(__name__)

_DARK = {"paper_bgcolor": "#303030", "plot_bgcolor": "#303030", "font": {"color": "#ffffff"}}
_REGION_COLOURS = {
    "Norte": "#d62728", "Nordeste": "#ff7f0e", "Centro-Oeste": "#bcbd22",
    "Sul": "#2ca02c",  "Sudeste": "#1f77b4",
}


def layout(combined_df: pd.DataFrame, metadata: dict) -> html.Div:
    sub_dims = (
        sorted(combined_df["sub_dimension"].dropna().unique().tolist())
        if not combined_df.empty and "sub_dimension" in combined_df.columns
        else []
    )
    sources = (
        sorted(combined_df["source"].dropna().unique().tolist())
        if not combined_df.empty and "source" in combined_df.columns
        else []
    )
    default_dim = sub_dims[0] if sub_dims else None

    scopus_banner = []
    if metadata.get("scopus", {}).get("sdg_available") is False:
        scopus_banner = [html.Div(
            "Scopus SDG data not available via standard API (requires SciVal). "
            "sdg_coverage scored as 0.0 for Scopus.",
            style={"backgroundColor": "#4a3000", "color": "#ffcc00",
                   "padding": "10px 16px", "borderRadius": "6px",
                   "marginBottom": "12px", "fontSize": "0.9rem"},
        )]

    pending_notes = html.Div(
        [
            html.Span("Pending cross-references: ", style={"fontWeight": "bold"}),
            "policy_document_rate (Overton), patent_link_rate (Derwent / The Lens)",
        ],
        style={"color": "#aaa", "fontSize": "0.82rem", "marginBottom": "16px"},
        id="enrichment-caveat-notes",
    )

    store_data = combined_df.to_json(orient="records") if not combined_df.empty else "[]"

    return html.Div([
        html.H4("Enrichment — Source Quality by Institution Type & Region",
                className="mt-3 mb-2"),
        *scopus_banner,
        pending_notes,

        # Filters row
        html.Div([
            html.Div([
                html.Label("Sub-dimension:", style={"fontSize": "0.85rem"}),
                dcc.Dropdown(
                    id="enrichment-sub-dim-filter",
                    options=[{"label": d, "value": d} for d in sub_dims],
                    value=default_dim,
                    clearable=False,
                    style={"color": "#000", "minWidth": "280px"},
                ),
            ], style={"marginRight": "24px"}),
            html.Div([
                html.Label("Source(s):", style={"fontSize": "0.85rem"}),
                dcc.Dropdown(
                    id="enrichment-source-filter",
                    options=[{"label": s, "value": s} for s in sources],
                    value=sources,
                    multi=True,
                    style={"color": "#000", "minWidth": "280px"},
                ),
            ]),
        ], style={"display": "flex", "flexWrap": "wrap", "gap": "8px",
                  "marginBottom": "20px", "alignItems": "flex-end"}),

        dcc.Graph(id="enrichment-heatmap"),
        dcc.Graph(id="enrichment-bar-chart"),

        dcc.Store(id="enrichment-data-store", data=store_data),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("enrichment-heatmap", "figure"),
        Output("enrichment-bar-chart", "figure"),
        Input("enrichment-sub-dim-filter", "value"),
        Input("enrichment-source-filter", "value"),
        State("enrichment-data-store", "data"),
    )
    def update_enrichment_charts(sub_dim, sources, data_json):
        if not data_json or data_json == "[]":
            empty = _empty_figure("No enrichment data — run run_enrichment.py first")
            return empty, empty

        df = pd.read_json(data_json, orient="records")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["n_papers"] = pd.to_numeric(df["n_papers"], errors="coerce").fillna(0).astype(int)

        if not sub_dim or "sub_dimension" not in df.columns:
            empty = _empty_figure("Select a sub-dimension")
            return empty, empty

        sub = df[df["sub_dimension"] == sub_dim].copy()
        if sources:
            sub = sub[sub["source"].isin(sources)]

        if sub.empty:
            empty = _empty_figure(f"No data for {sub_dim}")
            return empty, empty

        return _heatmap_figure(sub, sub_dim), _bar_figure(sub, sub_dim)


def _heatmap_figure(df: pd.DataFrame, sub_dim: str) -> go.Figure:
    if df.empty or "inst_type" not in df.columns or "region" not in df.columns:
        return _empty_figure("Insufficient data for heatmap")

    # Average across sources for the heatmap pivot
    pivot_df = (
        df.groupby(["inst_type", "region"])
        .agg(value=("value", "mean"), confidence_tier=("confidence_tier", "first"))
        .reset_index()
    )
    inst_types = sorted(pivot_df["inst_type"].unique())
    regions    = sorted(pivot_df["region"].unique())

    z, text = [], []
    for it in inst_types:
        z_row, t_row = [], []
        for reg in regions:
            match = pivot_df[(pivot_df["inst_type"] == it) & (pivot_df["region"] == reg)]
            if match.empty:
                z_row.append(None)
                t_row.append("")
            else:
                v = match["value"].iloc[0]
                tier = match["confidence_tier"].iloc[0]
                z_row.append(v if v == v else None)
                label = f"{v:.2f}" if v == v else "—"
                if tier == "insufficient":
                    label += "\n(insuff.)"
                t_row.append(label)
        z.append(z_row)
        text.append(t_row)

    fig = go.Figure(go.Heatmap(
        z=z, x=regions, y=inst_types, text=text,
        texttemplate="%{text}", colorscale="RdYlGn",
        zmin=0, zmax=1,
        hovertemplate="inst_type: %{y}<br>region: %{x}<br>value: %{z:.3f}<extra></extra>",
    ))
    fig.update_layout(
        title=f"{sub_dim} — (inst_type × region)",
        xaxis_title="Region", yaxis_title="Institution type",
        **_DARK,
        margin={"t": 60, "b": 80, "l": 160, "r": 20},
    )
    return fig


def _bar_figure(df: pd.DataFrame, sub_dim: str) -> go.Figure:
    if df.empty or "inst_type" not in df.columns:
        return _empty_figure("No bar data")

    fig = go.Figure()
    regions = sorted(df["region"].dropna().unique()) if "region" in df.columns else ["all"]
    for region in regions:
        sub = df[df["region"] == region] if "region" in df.columns else df
        sub = sub.sort_values("inst_type")
        fig.add_trace(go.Bar(
            name=region,
            x=sub["inst_type"].tolist(),
            y=sub["value"].tolist(),
            marker_color=_REGION_COLOURS.get(str(region), "#aaa"),
            hovertemplate="%{x}<br>" + str(region) + ": %{y:.3f}<extra></extra>",
        ))

    fig.update_layout(
        barmode="group",
        title=f"{sub_dim} by institution type",
        xaxis_title="Institution type", yaxis_title="Value",
        yaxis={"range": [0, 1]},
        **_DARK,
        margin={"t": 60, "b": 100, "l": 60, "r": 20},
        xaxis={"tickangle": -30},
    )
    return fig


def _empty_figure(msg: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(title=msg, **_DARK)
    return fig
