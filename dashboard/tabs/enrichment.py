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


def layout(geographic_df: pd.DataFrame, combined_df: pd.DataFrame, metadata: dict) -> html.Div:
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
            "policy_document_rate (Overton), patent_link_rate (Derwent / The Lens). "
            "Geographic skew is a comparative coverage pattern, not a field-adjusted fairness estimate.",
        ],
        style={"color": "#aaa", "fontSize": "0.82rem", "marginBottom": "16px"},
        id="enrichment-caveat-notes",
    )

    store_data = combined_df.to_json(orient="records") if not combined_df.empty else "[]"
    geographic_store = geographic_df.to_json(orient="records") if not geographic_df.empty else "[]"
    stratum_options = []
    if not geographic_df.empty and {"inst_type", "region"}.issubset(geographic_df.columns):
        strata = (
            geographic_df[["inst_type", "region"]]
            .drop_duplicates()
            .sort_values(["inst_type", "region"])
        )
        stratum_options = [
            {"label": f"{row.inst_type} | {row.region}", "value": f"{row.inst_type}||{row.region}"}
            for row in strata.itertuples()
        ]
    geo_sources = (
        sorted(geographic_df["source"].dropna().unique().tolist())
        if not geographic_df.empty and "source" in geographic_df.columns
        else []
    )

    return html.Div([
        html.H4("Enrichment — Source Quality by Institution Type & Region",
                className="mt-3 mb-2"),
        *scopus_banner,
        pending_notes,

        html.H5("Geographic Comparison", className="mt-3 mb-2"),
        html.Div([
            html.Div([
                html.Label("Source(s):", style={"fontSize": "0.85rem"}),
                dcc.Dropdown(
                    id="geographic-source-filter",
                    options=[{"label": s, "value": s} for s in geo_sources],
                    value=geo_sources,
                    multi=True,
                    style={"color": "#000", "minWidth": "280px"},
                ),
            ], style={"marginRight": "24px"}),
            html.Div([
                html.Label("Strata:", style={"fontSize": "0.85rem"}),
                dcc.Dropdown(
                    id="geographic-stratum-filter",
                    options=stratum_options,
                    value=[opt["value"] for opt in stratum_options],
                    multi=True,
                    style={"color": "#000", "minWidth": "360px"},
                ),
            ]),
        ], style={"display": "flex", "flexWrap": "wrap", "gap": "8px",
                  "marginBottom": "20px", "alignItems": "flex-end"}),
        dcc.Graph(id="geographic-skew-heatmap"),
        dcc.Graph(id="geographic-share-bar-chart"),

        # Filters row
        html.H5("Other Enrichment Signals", className="mt-4 mb-2"),
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
        dcc.Store(id="geographic-data-store", data=geographic_store),
    ])


def register_callbacks(app) -> None:

    @app.callback(
        Output("geographic-skew-heatmap", "figure"),
        Output("geographic-share-bar-chart", "figure"),
        Input("geographic-source-filter", "value"),
        Input("geographic-stratum-filter", "value"),
        State("geographic-data-store", "data"),
    )
    def update_geographic_charts(sources, selected_strata, data_json):
        if not data_json or data_json == "[]":
            empty = _empty_figure("No geographic comparison data")
            return empty, empty

        df = pd.read_json(data_json, orient="records")
        for col in [
            "n_records",
            "source_publication_share",
            "peer_mean_share",
            "comparative_skew",
            "cohort_institution_share",
            "cohort_phd_faculty_share",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if sources:
            df = df[df["source"].isin(sources)]
        if selected_strata:
            keys = df["inst_type"].astype(str) + "||" + df["region"].astype(str)
            df = df[keys.isin(selected_strata)]

        if df.empty:
            empty = _empty_figure("No geographic comparison data for selected filters")
            return empty, empty

        return _geographic_heatmap_figure(df), _geographic_bar_figure(df)

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


def _geographic_heatmap_figure(df: pd.DataFrame) -> go.Figure:
    required = {
        "source",
        "inst_type",
        "region",
        "comparative_skew",
        "source_publication_share",
        "peer_mean_share",
        "cohort_institution_share",
        "cohort_phd_faculty_share",
        "n_records",
    }
    if df.empty or not required.issubset(df.columns):
        return _empty_figure("Insufficient data for geographic heatmap")

    df = df.copy()
    df["stratum"] = df["inst_type"].astype(str) + " | " + df["region"].astype(str)
    sources = sorted(df["source"].astype(str).unique().tolist())
    strata = sorted(df["stratum"].astype(str).unique().tolist())

    z, text, custom = [], [], []
    for source in sources:
        z_row, text_row, custom_row = [], [], []
        for stratum in strata:
            match = df[(df["source"] == source) & (df["stratum"] == stratum)]
            if match.empty:
                z_row.append(None)
                text_row.append("")
                custom_row.append([None, None, None, None, None])
                continue
            row = match.iloc[0]
            skew = row["comparative_skew"]
            z_row.append(skew if skew == skew else None)
            text_row.append(f"{skew:+.2f}" if skew == skew else "—")
            custom_row.append([
                row["source_publication_share"],
                row["peer_mean_share"],
                row["cohort_institution_share"],
                row["cohort_phd_faculty_share"],
                int(row["n_records"]),
            ])
        z.append(z_row)
        text.append(text_row)
        custom.append(custom_row)

    fig = go.Figure(go.Heatmap(
        z=z,
        x=strata,
        y=sources,
        text=text,
        texttemplate="%{text}",
        customdata=custom,
        colorscale="RdBu",
        zmid=0.0,
        hovertemplate=(
            "source: %{y}<br>stratum: %{x}<br>"
            "comparative_skew: %{z:+.3f}<br>"
            "source_publication_share: %{customdata[0]:.3f}<br>"
            "peer_mean_share: %{customdata[1]:.3f}<br>"
            "cohort_institution_share: %{customdata[2]:.3f}<br>"
            "cohort_phd_faculty_share: %{customdata[3]:.3f}<br>"
            "n_records: %{customdata[4]}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title="Comparative geographic skew by source",
        xaxis_title="Institution type | region",
        yaxis_title="Source",
        **_DARK,
        margin={"t": 60, "b": 110, "l": 100, "r": 20},
        xaxis={"tickangle": -35},
    )
    return fig


def _geographic_bar_figure(df: pd.DataFrame) -> go.Figure:
    required = {"source", "inst_type", "region", "source_publication_share"}
    if df.empty or not required.issubset(df.columns):
        return _empty_figure("No geographic share data")

    df = df.copy()
    df["stratum"] = df["inst_type"].astype(str) + " | " + df["region"].astype(str)
    fig = go.Figure()
    for source in sorted(df["source"].astype(str).unique().tolist()):
        sub = df[df["source"] == source].sort_values("stratum")
        fig.add_trace(go.Bar(
            name=source,
            x=sub["stratum"].tolist(),
            y=sub["source_publication_share"].tolist(),
        ))
    fig.update_layout(
        barmode="group",
        title="Raw source publication share by selected stratum",
        xaxis_title="Institution type | region",
        yaxis_title="Source publication share",
        yaxis={"tickformat": ".0%"},
        **_DARK,
        margin={"t": 60, "b": 120, "l": 60, "r": 20},
        xaxis={"tickangle": -35},
    )
    return fig


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
