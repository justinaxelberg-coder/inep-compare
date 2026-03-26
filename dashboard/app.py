# dashboard/app.py
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html

from dashboard.data_loader import (
    load_convergence,
    load_fitness_profiles,
    load_geographic,
    load_registry,
    load_enrichment_combined,
    load_source_metadata,
    load_source_reliability_summary,
    load_source_reliability_flags,
)
from dashboard.tabs import fitness as fitness_tab
from dashboard.tabs import convergence as convergence_tab
from dashboard.tabs import registry as registry_tab
from dashboard.tabs import enrichment as enrichment_tab
from dashboard.tabs import reliability as reliability_tab

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_PROCESSED = Path(__file__).parent.parent / "data" / "processed"
_REGISTRY  = Path(__file__).parent.parent / "registry"

logger.info("Loading fitness profiles...")
_fitness_df = load_fitness_profiles(
    db_path=next(
        iter(sorted(_PROCESSED.glob("fitness_*.db"), reverse=True)),
        _PROCESSED / "fitness.db",
    ),
    csv_dir=_PROCESSED,
)

logger.info("Loading convergence data...")
_overlap_df, _divs_df = load_convergence(csv_dir=_PROCESSED)

logger.info("Loading institution registry...")
_registry_df = load_registry(csv_dir=_REGISTRY)

logger.info("Loading enrichment data...")
_geographic_df = load_geographic(csv_dir=_PROCESSED)
_enrichment_df = load_enrichment_combined(csv_dir=_PROCESSED)
_metadata      = load_source_metadata(processed_dir=_PROCESSED)

logger.info("Loading reliability data...")
_reliability_summary_df = load_source_reliability_summary(csv_dir=_PROCESSED)
_reliability_flags_df = load_source_reliability_flags(csv_dir=_PROCESSED)

logger.info(
    "Data ready: %d fitness rows, %d overlap rows, %d institutions",
    len(_fitness_df), len(_overlap_df), len(_registry_df),
)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="INEP Bibliometric Tool — Source Evaluation Dashboard",
    suppress_callback_exceptions=True,
)
server = app.server

app.layout = dbc.Container(
    fluid=True,
    children=[
        dbc.Row(dbc.Col(html.H2(
            "INEP Bibliometric Source Evaluation",
            className="text-center my-3",
        ))),
        dbc.Row(dbc.Col(html.P(
            "Section C — Dashboard | Data: SINAES 2026 evaluation cycle",
            className="text-center text-muted mb-2",
        ))),
        dcc.Tabs(
            id="main-tabs",
            value="tab-fitness",
            children=[
                dcc.Tab(label="Fitness Matrix",       value="tab-fitness",
                        style={"color": "#ccc"}, selected_style={"color": "#fff"}),
                dcc.Tab(label="Convergence Explorer", value="tab-convergence",
                        style={"color": "#ccc"}, selected_style={"color": "#fff"}),
                dcc.Tab(label="Registry Map",         value="tab-registry",
                        style={"color": "#ccc"}, selected_style={"color": "#fff"}),
                dcc.Tab(label="Enrichment",           value="tab-enrichment",
                        style={"color": "#ccc"}, selected_style={"color": "#fff"}),
                dcc.Tab(label="Reliability",          value="tab-reliability",
                        style={"color": "#ccc"}, selected_style={"color": "#fff"}),
            ],
        ),
        html.Div(id="tab-content", className="mt-2"),
    ],
)


@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "value"),
)
def render_tab(tab: str):
    if tab == "tab-fitness":
        return fitness_tab.layout(_fitness_df)
    if tab == "tab-convergence":
        return convergence_tab.layout(_overlap_df, _divs_df)
    if tab == "tab-registry":
        return registry_tab.layout(_registry_df)
    if tab == "tab-enrichment":
        return enrichment_tab.layout(_geographic_df, _enrichment_df, _metadata)
    if tab == "tab-reliability":
        return reliability_tab.layout(_reliability_summary_df, _reliability_flags_df)
    return html.P("Unknown tab.", className="text-muted")


fitness_tab.register_callbacks(app)
convergence_tab.register_callbacks(app)
registry_tab.register_callbacks(app)
enrichment_tab.register_callbacks(app)
reliability_tab.register_callbacks(app)


if __name__ == "__main__":
    logger.info("Starting dashboard on http://localhost:8050")
    app.run(debug=True, host="0.0.0.0", port=8050)
