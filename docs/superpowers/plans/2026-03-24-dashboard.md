# INEP Bibliometric Tool — Dashboard (Section C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a three-tab Dash dashboard (`dashboard/app.py`) that visualises fitness matrix scores, source-pair convergence per institution, and the full HEI registry — all read from pre-computed SQLite/CSV outputs with no live API calls and graceful empty states when data files are absent.

**Architecture:** A thin `data_loader.py` module handles all I/O (SQLite primary, CSV fallback, empty-state DataFrames on missing files). Three tab modules under `dashboard/tabs/` each own their layout and callbacks. `app.py` imports and wires everything. The server is started with `python dashboard/app.py` on port 8050.

**Tech Stack:** Dash 4.x, Plotly 6.x, pandas, dash-bootstrap-components 2.x (DARKLY theme), sqlite3 (stdlib). Install: `pip install dash plotly pandas dash-bootstrap-components`.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `dashboard/__init__.py` | Create | Empty package marker |
| `dashboard/data_loader.py` | Create | SQLite/CSV loading, caching, empty-state helpers |
| `dashboard/tabs/__init__.py` | Create | Empty package marker |
| `dashboard/tabs/fitness.py` | Create | Tab 1: heatmap, radar chart, rankings table |
| `dashboard/tabs/convergence.py` | Create | Tab 2: institution dropdown, bar chart, flag badges, summary stats |
| `dashboard/tabs/registry.py` | Create | Tab 3: filterable table, summary cards, PhD bar chart |
| `dashboard/app.py` | Create | Entry point: layout, tab wiring, `if __name__ == "__main__"` |
| `tests/dashboard/__init__.py` | Create | Empty package marker |
| `tests/dashboard/test_data_loader.py` | Create | Unit tests for loader (SQLite, CSV fallback, empty states) |

---

## Task 1: `dashboard/data_loader.py` + `tests/dashboard/test_data_loader.py`

**Files:**
- Create: `dashboard/__init__.py`
- Create: `dashboard/data_loader.py`
- Create: `tests/dashboard/__init__.py`
- Create: `tests/dashboard/test_data_loader.py`

The loader is the foundation everything else depends on. It must never crash the dashboard — every function returns an empty DataFrame with correct columns when data is absent.

- [ ] **Step 1: Write failing tests**

```python
# tests/dashboard/test_data_loader.py
from __future__ import annotations
import sqlite3
import pandas as pd
import pytest
from pathlib import Path

import sys
sys.path.insert(0, ".")

from dashboard.data_loader import (
    load_fitness_profiles,
    load_convergence,
    load_registry,
    FITNESS_COLUMNS,
    CONVERGENCE_COLUMNS,
    REGISTRY_COLUMNS,
)


# --- fixtures ---

@pytest.fixture
def fitness_db(tmp_path):
    db = tmp_path / "fitness.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE fitness_profiles "
        "(source TEXT, inst_type TEXT, coverage REAL, data_quality REAL, "
        "reliability REAL, accessibility REAL, social_impact REAL, "
        "governance REAL, innovation_link REAL, composite REAL)"
    )
    conn.execute(
        "INSERT INTO fitness_profiles VALUES "
        "('openalex','federal_university',0.17,0.29,0.60,0.99,0.25,0.90,0.0,0.49)"
    )
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def fitness_csv(tmp_path):
    df = pd.DataFrame([{
        "source": "scopus", "inst_type": "federal_university",
        "coverage": 0.17, "data_quality": 0.31, "reliability": 0.47,
        "accessibility": 0.33, "social_impact": 0.24, "governance": 0.43,
        "innovation_link": 0.0, "composite": 0.30,
    }])
    path = tmp_path / "fitness_matrix_2026-03-24.csv"
    df.to_csv(path, index=False)
    return tmp_path


@pytest.fixture
def overlap_csv(tmp_path):
    df = pd.DataFrame([{
        "source_a": "openalex", "source_b": "scopus",
        "e_mec_code": "1982", "n_a": 500, "n_b": 225,
        "n_matched": 155, "overlap_pct_a": 0.31,
        "overlap_pct_b": 0.69, "overlap_pct_min": 0.31,
    }])
    path = tmp_path / "overlap_phase2_2026-03-23.csv"
    df.to_csv(path, index=False)

    div_df = pd.DataFrame([{
        "e_mec_code": "1982", "institution_name": "Inst A",
        "source_a": "openalex", "source_b": "scopus",
        "count_a": 500, "count_b": 225,
        "discrepancy_pct": 0.55, "direction": "a_higher",
    }])
    div_df.to_csv(tmp_path / "divergences_phase2_2026-03-23.csv", index=False)
    return tmp_path


@pytest.fixture
def registry_csv(tmp_path):
    df = pd.DataFrame([{
        "e_mec_code": "000001", "name": "UFMT", "abbreviation": "UFMT",
        "city": "Cuiabá", "state": "MT", "faculty_with_phd": 1263,
        "faculty_total": 1588, "org_type": "federal", "category": "universidade",
        "region": "Centro-Oeste", "sinaes_type": "federal_university",
        "phd_faculty_share": 0.80, "censo_year": 2023,
    }])
    path = tmp_path / "institutions.csv"
    df.to_csv(path, index=False)
    return tmp_path


# --- load_fitness_profiles ---

def test_fitness_loads_from_sqlite(fitness_db, tmp_path):
    df = load_fitness_profiles(db_path=fitness_db, csv_dir=tmp_path)
    assert len(df) == 1


def test_fitness_returns_correct_columns_from_sqlite(fitness_db, tmp_path):
    df = load_fitness_profiles(db_path=fitness_db, csv_dir=tmp_path)
    assert set(FITNESS_COLUMNS).issubset(df.columns)


def test_fitness_falls_back_to_csv_when_db_absent(fitness_csv):
    df = load_fitness_profiles(
        db_path=fitness_csv / "nonexistent.db",
        csv_dir=fitness_csv,
    )
    assert len(df) == 1
    assert df.iloc[0]["source"] == "scopus"


def test_fitness_returns_empty_df_when_nothing_present(tmp_path):
    df = load_fitness_profiles(db_path=tmp_path / "x.db", csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == FITNESS_COLUMNS
    assert len(df) == 0


# --- load_convergence ---

def test_convergence_loads_overlap_and_divergences(overlap_csv):
    overlap, divs = load_convergence(csv_dir=overlap_csv)
    assert len(overlap) >= 1
    assert len(divs) >= 1


def test_convergence_overlap_has_required_columns(overlap_csv):
    overlap, _ = load_convergence(csv_dir=overlap_csv)
    for col in CONVERGENCE_COLUMNS:
        assert col in overlap.columns


def test_convergence_returns_empty_dfs_when_absent(tmp_path):
    overlap, divs = load_convergence(csv_dir=tmp_path)
    assert isinstance(overlap, pd.DataFrame)
    assert isinstance(divs, pd.DataFrame)
    assert len(overlap) == 0
    assert len(divs) == 0


# --- load_registry ---

def test_registry_loads_institutions(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    assert len(df) == 1
    assert df.iloc[0]["sinaes_type"] == "federal_university"


def test_registry_has_required_columns(registry_csv):
    df = load_registry(csv_dir=registry_csv)
    for col in REGISTRY_COLUMNS:
        assert col in df.columns


def test_registry_returns_empty_df_when_absent(tmp_path):
    df = load_registry(csv_dir=tmp_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
```

- [ ] **Step 2: Run failing tests**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
pytest tests/dashboard/test_data_loader.py -v
```

Expected: `ModuleNotFoundError: No module named 'dashboard'`

- [ ] **Step 3: Create `dashboard/__init__.py` and `tests/dashboard/__init__.py`**

Both files are empty — just package markers.

- [ ] **Step 4: Write `dashboard/data_loader.py`**

```python
# dashboard/data_loader.py
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

FITNESS_COLUMNS: list[str] = [
    "source", "inst_type", "coverage", "data_quality", "reliability",
    "accessibility", "social_impact", "governance", "innovation_link", "composite",
]
CONVERGENCE_COLUMNS: list[str] = [
    "source_a", "source_b", "e_mec_code", "n_a", "n_b",
    "n_matched", "overlap_pct_a", "overlap_pct_b", "overlap_pct_min",
]
REGISTRY_COLUMNS: list[str] = [
    "e_mec_code", "name", "abbreviation", "city", "state",
    "faculty_with_phd", "faculty_total", "org_type", "category",
    "region", "sinaes_type", "phd_faculty_share", "censo_year",
]

_DEFAULT_PROCESSED = Path("data/processed")
_DEFAULT_REGISTRY  = Path("registry")


def load_fitness_profiles(
    db_path: Path | None = None,
    csv_dir: Path | None = None,
) -> pd.DataFrame:
    """Return fitness_profiles as a DataFrame.

    Priority: SQLite fitness_profiles table → CSV glob fitness_matrix_*.csv.
    Returns empty DataFrame with FITNESS_COLUMNS on any failure.
    """
    db_path = Path(db_path) if db_path else _DEFAULT_PROCESSED / "fitness.db"
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED

    # 1. Try SQLite
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as conn:
                tables = [r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()]
                table = "fitness_profiles" if "fitness_profiles" in tables else (
                    "fitness_matrix" if "fitness_matrix" in tables else None
                )
                if table:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    logger.info(f"Loaded {len(df)} fitness rows from {db_path}:{table}")
                    return _ensure_columns(df, FITNESS_COLUMNS)
        except Exception as exc:
            logger.warning(f"SQLite load failed ({db_path}): {exc}")

    # 2. CSV fallback
    files = sorted(Path(csv_dir).glob("fitness_matrix_*.csv"))
    if files:
        try:
            df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
            logger.info(f"Loaded {len(df)} fitness rows from {len(files)} CSV(s)")
            return _ensure_columns(df, FITNESS_COLUMNS)
        except Exception as exc:
            logger.warning(f"CSV fitness load failed: {exc}")

    logger.warning("No fitness data found — returning empty DataFrame")
    return pd.DataFrame(columns=FITNESS_COLUMNS)


def load_convergence(
    csv_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (overlap_df, divergences_df).

    Reads overlap_phase2_*.csv and divergences_phase2_*.csv from csv_dir.
    Returns empty DataFrames on missing files.
    """
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED

    overlap_files = sorted(Path(csv_dir).glob("overlap_phase2_*.csv"))
    div_files     = sorted(Path(csv_dir).glob("divergences_phase2_*.csv"))

    overlap = _read_csvs(overlap_files, CONVERGENCE_COLUMNS, "overlap")
    divs    = _read_csvs(div_files,    [], "divergences")

    return overlap, divs


def load_registry(
    csv_dir: Path | None = None,
) -> pd.DataFrame:
    """Return institution registry DataFrame.

    Reads registry/institutions.csv (or csv_dir/institutions.csv).
    Returns empty DataFrame on missing file.
    """
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_REGISTRY
    path = Path(csv_dir) / "institutions.csv"
    if path.exists():
        try:
            df = pd.read_csv(path, dtype={"e_mec_code": str})
            logger.info(f"Loaded {len(df)} institutions from {path}")
            return df
        except Exception as exc:
            logger.warning(f"Registry load failed: {exc}")
    logger.warning(f"Registry file not found at {path}")
    return pd.DataFrame(columns=REGISTRY_COLUMNS)


# --- helpers ---

def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Add missing columns as NaN; return df with at least those columns."""
    for col in columns:
        if col not in df.columns:
            df[col] = float("nan")
    return df


def _read_csvs(
    files: list[Path],
    required_cols: list[str],
    label: str,
) -> pd.DataFrame:
    if not files:
        logger.warning(f"No {label} CSV files found")
        return pd.DataFrame(columns=required_cols)
    try:
        df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        logger.info(f"Loaded {len(df)} {label} rows from {len(files)} file(s)")
        return _ensure_columns(df, required_cols)
    except Exception as exc:
        logger.warning(f"{label} CSV load failed: {exc}")
        return pd.DataFrame(columns=required_cols)
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
pytest tests/dashboard/test_data_loader.py -v
```

Expected: `10 passed`

- [ ] **Step 6: Git commit**

```bash
git add dashboard/__init__.py dashboard/data_loader.py \
        tests/dashboard/__init__.py tests/dashboard/test_data_loader.py
git commit -m "feat(dashboard): add data_loader with SQLite/CSV loading and empty-state helpers"
```

---

## Task 2: `dashboard/tabs/fitness.py` — Tab 1: Heatmap + Radar + Rankings

**Files:**
- Create: `dashboard/tabs/__init__.py`
- Create: `dashboard/tabs/fitness.py`

- [ ] **Step 1: Create `dashboard/tabs/__init__.py`** (empty)

- [ ] **Step 2: Write `dashboard/tabs/fitness.py`**

```python
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
```

- [ ] **Step 3: Smoke-test layout builds without crashing**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python3 -c "
import sys; sys.path.insert(0, '.')
import pandas as pd
from dashboard.tabs.fitness import layout
df = pd.DataFrame([{
    'source': 'openalex', 'inst_type': 'federal_university',
    'coverage': 0.17, 'data_quality': 0.29, 'reliability': 0.60,
    'accessibility': 0.99, 'social_impact': 0.25, 'governance': 0.90,
    'innovation_link': 0.0, 'composite': 0.49,
}])
assert layout(df) is not None
print('fitness layout OK')
"
```

Expected: `fitness layout OK`

- [ ] **Step 4: Git commit**

```bash
git add dashboard/tabs/__init__.py dashboard/tabs/fitness.py
git commit -m "feat(dashboard): add Tab 1 fitness heatmap, radar chart, and rankings table"
```

---

## Task 3: `dashboard/tabs/convergence.py` — Tab 2: Bar Chart + Flags + Summary Stats

**Files:**
- Create: `dashboard/tabs/convergence.py`

- [ ] **Step 1: Write `dashboard/tabs/convergence.py`**

```python
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
```

- [ ] **Step 2: Smoke-test layout builds without crashing**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python3 -c "
import sys; sys.path.insert(0, '.')
import pandas as pd
from dashboard.tabs.convergence import layout
overlap = pd.DataFrame([{
    'source_a': 'openalex', 'source_b': 'scopus', 'e_mec_code': '1982',
    'n_a': 500, 'n_b': 225, 'n_matched': 155,
    'overlap_pct_a': 0.31, 'overlap_pct_b': 0.69, 'overlap_pct_min': 0.31,
}])
divs = pd.DataFrame()
assert layout(overlap, divs) is not None
print('convergence layout OK')
"
```

Expected: `convergence layout OK`

- [ ] **Step 3: Git commit**

```bash
git add dashboard/tabs/convergence.py
git commit -m "feat(dashboard): add Tab 2 convergence bar chart, divergence badges, summary stats"
```

---

## Task 4: `dashboard/tabs/registry.py` — Tab 3: Table + Cards + PhD Bar Chart

**Files:**
- Create: `dashboard/tabs/registry.py`

- [ ] **Step 1: Write `dashboard/tabs/registry.py`**

```python
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
        df = _apply_filters(df, regions, types, states)
        return _data_table(df), _phd_bar_figure(df), _summary_cards(df)


def _options(df: pd.DataFrame, col: str) -> list[dict]:
    if df.empty or col not in df.columns:
        return []
    return [{"label": str(v), "value": str(v)} for v in sorted(df[col].dropna().unique())]


def _apply_filters(df, regions, types, states):
    if df.empty:
        return df
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
    labels = [n[:30] + "…" if len(str(n)) > 30 else str(n) for n in top["name"]]
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
```

- [ ] **Step 2: Smoke-test layout builds without crashing**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python3 -c "
import sys; sys.path.insert(0, '.')
import pandas as pd
from dashboard.tabs.registry import layout
df = pd.DataFrame([{
    'e_mec_code': '000001', 'name': 'UFMT', 'abbreviation': 'UFMT',
    'city': 'Cuiaba', 'state': 'MT', 'faculty_with_phd': 1263,
    'faculty_total': 1588, 'org_type': 'federal', 'category': 'universidade',
    'region': 'Centro-Oeste', 'sinaes_type': 'federal_university',
    'phd_faculty_share': 0.80, 'censo_year': 2023,
}])
assert layout(df) is not None
print('registry layout OK')
"
```

Expected: `registry layout OK`

- [ ] **Step 3: Git commit**

```bash
git add dashboard/tabs/registry.py
git commit -m "feat(dashboard): add Tab 3 registry table, summary cards, PhD bar chart"
```

---

## Task 5: `dashboard/app.py` — Entry Point Wiring All Tabs

**Files:**
- Create: `dashboard/app.py`

- [ ] **Step 1: Write `dashboard/app.py`**

```python
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
    load_registry,
)
from dashboard.tabs import fitness as fitness_tab
from dashboard.tabs import convergence as convergence_tab
from dashboard.tabs import registry as registry_tab

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

logger.info(
    f"Data ready: {len(_fitness_df)} fitness rows, "
    f"{len(_overlap_df)} overlap rows, "
    f"{len(_registry_df)} institutions"
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
    return html.P("Unknown tab.", className="text-muted")


fitness_tab.register_callbacks(app)
convergence_tab.register_callbacks(app)
registry_tab.register_callbacks(app)


if __name__ == "__main__":
    logger.info("Starting dashboard on http://localhost:8050")
    app.run(debug=True, host="0.0.0.0", port=8050)
```

- [ ] **Step 2: Install dependencies**

```bash
pip install dash dash-bootstrap-components plotly
```

- [ ] **Step 3: Verify the app imports and initialises without error**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python3 -c "
import sys; sys.path.insert(0, '.')
import importlib.util
spec = importlib.util.spec_from_file_location('app', 'dashboard/app.py')
mod  = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
assert mod.app is not None
print('app.py imports OK, Dash app initialised')
"
```

Expected: `app.py imports OK, Dash app initialised`

- [ ] **Step 4: Run the full test suite — no regressions**

```bash
pytest tests/ -v --tb=short
```

Expected: All 157 pre-existing tests + 10 new data_loader tests pass (167 total). No failures.

- [ ] **Step 5: Manual smoke test**

```bash
python dashboard/app.py
```

Navigate to `http://localhost:8050` and verify:
- Tab 1: heatmap, radar on source select, rankings table
- Tab 2: dropdown populated, bar chart + badges update on institution select
- Tab 3: summary cards (1,941 faculdades etc.), top-20 PhD chart, filterable table

- [ ] **Step 6: Git commit**

```bash
git add dashboard/app.py
git commit -m "feat(dashboard): add app.py entry point — run with python dashboard/app.py"
```

---

## Reviewer Notes

- `dcc.Store` serialises DataFrame to JSON at layout-build time; 2,580-row registry ≈ 500KB — within Dash defaults.
- `data_loader.py` handles both `fitness_profiles` and `fitness_matrix` table names for SQLite compatibility.
- Empty-state logic tested in Task 1 ensures no tab crashes when `data/processed/` is absent.
