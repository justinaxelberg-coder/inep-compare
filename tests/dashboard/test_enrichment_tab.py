# tests/dashboard/test_enrichment_tab.py
from __future__ import annotations
import pandas as pd
import pytest
import dash
from dashboard.tabs.enrichment import layout, register_callbacks

_COMBINED = pd.DataFrame([
    {"source": "openalex", "inst_type": "federal_university", "region": "Sudeste",
     "sub_dimension": "geographic_coverage_gap", "value": 0.8, "n_papers": 500,
     "confidence_tier": "reliable"},
    {"source": "scopus", "inst_type": "isolated_faculty", "region": "Norte",
     "sub_dimension": "sensitivity", "value": 0.41, "n_papers": 47,
     "confidence_tier": "moderate"},
])
_META = {"scopus": {"sdg_available": False}}


def test_layout_renders():
    result = layout(_COMBINED, _META)
    assert result is not None


def test_layout_empty_df():
    result = layout(pd.DataFrame(), {})
    assert result is not None


def test_register_callbacks_no_error():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)  # should not raise


def test_scopus_sdg_caveat_shown():
    result = layout(_COMBINED, {"scopus": {"sdg_available": False}})
    assert "Scopus" in str(result)


def test_chart_component_ids_present():
    result = str(layout(_COMBINED, _META))
    assert "enrichment-heatmap" in result
    assert "enrichment-bar-chart" in result


def test_layout_with_combined_data():
    result = layout(_COMBINED, _META)
    assert result is not None


def test_no_crash_missing_columns():
    bad_df = pd.DataFrame([{"source": "openalex"}])
    result = layout(bad_df, {})
    assert result is not None


def test_callbacks_registered():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)
    cb_ids = [str(cb) for cb in app.callback_map.keys()]
    assert any("enrichment" in cb_id for cb_id in cb_ids)
