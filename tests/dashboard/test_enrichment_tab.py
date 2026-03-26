# tests/dashboard/test_enrichment_tab.py
from __future__ import annotations
import pandas as pd
import pytest
import dash
from dashboard.tabs.enrichment import layout, register_callbacks

_GEO = pd.DataFrame([
    {"source": "openalex", "inst_type": "federal_university", "region": "Sudeste",
     "n_records": 70, "source_publication_share": 0.70, "peer_mean_share": 0.45,
     "comparative_skew": 0.25, "cohort_institution_share": 0.33,
     "cohort_phd_faculty_share": 0.50, "delta_vs_cohort_institution_share": 0.37,
     "delta_vs_cohort_phd_faculty_share": 0.20, "cohort_institutions": 1},
    {"source": "scopus", "inst_type": "federal_university", "region": "Sudeste",
     "n_records": 20, "source_publication_share": 0.20, "peer_mean_share": 0.45,
     "comparative_skew": -0.25, "cohort_institution_share": 0.33,
     "cohort_phd_faculty_share": 0.50, "delta_vs_cohort_institution_share": -0.13,
     "delta_vs_cohort_phd_faculty_share": -0.30, "cohort_institutions": 1},
])
_COMBINED = pd.DataFrame([
    {"source": "scopus", "inst_type": "isolated_faculty", "region": "Norte",
     "sub_dimension": "sensitivity", "value": 0.41, "n_papers": 47,
     "confidence_tier": "moderate"},
])
_META = {"scopus": {"sdg_available": False}}


def test_layout_renders():
    result = layout(_GEO, _COMBINED, _META)
    assert result is not None


def test_layout_empty_df():
    result = layout(pd.DataFrame(), pd.DataFrame(), {})
    assert result is not None


def test_register_callbacks_no_error():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)  # should not raise


def test_scopus_sdg_caveat_shown():
    result = layout(_GEO, _COMBINED, {"scopus": {"sdg_available": False}})
    assert "Scopus" in str(result)


def test_chart_component_ids_present():
    result = str(layout(_GEO, _COMBINED, _META))
    assert "geographic-skew-heatmap" in result
    assert "geographic-share-bar-chart" in result
    assert "enrichment-heatmap" in result
    assert "enrichment-bar-chart" in result


def test_layout_with_combined_data():
    result = layout(_GEO, _COMBINED, _META)
    assert result is not None


def test_no_crash_missing_columns():
    bad_df = pd.DataFrame([{"source": "openalex"}])
    result = layout(bad_df, bad_df, {})
    assert result is not None


def test_field_adjustment_caveat_shown():
    result = layout(_GEO, _COMBINED, _META)
    assert "field-adjusted fairness estimate" in str(result)


def test_callbacks_registered():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)
    cb_ids = [str(cb) for cb in app.callback_map.keys()]
    assert any("enrichment" in cb_id for cb_id in cb_ids)
