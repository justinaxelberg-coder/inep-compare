from __future__ import annotations

import pandas as pd
import dash

from dashboard.tabs import reliability as reliability_tab


_SUMMARY = pd.DataFrame([
    {
        "source": "openalex",
        "record_type": "__all__",
        "canonical_works": 10,
        "integration_ready_works": 6,
        "reviewable_disputed_works": 2,
        "not_integration_ready_works": 2,
        "high_confidence_works": 4,
        "medium_confidence_works": 4,
        "low_confidence_works": 2,
        "externally_corroborated_works": 6,
        "major_conflict_works": 2,
        "doi_expected_missing_works": 1,
        "integration_ready_share": 0.6,
        "reviewable_disputed_share": 0.2,
        "not_integration_ready_share": 0.2,
        "high_confidence_share": 0.4,
        "medium_confidence_share": 0.4,
        "low_confidence_share": 0.2,
        "external_corroboration_share": 0.6,
        "major_conflict_share": 0.2,
        "doi_expected_missing_share": 0.1,
    },
    {
        "source": "openalex",
        "record_type": "journal_article",
        "canonical_works": 8,
        "integration_ready_works": 6,
        "reviewable_disputed_works": 1,
        "not_integration_ready_works": 1,
        "high_confidence_works": 4,
        "medium_confidence_works": 3,
        "low_confidence_works": 1,
        "externally_corroborated_works": 6,
        "major_conflict_works": 1,
        "doi_expected_missing_works": 1,
        "integration_ready_share": 0.75,
        "reviewable_disputed_share": 0.125,
        "not_integration_ready_share": 0.125,
        "high_confidence_share": 0.5,
        "medium_confidence_share": 0.375,
        "low_confidence_share": 0.125,
        "external_corroboration_share": 0.75,
        "major_conflict_share": 0.125,
        "doi_expected_missing_share": 0.125,
    },
])

_FLAGS = pd.DataFrame([
    {
        "source": "openalex",
        "record_type": "journal_article",
        "flag": "major_conflict",
        "n_works": 2,
        "denominator": 8,
        "share": 0.25,
    },
    {
        "source": "openalex",
        "record_type": "journal_article",
        "flag": "doi_expected_missing",
        "n_works": 1,
        "denominator": 8,
        "share": 0.125,
    },
])


def test_layout_renders_empty_state_when_no_data():
    layout = reliability_tab.layout(pd.DataFrame(), pd.DataFrame())
    assert layout is not None
    assert "No reliability outputs found" in str(layout)


def test_layout_renders_reliability_views_with_data():
    layout = reliability_tab.layout(_SUMMARY, _FLAGS)
    rendered = str(layout)
    assert "Reliability" in rendered
    assert "reliability-overall-chart" in rendered
    assert "reliability-record-type-chart" in rendered
    assert "reliability-doi-chart" in rendered
    assert "reliability-flag-table" in rendered


def test_outcome_share_chart_uses_expected_columns():
    fig = reliability_tab._outcome_share_figure(_SUMMARY[_SUMMARY["record_type"] == "__all__"])
    assert len(fig.data) == 3


def test_record_type_breakdown_figure_filters_out_overall_rows():
    fig = reliability_tab._record_type_figure(_SUMMARY, metric="integration_ready_share")
    assert fig.data
    assert all(trace.name == "openalex" for trace in fig.data)


def test_flag_table_handles_rows():
    table = reliability_tab._flag_table(_FLAGS)
    assert table is not None


def test_register_callbacks_no_error():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    reliability_tab.register_callbacks(app)
