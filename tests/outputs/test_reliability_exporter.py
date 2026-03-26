from __future__ import annotations

import pandas as pd
import pytest

from outputs.dataset.exporter import DatasetExporter
from run_reliability import _load_latest_matches, _load_latest_records


def test_export_reliability_outputs_writes_expected_files(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)

    source_record_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "has_external_corroboration": True,
                "has_major_conflict": False,
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": [],
            }
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "has_external_corroboration": True,
                "has_major_conflict": False,
                "conflict_fields": [],
                "flags": [],
            }
        ]
    )
    summary_df = pd.DataFrame(
        [
            {
                "source": "openalex",
                "record_type": "__all__",
                "canonical_works": 1,
                "integration_ready_share": 1.0,
                "reviewable_disputed_share": 0.0,
                "not_integration_ready_share": 0.0,
                "high_confidence_share": 1.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 0.0,
                "external_corroboration_share": 1.0,
                "major_conflict_share": 0.0,
                "doi_expected_missing_share": 0.0,
            }
        ]
    )

    paths = exporter.export_reliability_outputs(
        source_record_df,
        canonical_df,
        summary_df,
        run_id="2026-03-25",
    )

    assert set(paths) == {"source_records", "canonical", "summary", "flags", "report"}
    for path in paths.values():
        assert path.exists()

    report = paths["report"].read_text(encoding="utf-8")
    assert "usable coverage" in report
    assert "disputed coverage" in report
    assert "verification-risk shares" in report
    assert "Top Downgrade Reasons" in report


def test_export_reliability_outputs_deduplicates_duplicate_source_work_rows(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)

    source_record_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1_dup",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_2",
                "source": "openalex",
                "source_record_id": "oa_2",
                "record_type": "journal_article",
                "match_basis": "fallback",
                "outcome_state": "not_integration_ready",
                "confidence_band": "low",
                "has_external_corroboration": False,
                "has_major_conflict": False,
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": True,
                "introduced_weak_institution_linkage": True,
                "introduced_doi_expected_missing": True,
                "flags": ["doi_expected_missing", "unverifiable_author_identity"],
            },
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "conflict_fields": ["publication_year"],
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_2",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "not_integration_ready",
                "confidence_band": "low",
                "has_external_corroboration": False,
                "has_major_conflict": False,
                "conflict_fields": [],
                "flags": ["doi_expected_missing", "unverifiable_author_identity"],
            },
        ]
    )
    summary_df = pd.DataFrame(
        [
            {
                "source": "openalex",
                "record_type": "__all__",
                "canonical_works": 2,
                "integration_ready_share": 0.0,
                "reviewable_disputed_share": 0.5,
                "not_integration_ready_share": 0.5,
                "high_confidence_share": 0.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 1.0,
                "external_corroboration_share": 0.5,
                "major_conflict_share": 0.5,
                "doi_expected_missing_share": 0.5,
            }
        ]
    )

    paths = exporter.export_reliability_outputs(
        source_record_df,
        canonical_df,
        summary_df,
        run_id="2026-03-25",
    )
    flags_df = pd.read_csv(paths["flags"])

    major = flags_df[(flags_df["source"] == "openalex") & (flags_df["flag"] == "major_conflict")].iloc[0]
    doi_missing = flags_df[(flags_df["source"] == "openalex") & (flags_df["flag"] == "doi_expected_missing")].iloc[0]

    assert major["n_works"] == 1
    assert major["denominator"] == 2
    assert major["share"] == pytest.approx(0.5)
    assert doi_missing["n_works"] == 1
    assert doi_missing["denominator"] == 2
    assert doi_missing["share"] == pytest.approx(0.5)


def test_export_reliability_outputs_uses_canonical_record_type_for_typed_flags(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)

    source_record_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "thesis",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1_dup",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": ["major_conflict"],
            },
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "conflict_fields": ["record_type"],
                "flags": ["major_conflict"],
            }
        ]
    )
    summary_df = pd.DataFrame(
        [
            {
                "source": "openalex",
                "record_type": "__all__",
                "canonical_works": 1,
                "integration_ready_share": 0.0,
                "reviewable_disputed_share": 1.0,
                "not_integration_ready_share": 0.0,
                "high_confidence_share": 0.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 1.0,
                "external_corroboration_share": 1.0,
                "major_conflict_share": 1.0,
                "doi_expected_missing_share": 0.0,
            }
        ]
    )

    paths = exporter.export_reliability_outputs(
        source_record_df,
        canonical_df,
        summary_df,
        run_id="2026-03-25",
    )
    flags_df = pd.read_csv(paths["flags"])

    typed = flags_df[(flags_df["source"] == "openalex") & (flags_df["record_type"] == "journal_article") & (flags_df["flag"] == "major_conflict")].iloc[0]

    assert typed["n_works"] == 1
    assert typed["record_type"] == "journal_article"
    assert typed["share"] == pytest.approx(1.0)


def test_report_includes_per_source_and_record_type_downgrade_reasons(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)

    source_record_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_2",
                "source": "openalex",
                "source_record_id": "oa_2",
                "record_type": "thesis",
                "match_basis": "fallback",
                "outcome_state": "not_integration_ready",
                "confidence_band": "low",
                "has_external_corroboration": False,
                "has_major_conflict": False,
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": True,
                "introduced_weak_institution_linkage": True,
                "introduced_doi_expected_missing": False,
                "flags": ["missing_critical_verifiability_fields", "unverifiable_author_identity"],
            },
            {
                "canonical_work_id": "cw_3",
                "source": "scopus",
                "source_record_id": "sc_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "has_external_corroboration": True,
                "has_major_conflict": False,
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "flags": [],
            },
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "has_external_corroboration": True,
                "has_major_conflict": True,
                "conflict_fields": ["publication_year"],
                "flags": ["major_conflict"],
            },
            {
                "canonical_work_id": "cw_2",
                "record_type": "thesis",
                "n_sources": 1,
                "sources": ["openalex"],
                "outcome_state": "not_integration_ready",
                "confidence_band": "low",
                "has_external_corroboration": False,
                "has_major_conflict": False,
                "conflict_fields": [],
                "flags": ["missing_critical_verifiability_fields", "unverifiable_author_identity"],
            },
            {
                "canonical_work_id": "cw_3",
                "record_type": "journal_article",
                "n_sources": 1,
                "sources": ["scopus"],
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "has_external_corroboration": True,
                "has_major_conflict": False,
                "conflict_fields": [],
                "flags": [],
            },
        ]
    )
    summary_df = pd.DataFrame(
        [
            {
                "source": "openalex",
                "record_type": "__all__",
                "canonical_works": 2,
                "integration_ready_share": 0.0,
                "reviewable_disputed_share": 0.5,
                "not_integration_ready_share": 0.5,
                "high_confidence_share": 0.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 1.0,
                "external_corroboration_share": 0.5,
                "major_conflict_share": 0.5,
                "doi_expected_missing_share": 0.0,
            },
            {
                "source": "openalex",
                "record_type": "journal_article",
                "canonical_works": 1,
                "integration_ready_share": 0.0,
                "reviewable_disputed_share": 1.0,
                "not_integration_ready_share": 0.0,
                "high_confidence_share": 0.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 1.0,
                "external_corroboration_share": 1.0,
                "major_conflict_share": 1.0,
                "doi_expected_missing_share": 0.0,
            },
            {
                "source": "openalex",
                "record_type": "thesis",
                "canonical_works": 1,
                "integration_ready_share": 0.0,
                "reviewable_disputed_share": 0.0,
                "not_integration_ready_share": 1.0,
                "high_confidence_share": 0.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 1.0,
                "external_corroboration_share": 0.0,
                "major_conflict_share": 0.0,
                "doi_expected_missing_share": 0.0,
            },
            {
                "source": "scopus",
                "record_type": "__all__",
                "canonical_works": 1,
                "integration_ready_share": 1.0,
                "reviewable_disputed_share": 0.0,
                "not_integration_ready_share": 0.0,
                "high_confidence_share": 1.0,
                "medium_confidence_share": 0.0,
                "low_confidence_share": 0.0,
                "external_corroboration_share": 1.0,
                "major_conflict_share": 0.0,
                "doi_expected_missing_share": 0.0,
            },
        ]
    )

    report = exporter.export_reliability_outputs(
        source_record_df,
        canonical_df,
        summary_df,
        run_id="2026-03-25",
    )["report"].read_text(encoding="utf-8")

    assert "## Source: openalex" in report
    assert "## Source: scopus" in report
    assert "missing_critical_verifiability_fields" in report
    assert "Record-Type Breakdown" in report
    assert "journal_article" in report
    assert "thesis" in report


def test_runner_loads_latest_records_and_matches_and_deserialises_nested_fields(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    old_records = pd.DataFrame(
        [
            {
                "source_record_id": "oa_old",
                "title": "Old title",
                "year": 2023,
                "record_type": "journal_article",
                "authors": '[{"name": "Old Author"}]',
                "institutions": "[]",
                "external_ids": "[]",
            }
        ]
    )
    new_records = pd.DataFrame(
        [
            {
                "source_record_id": "oa_new",
                "title": "New title",
                "year": 2024,
                "record_type": "journal_article",
                "authors": '[{"name": "New Author"}]',
                "institutions": "[]",
                "external_ids": "[]",
            }
        ]
    )
    old_records.to_parquet(processed / "records_openalex_2026-03-24.parquet", index=False)
    new_records.to_parquet(processed / "records_openalex_2026-03-25.parquet", index=False)

    old_matches = pd.DataFrame([{"record_id_a": "oa_old", "record_id_b": "x"}])
    new_matches = pd.DataFrame([{"record_id_a": "oa_new", "record_id_b": "y"}])
    old_matches.to_parquet(processed / "matches_phase2_2026-03-24.parquet", index=False)
    new_matches.to_parquet(processed / "matches_phase2_2026-03-25.parquet", index=False)

    records_by_source = _load_latest_records(processed_dir=processed)
    matches_df = _load_latest_matches(processed_dir=processed)

    assert list(records_by_source) == ["openalex"]
    assert records_by_source["openalex"][0]["source_record_id"] == "oa_new"
    assert records_by_source["openalex"][0]["authors"] == [{"name": "New Author"}]
    assert matches_df.iloc[0]["record_id_a"] == "oa_new"


def test_runner_fails_fast_when_records_are_missing(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError, match="No records_\\*.parquet files found in data/processed"):
        _load_latest_records(processed_dir=processed)
