from __future__ import annotations

import pandas as pd

from outputs.dataset.exporter import DatasetExporter


def test_phase2_report_includes_geographic_skew_summary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    geo = pd.DataFrame([
        {
            "source": "openalex",
            "inst_type": "federal_university",
            "region": "Sudeste",
            "n_records": 70,
            "source_publication_share": 0.70,
            "peer_mean_share": 0.45,
            "comparative_skew": 0.25,
            "cohort_institution_share": 1 / 3,
            "cohort_phd_faculty_share": 0.50,
            "delta_vs_cohort_institution_share": 0.37,
            "delta_vs_cohort_phd_faculty_share": 0.20,
            "cohort_institutions": 1,
        },
        {
            "source": "openalex",
            "inst_type": "private_university",
            "region": "Sudeste",
            "n_records": 10,
            "source_publication_share": 0.10,
            "peer_mean_share": 0.45,
            "comparative_skew": -0.35,
            "cohort_institution_share": 1 / 3,
            "cohort_phd_faculty_share": 0.25,
            "delta_vs_cohort_institution_share": -0.23,
            "delta_vs_cohort_phd_faculty_share": -0.15,
            "cohort_institutions": 1,
        },
    ])
    geo.to_csv(processed / "geographic_coverage_2026-03-25.csv", index=False)

    exporter = DatasetExporter(output_dir=tmp_path / "out")
    report = exporter.export_phase2_report(
        coverage_results=[],
        oa_results=[],
        convergence={},
        run_id="phase2_test",
        config={"sources": ["openalex", "scopus"], "start_year": 2023, "end_year": 2023, "max_records": None},
    )

    content = report.read_text(encoding="utf-8")
    assert "Geographic Skew by Source" in content
    assert "not a field-adjusted fairness estimate" in content
    assert "openalex" in content
