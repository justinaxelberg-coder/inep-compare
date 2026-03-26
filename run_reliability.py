from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from connectors.api.crossref import CrossrefConnector
from convergence.external_validation import external_corroboration_for_work, _normalise_doi
from convergence.reliability import (
    build_canonical_work_summary,
    build_source_record_reliability_table,
    build_source_reliability_summary,
    canonical_ids_from_records,
)
from convergence.reliability_rules import (
    AUTHOR_NONE,
    INSTITUTION_NONE,
    LOCATOR_NONE,
    classify_author_strength,
    classify_institution_strength,
    classify_locator_strength,
    confidence_band_for_work,
    flags_for_work,
    outcome_state_for_work,
)
from outputs.dataset.exporter import DatasetExporter

logger = logging.getLogger(__name__)

PROCESSED = Path("data/processed")
_NESTED_COLUMNS = [
    "authors",
    "institutions",
    "fields",
    "funding",
    "patent_citations",
    "external_ids",
]


def _deserialise_nested(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _latest_run_id(files: list[Path], extractor) -> str:
    run_ids: list[str] = []
    for path in files:
        try:
            run_ids.append(extractor(path))
        except ValueError:
            continue
    if not run_ids:
        raise FileNotFoundError("No matching parquet files found in data/processed.")
    return max(run_ids)


def _records_run_id(path: Path) -> str:
    source, run_id = _split_records_path(path)
    if not source or not run_id:
        raise ValueError(f"Unexpected records filename: {path.name}")
    return run_id


def _matches_run_id(path: Path) -> str:
    stem = path.stem
    prefix = "matches_phase2_"
    if not stem.startswith(prefix):
        raise ValueError(f"Unexpected matches filename: {path.name}")
    run_id = stem[len(prefix) :]
    if not run_id:
        raise ValueError(f"Unexpected matches filename: {path.name}")
    return run_id


def _split_records_path(path: Path) -> tuple[str, str]:
    stem = path.stem
    prefix = "records_"
    if not stem.startswith(prefix):
        raise ValueError(f"Unexpected records filename: {path.name}")
    remainder = stem[len(prefix) :]
    if "_" not in remainder:
        raise ValueError(f"Unexpected records filename: {path.name}")
    source, run_id = remainder.rsplit("_", 1)
    return source, run_id


def _load_latest_records(processed_dir: Path = PROCESSED) -> dict[str, list[dict]]:
    files = sorted(processed_dir.glob("records_*.parquet"))
    if not files:
        raise FileNotFoundError(
            "No records_*.parquet files found in data/processed. "
            "Run the harvest/export pipeline first."
        )

    latest_run_id = _latest_run_id(files, _records_run_id)
    records_by_source: dict[str, list[dict]] = {}
    for path in files:
        source, run_id = _split_records_path(path)
        if run_id != latest_run_id:
            continue
        df = pd.read_parquet(path)
        for column in _NESTED_COLUMNS:
            if column in df.columns:
                df[column] = df[column].apply(_deserialise_nested)
        records_by_source[source] = df.to_dict(orient="records")

    if not records_by_source:
        raise FileNotFoundError(
            f"No records_*.parquet files found for latest run_id {latest_run_id!r} "
            f"in {processed_dir}."
        )
    return records_by_source


def _load_latest_matches(processed_dir: Path = PROCESSED) -> pd.DataFrame:
    files = sorted(processed_dir.glob("matches_phase2_*.parquet"))
    if not files:
        raise FileNotFoundError(
            "No matches_phase2_*.parquet files found in data/processed. "
            "Run the phase 2 matching pipeline first."
        )

    latest_run_id = _latest_run_id(files, _matches_run_id)
    latest_path = processed_dir / f"matches_phase2_{latest_run_id}.parquet"
    return pd.read_parquet(latest_path)


def _normalise_record_type(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip().lower().replace("-", "_") or None


def _coerce_scalar(value: object) -> object:
    if isinstance(value, (list, tuple)):
        for item in value:
            if item not in (None, ""):
                return item
        return None
    return value


def _validate_crossref_record(record: dict, connector: CrossrefConnector | None) -> dict | None:
    if connector is None:
        return None
    doi = _normalise_doi(record.get("doi"))
    if not doi:
        return None
    return connector.validate_doi(doi)


def _build_work_rows(
    records_by_source: dict[str, list[dict]],
    mapping: dict[tuple[str, str], dict],
    connector: CrossrefConnector | None,
) -> pd.DataFrame:
    rows: list[dict] = []

    for source, records in sorted(records_by_source.items()):
        for record in records:
            source_record_id = str(record.get("source_record_id") or record.get("record_id") or "").strip()
            if not source_record_id:
                logger.warning("Skipping %s record without source_record_id", source)
                continue

            key = (source, source_record_id)
            canonical = mapping.get(
                key,
                {
                    "canonical_work_id": f"unmatched::{source}::{source_record_id}",
                    "match_basis": "fallback",
                },
            )

            work = dict(record)
            work["source"] = source
            work["source_record_id"] = source_record_id
            work["canonical_work_id"] = canonical["canonical_work_id"]
            work["match_basis"] = canonical["match_basis"]
            work["record_type"] = _normalise_record_type(
                _coerce_scalar(record.get("record_type") or record.get("type"))
            )
            work["title"] = _coerce_scalar(record.get("title"))
            work["year"] = _coerce_scalar(record.get("year") or record.get("published_year"))

            crossref_payload = _validate_crossref_record(work, connector)
            validation = external_corroboration_for_work(work, crossref_payload)
            locator_strength = classify_locator_strength(work)
            author_strength = classify_author_strength(work)
            institution_strength = classify_institution_strength(work)
            has_verifiable_author = author_strength != AUTHOR_NONE
            has_verifiable_institution = institution_strength != INSTITUTION_NONE
            has_stable_locator = locator_strength != LOCATOR_NONE
            work_identity_resolved = bool(work.get("title")) and bool(work.get("year")) and bool(work.get("record_type"))

            flags = flags_for_work(
                work,
                validation["has_external_corroboration"],
                validation["has_major_conflict"],
                has_verifiable_author=has_verifiable_author,
                has_verifiable_institution=has_verifiable_institution,
                has_stable_locator=has_stable_locator,
                work_identity_resolved=work_identity_resolved,
            )

            rows.append(
                {
                    "canonical_work_id": work["canonical_work_id"],
                    "source": source,
                    "source_record_id": source_record_id,
                    "record_type": work["record_type"],
                    "match_basis": work["match_basis"],
                    "outcome_state": outcome_state_for_work(
                        flags=flags,
                        has_external_corroboration=validation["has_external_corroboration"],
                        has_verifiable_author=has_verifiable_author,
                        has_verifiable_institution=has_verifiable_institution,
                        has_stable_locator=has_stable_locator,
                        work_identity_resolved=work_identity_resolved,
                    ),
                    "confidence_band": confidence_band_for_work(
                        flags=flags,
                        has_external_corroboration=validation["has_external_corroboration"],
                        has_major_conflict=validation["has_major_conflict"],
                        locator_strength=locator_strength,
                        author_strength=author_strength,
                        institution_strength=institution_strength,
                    ),
                    "has_external_corroboration": bool(validation["has_external_corroboration"]),
                    "has_major_conflict": bool(validation["has_major_conflict"]),
                    "introduced_major_conflict": "major_conflict" in flags,
                    "introduced_weak_author_identity": "weak_author_identity" in flags,
                    "introduced_weak_institution_linkage": "weak_institution_linkage" in flags,
                    "introduced_doi_expected_missing": "doi_expected_missing" in flags,
                    "conflict_fields": list(validation.get("conflict_fields") or []),
                    "flags": sorted(flags),
                }
            )

    return pd.DataFrame(rows)


def run_reliability(
    run_id: str,
    *,
    processed_dir: Path = PROCESSED,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    records_by_source = _load_latest_records(processed_dir=processed_dir)
    matches_df = _load_latest_matches(processed_dir=processed_dir)
    mapping = canonical_ids_from_records(records_by_source, matches_df)

    try:
        connector = CrossrefConnector()
    except ValueError as exc:
        logger.warning("Crossref validation disabled: %s", exc)
        connector = None

    work_df = _build_work_rows(records_by_source, mapping, connector)
    source_record_df = build_source_record_reliability_table(work_df)
    canonical_df = build_canonical_work_summary(work_df)
    summary_df = build_source_reliability_summary(source_record_df, canonical_df)

    exporter = DatasetExporter(output_dir=output_dir or processed_dir)
    return exporter.export_reliability_outputs(source_record_df, canonical_df, summary_df, run_id=run_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate record reliability outputs.")
    parser.add_argument("--run-id", required=True, help="Run identifier used in output filenames.")
    parser.add_argument(
        "--processed-dir",
        default=str(PROCESSED),
        help="Directory containing records_*.parquet and matches_phase2_*.parquet inputs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        paths = run_reliability(
            args.run_id,
            processed_dir=Path(args.processed_dir),
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    for label, path in paths.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
