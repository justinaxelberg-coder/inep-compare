from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

STRATIFIED_SCHEMA: list[str] = [
    "source", "inst_type", "region", "sub_dimension",
    "value", "n_papers", "confidence_tier",
]
GEOGRAPHIC_COLUMNS: list[str] = [
    "source",
    "inst_type",
    "region",
    "n_records",
    "source_publication_share",
    "peer_mean_share",
    "comparative_skew",
    "cohort_institution_share",
    "cohort_phd_faculty_share",
    "delta_vs_cohort_institution_share",
    "delta_vs_cohort_phd_faculty_share",
    "cohort_institutions",
]
SOURCE_RELIABILITY_SUMMARY_COLUMNS: list[str] = [
    "source",
    "record_type",
    "canonical_works",
    "integration_ready_works",
    "reviewable_disputed_works",
    "not_integration_ready_works",
    "high_confidence_works",
    "medium_confidence_works",
    "low_confidence_works",
    "externally_corroborated_works",
    "major_conflict_works",
    "doi_expected_missing_works",
    "integration_ready_share",
    "reviewable_disputed_share",
    "not_integration_ready_share",
    "high_confidence_share",
    "medium_confidence_share",
    "low_confidence_share",
    "external_corroboration_share",
    "major_conflict_share",
    "doi_expected_missing_share",
]
SOURCE_RELIABILITY_FLAG_COLUMNS: list[str] = [
    "source",
    "record_type",
    "flag",
    "n_works",
    "denominator",
    "share",
]

FITNESS_COLUMNS: list[str] = [
    "source", "inst_type", "coverage", "data_quality", "reliability",
    "accessibility", "social_impact", "governance", "innovation_link", "composite",
]
CONVERGENCE_COLUMNS: list[str] = [
    "source_a", "source_b", "e_mec_code", "n_a", "n_b",
    "n_matched", "overlap_pct_a", "overlap_pct_b", "overlap_pct_min",
]
DIVERGENCE_COLUMNS: list[str] = [
    "e_mec_code", "institution_name", "source_a", "source_b",
    "count_a", "count_b", "discrepancy_pct", "direction",
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
                if table is None:
                    logger.warning(f"fitness.db found but contains no recognised table — falling back to CSV")
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
    divs    = _read_csvs(div_files,    DIVERGENCE_COLUMNS, "divergences")

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
    df = df.copy()
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


def load_geographic(csv_dir: Path | None = None) -> pd.DataFrame:
    """Return dedicated geographic comparison DataFrame."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("geographic_coverage_*.csv"))
    if not files:
        logger.warning("No geographic_coverage_*.csv found")
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)
    try:
        df = pd.read_csv(files[-1])
        if not set(GEOGRAPHIC_COLUMNS).issubset(df.columns):
            logger.warning("Geographic file %s uses unsupported legacy schema", files[-1].name)
            return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)
        logger.info("Loaded %d geographic rows from %s", len(df), files[-1].name)
        return _ensure_columns(df, GEOGRAPHIC_COLUMNS)
    except Exception as exc:
        logger.warning("Geographic load failed: %s", exc)
        return pd.DataFrame(columns=GEOGRAPHIC_COLUMNS)


def load_sdg(csv_dir: Path | None = None) -> pd.DataFrame:
    """Return SDG coverage DataFrame. Empty if enrichment not yet run.
    Columns: source, inst_type, sdg_goal, sdg_label, rate, n_tagged, n_total
    """
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("sdg_by_source_type_*.csv"))
    if not files:
        logger.info("No sdg_by_source_type_*.csv found — SDG tab will show empty state")
        return pd.DataFrame(columns=["source","inst_type","sdg_goal","sdg_label","rate","n_tagged","n_total"])
    try:
        df = pd.read_csv(files[-1])
        logger.info("Loaded %d SDG rows from %s", len(df), files[-1].name)
        return df
    except Exception as exc:
        logger.warning("SDG load failed: %s", exc)
        return pd.DataFrame(columns=["source","inst_type","sdg_goal","sdg_label","rate","n_tagged","n_total"])


def load_sensitivity(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load most recent sensitivity_*.csv. Returns empty DataFrame if absent."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("sensitivity_*.csv"))
    if not files:
        logger.warning("No sensitivity_*.csv found in %s", csv_dir)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.read_csv(files[-1])


def load_metadata_quality(csv_dir: Path | None = None) -> pd.DataFrame:
    """Concatenate disambiguation, funder, and policy_docs stratified CSVs."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    frames = []
    for pat in ["disambiguation_*.csv", "funder_*.csv", "policy_docs_*.csv"]:
        files = sorted(Path(csv_dir).glob(pat))
        if files:
            frames.append(pd.read_csv(files[-1]))
    if not frames:
        logger.warning("No metadata quality CSVs found in %s", csv_dir)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.concat(frames, ignore_index=True)


def load_sdg_stratified(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load most recent sdg_stratified_*.csv. Falls back to sdg_by_source_type_*.csv."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("sdg_stratified_*.csv"))
    if files:
        return pd.read_csv(files[-1])
    legacy = sorted(Path(csv_dir).glob("sdg_by_source_type_*.csv"))
    if legacy:
        logger.info("Using legacy SDG file — re-run enrichment to get stratified version")
        return pd.read_csv(legacy[-1])
    return pd.DataFrame(columns=STRATIFIED_SCHEMA)


def load_enrichment_combined(csv_dir: Path | None = None) -> pd.DataFrame:
    """Merge non-geographic enrichment sources into one stratified DataFrame."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    frames = []
    for loader in [load_sensitivity, load_metadata_quality, load_sdg_stratified]:
        df = loader(csv_dir)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    result = pd.concat(frames, ignore_index=True)
    return result[[c for c in STRATIFIED_SCHEMA if c in result.columns]]


def load_source_reliability_summary(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load the latest source reliability summary CSV, if present."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("source_reliability_summary_*.csv"))
    if not files:
        logger.warning("No source_reliability_summary_*.csv found")
        return pd.DataFrame(columns=SOURCE_RELIABILITY_SUMMARY_COLUMNS)
    try:
        df = pd.read_csv(files[-1])
        logger.info("Loaded %d source reliability summary rows from %s", len(df), files[-1].name)
        return _ensure_columns(df, SOURCE_RELIABILITY_SUMMARY_COLUMNS)
    except Exception as exc:
        logger.warning("Source reliability summary load failed: %s", exc)
        return pd.DataFrame(columns=SOURCE_RELIABILITY_SUMMARY_COLUMNS)


def load_source_reliability_flags(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load the latest source reliability flags CSV, if present."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("source_reliability_flags_*.csv"))
    if not files:
        logger.warning("No source_reliability_flags_*.csv found")
        return pd.DataFrame(columns=SOURCE_RELIABILITY_FLAG_COLUMNS)
    try:
        df = pd.read_csv(files[-1])
        logger.info("Loaded %d source reliability flag rows from %s", len(df), files[-1].name)
        return _ensure_columns(df, SOURCE_RELIABILITY_FLAG_COLUMNS)
    except Exception as exc:
        logger.warning("Source reliability flags load failed: %s", exc)
        return pd.DataFrame(columns=SOURCE_RELIABILITY_FLAG_COLUMNS)


def load_source_metadata(processed_dir: Path | None = None) -> dict:
    """Return source_metadata.json as dict. Empty dict if absent."""
    import json
    path = (Path(processed_dir) if processed_dir else _DEFAULT_PROCESSED) / "source_metadata.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.warning("source_metadata.json load failed: %s", exc)
        return {}
