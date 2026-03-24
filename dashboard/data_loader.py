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
