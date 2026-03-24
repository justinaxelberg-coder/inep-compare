# enrichment/stratified.py
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

STRATIFIED_SCHEMA: list[str] = [
    "source", "inst_type", "region", "sub_dimension",
    "value", "n_papers", "confidence_tier",
]

_TIERS = [(200, "reliable"), (50, "moderate"), (10, "low"), (0, "insufficient")]


def assign_confidence_tier(n: int) -> str:
    for threshold, tier in _TIERS:
        if n >= threshold:
            return tier
    return "insufficient"


def make_stratum_row(
    source: str, inst_type: str, region: str,
    sub_dimension: str, value: float, n_papers: int,
) -> dict:
    return {
        "source": source,
        "inst_type": inst_type,
        "region": region,
        "sub_dimension": sub_dimension,
        "value": float(value),
        "n_papers": int(n_papers),
        "confidence_tier": assign_confidence_tier(n_papers),
    }


def write_stratified_csv(rows: list[dict], path: Path) -> None:
    """Write stratified rows to CSV, overwriting if exists."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=STRATIFIED_SCHEMA)
    df.to_csv(path, index=False)
    logger.info("Wrote %d stratified rows to %s", len(df), path.name)


def load_stratified_csv(path: Path) -> pd.DataFrame:
    """Load a stratified CSV; returns empty DataFrame if file absent."""
    path = Path(path)
    if not path.exists():
        logger.warning("Stratified CSV not found: %s", path)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.read_csv(path)
