"""
Fitness scoring runner.

Reads:
  data/processed/coverage_*.csv      — from run_sprint1.py or run_phase2.py
  data/processed/oa_*.csv            — from run_sprint1.py or run_phase2.py
  data/processed/overlap_phase2_*.csv — from run_phase2.py (convergence)

Writes:
  data/processed/fitness_matrix_<run_id>.csv
  data/processed/fitness_<run_id>.db
  data/processed/fitness_report_<run_id>.md
"""

from __future__ import annotations
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")
from config.secrets_loader import load_secrets
load_secrets()

from scoring.fitness import FitnessScorer
from outputs.dataset.exporter import DatasetExporter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROCESSED = Path("data/processed")

# Institution type inference from institution name heuristics
# (refined by INEP Microdados once integrated)
_NAME_TO_TYPE: dict[str, str] = {
    "instituto federal": "federal_institute",
    "universidade federal": "federal_university",
    "universidade estadual": "state_university",
    "pontifícia": "private_university",
    "puc": "private_university",
}

def _infer_inst_type(name: str) -> str:
    name_lower = name.lower()
    # Check longer/more specific patterns first
    for kw, t in sorted(_NAME_TO_TYPE.items(), key=lambda x: -len(x[0])):
        if kw in name_lower:
            return t
    return "other"


def _load_coverage(pattern: str) -> dict[str, dict[str, dict]]:
    """Return {source: {inst_type: coverage_dict}} with correct mean aggregation."""
    files = sorted(PROCESSED.glob(pattern))
    if not files:
        logger.warning(f"No coverage files matching {pattern}")
        return {}
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    numeric_fields = ("institutional_coverage", "field_coverage",
                      "language_coverage", "doi_coverage_rate")
    sums:      dict[tuple, dict] = {}
    counts:    dict[tuple, dict] = {}
    rec_counts: dict[tuple, int] = {}
    for _, row in df.iterrows():
        src       = str(row.get("source", "unknown"))
        inst_name = str(row.get("institution_name", ""))
        inst_type = _infer_inst_type(inst_name)
        key = (src, inst_type)
        sums.setdefault(key, {f: 0.0 for f in numeric_fields})
        counts.setdefault(key, {f: 0 for f in numeric_fields})
        rec_counts[key] = rec_counts.get(key, 0) + int(row.get("n_records", 0) or 0)
        for field in numeric_fields:
            if field in row and pd.notna(row[field]):
                sums[key][field] += float(row[field])
                counts[key][field] += 1
    result: dict[str, dict[str, dict]] = {}
    for (src, inst_type), s in sums.items():
        c = counts[(src, inst_type)]
        result.setdefault(src, {})[inst_type] = {
            "institutional_coverage": s["institutional_coverage"] / max(c["institutional_coverage"], 1),
            "field_coverage":         s["field_coverage"]         / max(c["field_coverage"], 1),
            "temporal_coverage":      1.0,
            "language_coverage":      s["language_coverage"]      / max(c["language_coverage"], 1),
            "doi_coverage_rate":      s["doi_coverage_rate"]      / max(c["doi_coverage_rate"], 1),
            "record_count":           rec_counts[(src, inst_type)],
        }
    return result


def _load_oa(pattern: str) -> dict[str, dict[str, dict]]:
    """Return {source: {inst_type: oa_dict}} with correct mean aggregation."""
    files = sorted(PROCESSED.glob(pattern))
    if not files:
        return {}
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    oa_fields = ("oa_rate", "diamond_rate", "unpaywall_agreement")
    sums:   dict[tuple, dict] = {}
    counts: dict[tuple, dict] = {}
    for _, row in df.iterrows():
        src       = str(row.get("source", "unknown"))
        inst_name = str(row.get("institution_name", ""))
        inst_type = _infer_inst_type(inst_name)
        key = (src, inst_type)
        sums.setdefault(key, {f: 0.0 for f in oa_fields})
        counts.setdefault(key, {f: 0 for f in oa_fields})
        for field in oa_fields:
            if field in row and pd.notna(row[field]):
                sums[key][field] += float(row[field])
                counts[key][field] += 1
    result: dict[str, dict[str, dict]] = {}
    for (src, inst_type), s in sums.items():
        c = counts[(src, inst_type)]
        result.setdefault(src, {})[inst_type] = {
            f: s[f] / max(c[f], 1) for f in oa_fields
        }
    return result


def _load_convergence(pattern: str) -> dict:
    """Return {(src_a, src_b): {overlap_pct, divergence_pct}} from overlap CSV."""
    files = sorted(PROCESSED.glob(pattern))
    if not files:
        return {}
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    result: dict[tuple, dict] = {}
    for _, row in df.iterrows():
        key = (str(row.get("source_a", "")), str(row.get("source_b", "")))
        pct = float(row.get("overlap_pct_min", 0) or 0)
        result[key] = {"overlap_pct": pct, "divergence_pct": 0.0}
    return result


def main() -> None:
    run_id   = str(date.today())
    exporter = DatasetExporter(output_dir="data/processed")
    scorer   = FitnessScorer()

    logger.info("Loading coverage data...")
    coverage = _load_coverage("coverage_*.csv")
    if not coverage:
        logger.error("No coverage data — run run_sprint1.py or run_phase2.py first")
        sys.exit(1)

    logger.info("Loading OA data...")
    oa = _load_oa("oa_*.csv")

    logger.info("Loading convergence data...")
    convergence = _load_convergence("overlap_phase2_*.csv")

    logger.info(f"Building fitness matrix: {len(coverage)} sources")
    matrix = scorer.build_matrix(coverage, oa, convergence)
    logger.info(f"  {len(matrix.rows)} source × institution-type profiles")

    csv_path    = exporter.export_fitness_matrix(matrix, run_id)
    report_path = exporter.export_fitness_report(matrix, run_id)

    logger.info("\n" + "─" * 60)
    logger.info(f"Fitness matrix : {csv_path}")
    logger.info(f"Fitness report : {report_path}")
    logger.info("─" * 60)


if __name__ == "__main__":
    main()
