"""
INEP Microdados runner.

Downloads and parses the Censo da Educação Superior, enriches the spotlight
crosswalk with institution denominators (PhD share, enrolment, category),
and saves the full registry for use by other runners.

Usage:
    python run_microdados.py [--year 2023] [--force-download] [--skip-download]
"""

from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")
from connectors.file.inep_microdados import INEPMicrodadosConnector

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REGISTRY_OUT  = Path("registry/institutions.csv")
CROSSWALK_IN  = Path("registry/crosswalk_template.csv")
CROSSWALK_OUT = Path("registry/crosswalk_enriched.csv")
DATA_DIR      = Path("data/raw/inep")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and parse INEP Censo IES")
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    connector = INEPMicrodadosConnector(data_dir=str(DATA_DIR))

    # Download
    if not args.skip_download:
        try:
            path = connector.download(year=args.year, force=args.force_download)
            logger.info(f"Censo file: {path}")
        except RuntimeError as e:
            logger.error(str(e))
            logger.info("Re-run with --skip-download if file is already in data/raw/inep/")
            sys.exit(1)

    # Parse
    try:
        registry = connector.load(year=args.year)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    summary = connector.summary(registry)
    logger.info(f"Registry summary: {summary}")

    # Save full registry
    REGISTRY_OUT.parent.mkdir(parents=True, exist_ok=True)
    registry.to_csv(REGISTRY_OUT, index=False, encoding="utf-8")
    logger.info(f"Full registry saved: {REGISTRY_OUT} ({len(registry):,} rows)")

    # Enrich spotlight crosswalk
    if CROSSWALK_IN.exists():
        crosswalk = pd.read_csv(CROSSWALK_IN, dtype={"e_mec_code": str})
        crosswalk["e_mec_code_padded"] = crosswalk["e_mec_code"].str.zfill(6)

        # Select columns that are guaranteed to exist; optional ones may be
        # absent depending on Censo year (column names vary across releases).
        _want = ["e_mec_code", "sinaes_type", "faculty_with_phd",
                 "region", "state", "city"]
        denoms = registry[[c for c in _want if c in registry.columns]].copy()

        enriched = crosswalk.merge(
            denoms, left_on="e_mec_code_padded", right_on="e_mec_code",
            how="left", suffixes=("", "_inep")
        ).drop(columns=["e_mec_code_padded", "e_mec_code_inep"], errors="ignore")

        unmatched = enriched[enriched["sinaes_type"].isna()]["e_mec_code"].tolist()
        if unmatched:
            logger.warning(f"Spotlight institutions not found in Censo: {unmatched}")

        enriched.to_csv(CROSSWALK_OUT, index=False, encoding="utf-8")
        logger.info(f"Enriched crosswalk saved: {CROSSWALK_OUT}")
        _show = [c for c in ['e_mec_code','name','sinaes_type','faculty_with_phd'] if c in enriched.columns]
        logger.info(f"\n{enriched[_show].to_string()}")
    else:
        logger.warning(f"Crosswalk not found: {CROSSWALK_IN}")

    logger.info("─" * 60)
    logger.info(f"INEP Microdados integration complete — year {args.year}")
    logger.info("─" * 60)


if __name__ == "__main__":
    main()
