"""
Sprint 1 runner.

Produces the first real finding:
  "For a spotlight sample of Brazilian HEIs, what percentage of their
   OpenAlex output has verified OA status via Unpaywall, and how does
   this vary by institution type?"

Usage:
    python run_sprint1.py

Requires:
    - OPENALEX_EMAIL env var (free, for polite pool)
    - UNPAYWALL_EMAIL env var (free, mandatory)
    - INEP Microdados Censo CSV in data/raw/inep/ (optional — enriches registry)

Output:
    data/processed/coverage_<run_id>.csv
    data/processed/oa_<run_id>.csv
    data/processed/sprint1_report_<run_id>.md
    data/processed/inep_bibliometric.db
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.secrets_loader import load_secrets
load_secrets()

from connectors.api.openalex import OpenAlexConnector
from connectors.api.unpaywall import UnpaywallConnector
from outputs.dataset.exporter import DatasetExporter
from scoring.coverage import CoverageScorer
from scoring.open_access import OAScorer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sprint1")


def load_config() -> dict:
    with open("config/sample_config.yaml") as f:
        return yaml.safe_load(f)


def load_spotlight(config: dict) -> list[dict]:
    """Return spotlight institutions from config."""
    return config.get("spotlight", [])


def run():
    config = load_config()
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spotlight = load_spotlight(config)

    logger.info(f"Sprint 1 — run {run_id}")
    logger.info(f"Spotlight: {len(spotlight)} institutions")
    logger.info(
        f"Temporal window: {config['temporal_window']['start_year']}–"
        f"{config['temporal_window']['end_year']}"
    )

    # ------------------------------------------------------------------
    # Initialise connectors
    # ------------------------------------------------------------------
    openalex_email = os.environ.get("OPENALEX_EMAIL", "")
    unpaywall_email = os.environ.get("UNPAYWALL_EMAIL", "")

    if not unpaywall_email:
        logger.error(
            "UNPAYWALL_EMAIL not set. "
            "Run: export UNPAYWALL_EMAIL=your@email.com"
        )
        sys.exit(1)

    openalex = OpenAlexConnector(
        email=openalex_email,
        max_records=config.get("max_records_per_query", 500),
    )
    unpaywall = UnpaywallConnector(
        email=unpaywall_email,
        validation_sample_size=config.get("unpaywall_validation_sample", 100),
        max_workers=10,
    )

    # ------------------------------------------------------------------
    # Fetch & score
    # ------------------------------------------------------------------
    cov_scorer = CoverageScorer(source="openalex")
    oa_scorer = OAScorer(source="openalex")

    coverage_results = []
    oa_results = []
    all_records: dict[str, list[dict]] = {}

    # total_counts: actual corpus size from API (may exceed max_records ceiling)
    total_counts: dict[str, int | None] = {}

    for inst in spotlight:
        e_mec = inst["e_mec_code"]
        name = inst["name"]
        ror_id = inst.get("ror_id")

        logger.info(f"→ {name} ({e_mec})")

        # Fetch from OpenAlex
        records = openalex.query_institution(
            e_mec_code=e_mec,
            ror_id=ror_id,
            name=name,
            start_year=config["temporal_window"]["start_year"],
            end_year=config["temporal_window"]["end_year"],
        )
        total_counts[e_mec] = getattr(openalex, "last_total_count", None)
        all_records[e_mec] = records

        ceiling = config.get("max_records_per_query", 500)
        ceiling_hit = len(records) >= ceiling
        actual = total_counts[e_mec]
        if ceiling_hit and actual:
            logger.info(
                f"  OpenAlex: {len(records)} records fetched "
                f"(ceiling hit — actual corpus: {actual:,})"
            )
        else:
            logger.info(f"  OpenAlex: {len(records)} records")

        # Fetch OA status from Unpaywall for records with DOIs
        dois = [r["doi"] for r in records if r.get("doi")]
        logger.info(f"  Unpaywall: looking up {len(dois)} DOIs")
        uw_lookup = unpaywall.lookup_dois(dois) if dois else {}

        # Score coverage
        institution_dict = {
            "e_mec_code": e_mec,
            "name": name,
            "category": _infer_category(inst),
            "org_type": _infer_org_type(inst),
            "region": _lookup_region(inst),
        }
        cov_result = cov_scorer.score(records=records, institution=institution_dict)
        oa_result = oa_scorer.score(
            records=records,
            institution=institution_dict,
            unpaywall_lookup=uw_lookup,
        )

        # Attach total count and ceiling flag for reporting
        cov_result.total_count = actual
        cov_result.ceiling_hit = ceiling_hit
        oa_result.total_count = actual
        oa_result.ceiling_hit = ceiling_hit

        coverage_results.append(cov_result)
        oa_results.append(oa_result)

        # Log headline
        if oa_result.oa_rate and not oa_result.oa_rate.suppressed:
            logger.info(
                f"  OA rate: {oa_result.oa_rate.estimate:.1%} "
                f"[{oa_result.oa_rate.ci_low:.1%}, {oa_result.oa_rate.ci_high:.1%}] "
                f"({oa_result.oa_rate.tier.value})"
            )
        else:
            logger.info(f"  OA rate: insufficient data (N={len(records)})")

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    exporter = DatasetExporter(output_dir="data/processed")

    cov_path = exporter.export_coverage(coverage_results, run_id)
    oa_path = exporter.export_oa(oa_results, run_id)
    db_path = exporter.export_to_sqlite(coverage_results, oa_results, run_id)
    report_path = exporter.export_sprint1_report(
        coverage_results, oa_results, run_id,
        config={
            "start_year": config["temporal_window"]["start_year"],
            "end_year": config["temporal_window"]["end_year"],
            "max_records": config.get("max_records_per_query", 500),
        },
    )

    logger.info("")
    logger.info("Sprint 1 complete.")
    logger.info(f"  Coverage:  {cov_path}")
    logger.info(f"  OA:        {oa_path}")
    logger.info(f"  Database:  {db_path}")
    logger.info(f"  Report:    {report_path}")


# Region lookup for spotlight sample — populated from INEP Microdados when available
_SPOTLIGHT_REGIONS = {
    "572":  "Sudeste",   # UFABC
    "97":   "Sudeste",   # UNIFESP
    "283":  "Norte",     # UFPA
    "524":  "Sudeste",   # IFSP
    "1982": "Sudeste",   # PUC-Campinas
}

def _lookup_region(inst: dict) -> str | None:
    return _SPOTLIGHT_REGIONS.get(inst.get("e_mec_code", "").lstrip("0"))

def _infer_category(inst: dict) -> str | None:
    """Infer institution category from spotlight config notes."""
    notes = (inst.get("notes") or "").lower()
    name = inst.get("name", "").lower()
    if "instituto federal" in name or "if " in name:
        return "instituto_federal"
    if "universidade" in name:
        return "universidade"
    return None


def _infer_org_type(inst: dict) -> str | None:
    name = inst.get("name", "").lower()
    if "federal" in name:
        return "federal"
    if "estadual" in name:
        return "estadual"
    if "pontifícia" in name or "puc" in name:
        return "privada_sem_fins"
    return None


if __name__ == "__main__":
    run()
