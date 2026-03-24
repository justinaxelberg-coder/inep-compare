"""
Phase 2 runner.

Fetches records from all automated API sources for the spotlight sample,
scores coverage and OA per source, runs the convergence engine, and
exports the full multi-source dataset.

Sources: OpenAlex · Scopus · WoS · Dimensions + Unpaywall validation

Usage:
    python run_phase2.py

Required env vars:
    OPENALEX_EMAIL       free, for polite pool
    UNPAYWALL_EMAIL      free, mandatory
    SCOPUS_API_KEY       institutional
    WOS_API_KEY          institutional
    DIMENSIONS_API_KEY   or DIMENSIONS_USERNAME + DIMENSIONS_PASSWORD

Output:
    data/processed/phase2_coverage_<run_id>.csv
    data/processed/phase2_oa_<run_id>.csv
    data/processed/phase2_overlap_<run_id>.csv
    data/processed/phase2_divergences_<run_id>.csv
    data/processed/phase2_review_queue_<run_id>.csv
    data/processed/inep_bibliometric.db
    data/processed/phase2_report_<run_id>.md
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from config.secrets_loader import load_secrets
load_secrets()

from connectors.api.openalex import OpenAlexConnector
from connectors.api.scopus import ScopusConnector
from connectors.api.wos import WoSConnector
from connectors.api.dimensions import DimensionsConnector
from connectors.api.unpaywall import UnpaywallConnector
from convergence.matcher import ConvergenceEngine
from outputs.dataset.exporter import DatasetExporter
from scoring.coverage import CoverageScorer
from scoring.open_access import OAScorer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("phase2")


def load_config() -> dict:
    with open("config/sample_config.yaml") as f:
        return yaml.safe_load(f)


def init_connectors(config: dict) -> dict:
    """Initialise all available connectors. Skip any missing credentials."""
    connectors = {}
    max_r = config.get("max_records_per_query", 500)

    # OpenAlex — always available
    connectors["openalex"] = OpenAlexConnector(
        email=os.environ.get("OPENALEX_EMAIL", ""),
        max_records=max_r,
    )

    # Scopus
    if os.environ.get("SCOPUS_API_KEY"):
        connectors["scopus"] = ScopusConnector(
            api_key=os.environ["SCOPUS_API_KEY"],
            max_records=max_r,
        )
    else:
        logger.warning("SCOPUS_API_KEY not set — Scopus skipped")

    # WoS
    if os.environ.get("WOS_API_KEY"):
        connectors["wos"] = WoSConnector(
            api_key=os.environ["WOS_API_KEY"],
            max_records=max_r,
        )
    else:
        logger.warning("WOS_API_KEY not set — WoS skipped")

    # Dimensions
    if os.environ.get("DIMENSIONS_API_KEY"):
        connectors["dimensions"] = DimensionsConnector(
            api_key=os.environ["DIMENSIONS_API_KEY"],
            max_records=max_r,
        )
    elif os.environ.get("DIMENSIONS_USERNAME") and os.environ.get("DIMENSIONS_PASSWORD"):
        connectors["dimensions"] = DimensionsConnector(
            username=os.environ["DIMENSIONS_USERNAME"],
            password=os.environ["DIMENSIONS_PASSWORD"],
            max_records=max_r,
        )
    else:
        logger.warning("DIMENSIONS_API_KEY not set — Dimensions skipped")

    return connectors


def build_institution_dict(inst: dict) -> dict:
    return {
        "e_mec_code": inst["e_mec_code"],
        "name": inst["name"],
        "category": _infer_category(inst),
        "org_type": _infer_org_type(inst),
        "region": _SPOTLIGHT_REGIONS.get(inst["e_mec_code"].lstrip("0")),
    }


def run():
    config = load_config()
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spotlight = config.get("spotlight", [])
    start_year = config["temporal_window"]["start_year"]
    end_year = config["temporal_window"]["end_year"]
    max_r = config.get("max_records_per_query", 500)

    unpaywall_email = os.environ.get("UNPAYWALL_EMAIL", "")
    if not unpaywall_email:
        logger.error("UNPAYWALL_EMAIL not set.")
        sys.exit(1)

    logger.info(f"Phase 2 — run {run_id}")
    logger.info(f"Spotlight: {len(spotlight)} institutions")
    logger.info(f"Temporal window: {start_year}–{end_year}")

    connectors = init_connectors(config)
    logger.info(f"Active sources: {list(connectors.keys())}")

    unpaywall = UnpaywallConnector(
        email=unpaywall_email,
        validation_sample_size=config.get("unpaywall_validation_sample", 100),
        max_workers=10,
    )

    # ------------------------------------------------------------------
    # Per-institution, per-source fetch + score
    # ------------------------------------------------------------------

    # records_by_source[source][e_mec_code] = [records]
    records_by_source: dict[str, dict[str, list[dict]]] = {s: {} for s in connectors}
    coverage_results: list = []
    oa_results: list = []

    for inst in spotlight:
        e_mec = inst["e_mec_code"]
        name = inst["name"]
        ror_id = inst.get("ror_id")
        institution_dict = build_institution_dict(inst)

        logger.info(f"\n{'─'*60}")
        logger.info(f"→ {name} ({e_mec})")

        for source_id, connector in connectors.items():
            logger.info(f"  [{source_id}] fetching...")
            try:
                records = connector.query_institution(
                    e_mec_code=e_mec,
                    ror_id=ror_id,
                    name=name,
                    start_year=start_year,
                    end_year=end_year,
                )
            except Exception as exc:
                logger.error(f"  [{source_id}] FAILED: {exc}")
                records = []

            total = getattr(connector, "last_total_count", None)
            ceiling_hit = len(records) >= max_r
            records_by_source[source_id][e_mec] = records

            if ceiling_hit and total:
                logger.info(f"  [{source_id}] {len(records)} fetched ⚠ ceiling (actual: {total:,})")
            else:
                logger.info(f"  [{source_id}] {len(records)} records")

            # OA validation via Unpaywall
            dois = [r["doi"] for r in records if r.get("doi")]
            uw_lookup = unpaywall.lookup_dois(dois) if dois else {}

            # Score
            cov = CoverageScorer(source=source_id)
            oa = OAScorer(source=source_id)

            cov_result = cov.score(records=records, institution=institution_dict)
            oa_result = oa.score(records=records, institution=institution_dict,
                                 unpaywall_lookup=uw_lookup)

            cov_result.total_count = total
            cov_result.ceiling_hit = ceiling_hit
            oa_result.total_count = total
            oa_result.ceiling_hit = ceiling_hit

            coverage_results.append(cov_result)
            oa_results.append(oa_result)

            if oa_result.oa_rate and not oa_result.oa_rate.suppressed:
                logger.info(
                    f"  [{source_id}] OA: {oa_result.oa_rate.estimate:.1%} "
                    f"[{oa_result.oa_rate.ci_low:.1%}, {oa_result.oa_rate.ci_high:.1%}] "
                    f"({oa_result.oa_rate.tier.value})"
                )

    # ------------------------------------------------------------------
    # Convergence engine
    # ------------------------------------------------------------------
    logger.info(f"\n{'─'*60}")
    logger.info("Running convergence engine...")

    engine = ConvergenceEngine(source_ids=list(connectors.keys()))
    convergence = engine.run(records_by_source)
    convergence["ceiling"] = max_r   # pass ceiling to exporter for ⚠ flagging

    logger.info(f"  Overlap matrix computed for {len(connectors)} sources")
    logger.info(f"  Divergence flags: {len(convergence['divergences'])}")
    logger.info(f"  Review queue items: {len(convergence['review_queue'])}")

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    logger.info(f"\n{'─'*60}")
    exporter = DatasetExporter(output_dir="data/processed")

    cov_path  = exporter.export_coverage(coverage_results, f"phase2_{run_id}")
    oa_path   = exporter.export_oa(oa_results, f"phase2_{run_id}")
    db_path   = exporter.export_to_sqlite(coverage_results, oa_results, f"phase2_{run_id}")
    conv_paths = exporter.export_convergence(convergence, f"phase2_{run_id}")
    report_path = exporter.export_phase2_report(
        coverage_results, oa_results, convergence,
        run_id=f"phase2_{run_id}",
        config={
            "start_year": start_year,
            "end_year": end_year,
            "max_records": max_r,
            "sources": list(connectors.keys()),
        },
    )

    logger.info("\nPhase 2 complete.")
    logger.info(f"  Coverage:    {cov_path}")
    logger.info(f"  OA:          {oa_path}")
    logger.info(f"  Database:    {db_path}")
    logger.info(f"  Overlap:     {conv_paths.get('overlap')}")
    logger.info(f"  Divergences: {conv_paths.get('divergences')}")
    logger.info(f"  Queue:       {conv_paths.get('review_queue')}")
    logger.info(f"  Report:      {report_path}")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_SPOTLIGHT_REGIONS = {
    "572":  "Sudeste",
    "97":   "Sudeste",
    "283":  "Norte",
    "524":  "Sudeste",
    "1982": "Sudeste",
}

def _infer_category(inst: dict) -> str | None:
    name = inst.get("name", "").lower()
    if "instituto federal" in name:
        return "instituto_federal"
    if "universidade" in name:
        return "universidade"
    return None

def _infer_org_type(inst: dict) -> str | None:
    name = inst.get("name", "").lower()
    if "federal" in name and "pontifícia" not in name:
        return "federal"
    if "pontifícia" in name or "puc" in name:
        return "privada_sem_fins"
    return None


if __name__ == "__main__":
    run()
