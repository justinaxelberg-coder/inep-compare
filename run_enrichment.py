# run_enrichment.py
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

from config.secrets_loader import load_secrets
load_secrets()

from enrichment.coauthorship import compute_coauth_metrics
from enrichment.diamond_oa import enrich_oa_file
from enrichment.geographic import load_and_compute as geo_compute
from enrichment.sdg import SDG_LABELS, compute_sdg_rates, write_sdg_flag
from enrichment.sensitivity import compute_sensitivity, aggregate_by_stratum, build_sensitivity_rows
from enrichment.disambiguation import compute_disambiguation_rate, build_disambiguation_rows
from enrichment.funder import compute_funder_rates, build_funder_rows
from enrichment.policy_docs import compute_policy_rates, build_policy_rows
from enrichment.coauthorship import compute_coauth_stratified
from enrichment.sdg import compute_sdg_stratified
from enrichment.patents import compute_patent_link_rate, build_patent_rows
from enrichment.stratified import write_stratified_csv

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)

_PROCESSED = Path("data/processed")
_REGISTRY  = Path("registry/institutions.csv")
_METADATA  = _PROCESSED / "source_metadata.json"


def _load_crosswalk() -> pd.DataFrame:
    path = Path("registry/crosswalk_enriched.csv")
    if not path.exists():
        logger.warning("crosswalk_enriched.csv not found — stratified enrichment will be empty")
        return pd.DataFrame(columns=["e_mec_code", "inst_type", "region"])
    df = pd.read_csv(path)
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    if "sinaes_type" in df.columns and "inst_type" not in df.columns:
        df = df.rename(columns={"sinaes_type": "inst_type"})
    return df[["e_mec_code", "inst_type", "region"]].dropna()


def _to_papers_df(papers: list[dict]) -> pd.DataFrame:
    if not papers:
        return pd.DataFrame()
    df = pd.DataFrame(papers)
    for col, default in [
        ("ror_resolved", False),
        ("funding", None),
        ("patent_citations", None),
        ("document_type", None),
        ("sdgs", None),
        ("affiliation_types", None),
        ("e_mec_code", ""),
        ("source", ""),
    ]:
        if col not in df.columns:
            df[col] = default
    return df


def _resolve_date(date_str: str | None, processed: Path) -> str:
    if date_str:
        return date_str
    files = sorted(processed.glob("coverage_phase2_*.csv"), key=lambda f: f.stat().st_mtime)
    if not files:
        logger.error("No phase 2 outputs found. Run run_phase2.py first.")
        sys.exit(1)
    return files[-1].stem.split("phase2_")[-1]


def _pub_counts_from_coverage(coverage_csv: Path, source: str) -> dict[str, int]:
    """Return {e_mec_code: n_records} for the given source from coverage CSV."""
    df = pd.read_csv(coverage_csv)
    subset = df[df["source"] == source][["e_mec_code", "n_records"]].dropna()
    return dict(zip(subset["e_mec_code"].astype(str), subset["n_records"].astype(int)))


def _fetch_papers_for_enrichment(source: str, e_mec_codes: list[str],
                                  config: dict) -> list[dict]:
    """Fetch normalised paper records for enrichment API calls.
    Returns [] if connector unavailable or API error.
    """
    import os
    spotlight = config.get("spotlight", [])
    papers: list[dict] = []

    start_year = config.get("temporal_window", {}).get("start_year", 2023)
    end_year   = config.get("temporal_window", {}).get("end_year",   2023)

    if source == "openalex":
        from connectors.api.openalex import OpenAlexConnector
        conn = OpenAlexConnector(email=os.getenv("OPENALEX_EMAIL"), max_records=None)
        for inst in spotlight:
            if str(inst.get("e_mec_code")) not in e_mec_codes:
                continue
            ror = inst.get("ror_id", "")
            if not ror:
                continue
            try:
                records = conn.query_institution(
                    e_mec_code=str(inst["e_mec_code"]),
                    ror_id=ror,
                    name=inst.get("name", ""),
                    start_year=start_year,
                    end_year=end_year,
                )
                papers.extend(records)  # query_institution already returns normalised records
            except Exception as exc:
                logger.warning("OpenAlex fetch failed for %s: %s", ror, exc)

    elif source == "dimensions":
        if not (os.getenv("DIMENSIONS_API_KEY") or
                (os.getenv("DIMENSIONS_USERNAME") and os.getenv("DIMENSIONS_PASSWORD"))):
            logger.warning("Dimensions credentials not set — skipping Dimensions enrichment")
            return papers
        from connectors.api.dimensions import DimensionsConnector
        dim_kwargs = {"max_records": None}
        if os.getenv("DIMENSIONS_API_KEY"):
            dim_kwargs["api_key"] = os.environ["DIMENSIONS_API_KEY"]
        else:
            dim_kwargs["username"] = os.environ["DIMENSIONS_USERNAME"]
            dim_kwargs["password"] = os.environ["DIMENSIONS_PASSWORD"]
        conn = DimensionsConnector(**dim_kwargs)
        for inst in spotlight:
            if str(inst.get("e_mec_code")) not in e_mec_codes:
                continue
            ror = inst.get("ror_id", "")
            if not ror:
                continue
            try:
                records = conn.query_institution(
                    e_mec_code=str(inst["e_mec_code"]),
                    ror_id=ror,
                    name=inst.get("name", ""),
                    start_year=start_year,
                    end_year=end_year,
                )
                papers.extend(records)  # query_institution already returns normalised records
            except Exception as exc:
                logger.warning("Dimensions fetch failed for %s: %s", ror, exc)

    return papers


def main() -> None:
    parser = argparse.ArgumentParser(description="Run enrichment pass on phase 2 outputs")
    parser.add_argument("--phase2-date", help="Date suffix YYYY-MM-DD of phase 2 run")
    parser.add_argument("--skip-diamond",  action="store_true")
    parser.add_argument("--skip-geo",      action="store_true")
    parser.add_argument("--skip-coauth",   action="store_true")
    parser.add_argument("--skip-sdg",      action="store_true")
    args = parser.parse_args()

    with open("config/sample_config.yaml") as f:
        config = yaml.safe_load(f)

    date = _resolve_date(args.phase2_date, _PROCESSED)
    logger.info("Enriching phase 2 outputs for date: %s", date)

    coverage_files = sorted(_PROCESSED.glob(f"coverage_phase2_{date}*.csv"))
    coverage_csv = coverage_files[-1] if coverage_files else None

    # 1. Diamond OA (in-place, no API)
    if not args.skip_diamond:
        oa_files = sorted(_PROCESSED.glob(f"oa_phase2_{date}*.csv"))
        if not oa_files:
            logger.warning("No OA file for date %s — skipping diamond OA", date)
        else:
            enrich_oa_file(oa_files[-1])

    # 2. Geographic bias (no API — uses coverage CSV for pub counts)
    if not args.skip_geo and coverage_csv:
        sources = pd.read_csv(coverage_csv)["source"].unique().tolist()
        geo_rows = []
        for source in sources:
            pub_counts = _pub_counts_from_coverage(coverage_csv, source)
            result = geo_compute(str(_REGISTRY), pub_counts, source)
            if result:
                logger.info("Geographic bias %s: score=%.3f", source,
                            result["geographic_bias_score"])
                for region, gap in result["coverage_gaps"].items():
                    geo_rows.append({
                        "source": source, "region": region,
                        "coverage_gap": gap,
                        "output_gap": result["output_gaps"].get(region),
                        "geographic_bias_score": result["geographic_bias_score"],
                    })
        if geo_rows:
            out = _PROCESSED / f"geographic_coverage_{date}.csv"
            pd.DataFrame(geo_rows).to_csv(out, index=False)
            logger.info("Wrote %s", out)

    # 3. Coauthorship (OpenAlex API re-query with affiliation_types)
    if not args.skip_coauth:
        if not coverage_csv:
            logger.warning("No coverage CSV found — skipping coauthorship")
        else:
            e_mec_codes = pd.read_csv(coverage_csv)["e_mec_code"].astype(str).unique().tolist()
            papers = _fetch_papers_for_enrichment("openalex", e_mec_codes, config)
            if papers:
                metrics = compute_coauth_metrics(papers)
                logger.info("Coauthorship metrics: %s", metrics)
                out = _PROCESSED / f"nonacademic_coauth_{date}.csv"
                pd.DataFrame([{"source": "openalex", "inst_type": "all", **metrics}]
                             ).to_csv(out, index=False)
                logger.info("Wrote %s", out)
            else:
                logger.warning("No papers fetched for coauthorship — check API key/connectivity")

    # 4. SDG (OpenAlex + Dimensions; Scopus flagged absent)
    if not args.skip_sdg:
        if not coverage_csv:
            logger.warning("No coverage CSV found — skipping SDG")
        else:
            e_mec_codes = pd.read_csv(coverage_csv)["e_mec_code"].astype(str).unique().tolist()
            sdg_rows = []
            for source in ["openalex", "dimensions"]:
                papers = _fetch_papers_for_enrichment(source, e_mec_codes, config)
                if papers:
                    rates = compute_sdg_rates(papers)
                    for goal, data in rates.items():
                        sdg_rows.append({
                            "source": source, "inst_type": "all",
                            "sdg_goal": goal,
                            "sdg_label": SDG_LABELS.get(goal, f"SDG {goal}"),
                            **data,
                        })
            write_sdg_flag(_METADATA, "scopus", available=False)
            if sdg_rows:
                out = _PROCESSED / f"sdg_by_source_type_{date}.csv"
                pd.DataFrame(sdg_rows).to_csv(out, index=False)
                logger.info("Wrote %s", out)

    # 5. Stratified enrichment (requires crosswalk + fetched papers)
    xw = _load_crosswalk()
    if xw.empty:
        logger.warning("Crosswalk empty — skipping stratified enrichment")
    else:
        # Sensitivity (no API — uses coverage CSVs already on disk)
        try:
            if coverage_csv:
                cov_df = pd.read_csv(coverage_csv)
                sens_df = compute_sensitivity(cov_df, xw)
                agg = aggregate_by_stratum(sens_df)
                rows = build_sensitivity_rows(agg)
                if rows:
                    write_stratified_csv(rows, _PROCESSED / f"sensitivity_{date}.csv")
        except Exception as exc:
            logger.warning("Sensitivity enrichment failed: %s", exc)

        # Stratified modules that need fetched papers (OpenAlex)
        if not args.skip_coauth:
            e_mec_codes = list(xw["e_mec_code"].astype(str).unique())
            papers = _fetch_papers_for_enrichment("openalex", e_mec_codes, config)
            papers_df = _to_papers_df(papers)
            if not papers_df.empty:
                for fn, out_name in [
                    (lambda df: build_disambiguation_rows(compute_disambiguation_rate(df, xw)),
                     f"disambiguation_{date}.csv"),
                    (lambda df: build_funder_rows(compute_funder_rates(df, xw)),
                     f"funder_{date}.csv"),
                    (lambda df: build_policy_rows(compute_policy_rates(df, xw)),
                     f"policy_docs_{date}.csv"),
                    (lambda df: compute_coauth_stratified(df, xw),
                     f"nonacademic_coauth_{date}.csv"),
                    (lambda df: compute_sdg_stratified(df, xw),
                     f"sdg_stratified_{date}.csv"),
                    (lambda df: build_patent_rows(compute_patent_link_rate(df, xw)),
                     f"patents_{date}.csv"),
                ]:
                    try:
                        rows = fn(papers_df)
                        if rows:
                            write_stratified_csv(rows, _PROCESSED / out_name)
                    except Exception as exc:
                        logger.warning("Stratified enrichment failed for %s: %s", out_name, exc)

    logger.info("Enrichment complete.")


if __name__ == "__main__":
    main()
