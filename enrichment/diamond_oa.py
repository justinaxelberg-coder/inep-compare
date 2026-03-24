from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

_DIAMOND_PATTERNS = ["scielo.br", "redalyc.org", "doaj.org/article", "ojs"]


def classify_oa(oa_status: str | None, pdf_url: str | None) -> str:
    if not oa_status or oa_status == "closed":
        return "closed"
    if oa_status == "diamond":
        return "diamond"
    if pdf_url and any(p in str(pdf_url) for p in _DIAMOND_PATTERNS):
        return "diamond"
    if oa_status == "gold":
        return "gold"
    if oa_status in ("green", "hybrid"):
        return oa_status
    return "unknown"


def enrich_oa_file(path: Path) -> None:
    """Add oa_type column to OA CSV in-place. Idempotent."""
    path = Path(path)
    df = pd.read_csv(path)
    if "oa_type" in df.columns:
        logger.info("oa_type already present in %s — skipping", path.name)
        return
    df["oa_type"] = df.apply(
        lambda r: classify_oa(r.get("oa_status"), r.get("pdf_url")), axis=1
    )
    df.to_csv(path, index=False)
    logger.info("Enriched %s with oa_type (%d rows)", path.name, len(df))
