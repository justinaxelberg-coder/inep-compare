# enrichment/sdg.py
from __future__ import annotations
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

SDG_LABELS = {
    1: "No Poverty", 2: "Zero Hunger", 3: "Good Health",
    4: "Quality Education", 5: "Gender Equality", 6: "Clean Water",
    7: "Clean Energy", 8: "Decent Work", 9: "Industry & Innovation",
    10: "Reduced Inequalities", 11: "Sustainable Cities",
    12: "Responsible Consumption", 13: "Climate Action",
    14: "Life Below Water", 15: "Life on Land",
    16: "Peace & Justice", 17: "Partnerships",
}


def compute_sdg_rates(papers: list[dict]) -> dict[int, dict]:
    """papers: list of dicts with 'sdgs': list[int]
    Returns: {goal_int: {'rate': float, 'n_tagged': int, 'n_total': int}}
    """
    n = len(papers)
    if n == 0:
        return {}
    counts: dict[int, int] = {}
    for paper in papers:
        for g in paper.get("sdgs") or []:
            counts[int(g)] = counts.get(int(g), 0) + 1
    return {
        g: {"rate": c / n, "n_tagged": c, "n_total": n}
        for g, c in counts.items()
    }


def compute_sdg_agreement(
    matched: list[dict],
    oa_sdg_map: dict[str, set[int]],
    dim_sdg_map: dict[str, set[int]],
) -> dict[int, dict]:
    """matched: list of {id_a: openalex_id, id_b: dimensions_id}
    Returns: {goal: {'agreement_rate': float, 'n_pairs': int}}
    """
    if not matched:
        return {}
    goal_agree: dict[int, list[bool]] = {}
    for pair in matched:
        a_sdgs = oa_sdg_map.get(pair["id_a"], set())
        b_sdgs = dim_sdg_map.get(pair["id_b"], set())
        all_goals = a_sdgs | b_sdgs
        for g in all_goals:
            goal_agree.setdefault(g, []).append(g in a_sdgs and g in b_sdgs)
    return {
        g: {"agreement_rate": sum(v) / len(v), "n_pairs": len(v)}
        for g, v in goal_agree.items()
    }


def write_sdg_flag(path: Path, source: str, available: bool) -> None:
    """Atomically update source_metadata.json with SDG availability flag."""
    path = Path(path)
    existing: dict = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except json.JSONDecodeError:
            logger.warning("Corrupt source_metadata.json — overwriting")
    existing.setdefault(source, {})["sdg_available"] = available
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, indent=2))
    os.replace(tmp, path)
    logger.info("source_metadata.json: %s sdg_available=%s", source, available)
