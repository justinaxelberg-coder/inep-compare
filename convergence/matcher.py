"""
Convergence engine.

Three-level record matching across sources:
  1. DOI match (exact)                         → confidence 1.0
  2. title + year + first-author family name   → confidence 0.85
  3. fuzzy title (RapidFuzz ≥ 90%)            → confidence 0.70, flagged

Produces:
  - match_table:    every matched record pair with confidence + keys used
  - overlap_matrix: n×n source pair overlap percentages per institution
  - divergences:    institution × source pairs where aggregate counts
                    differ by > threshold (default 15%)
  - review_queue:   low-confidence matches for bolsista review
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from itertools import combinations

from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

DIVERGENCE_THRESHOLD = 0.15   # 15% count discrepancy triggers a flag
FUZZY_THRESHOLD = 90.0        # minimum RapidFuzz score for title match


# ------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------

@dataclass
class MatchRecord:
    source_a: str
    source_b: str
    record_id_a: str
    record_id_b: str
    doi_a: str | None
    doi_b: str | None
    title_a: str | None
    title_b: str | None
    year_a: int | None
    year_b: int | None
    e_mec_code: str
    match_key: str          # doi | title_year_author | fuzzy_title
    confidence: float
    flagged: bool = False   # True for low-confidence matches → review queue


@dataclass
class DivergenceFlag:
    e_mec_code: str
    institution_name: str
    source_a: str
    source_b: str
    count_a: int
    count_b: int
    discrepancy_pct: float
    direction: str          # "a_higher" | "b_higher"


@dataclass
class ConvergenceResult:
    match_table: list[MatchRecord] = field(default_factory=list)
    overlap_matrix: dict = field(default_factory=dict)  # {(src_a, src_b, e_mec): overlap_pct}
    divergences: list[DivergenceFlag] = field(default_factory=list)
    review_queue: list[MatchRecord] = field(default_factory=list)


# ------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------

class ConvergenceEngine:
    """
    Multi-source record matching engine.

    Usage:
        engine = ConvergenceEngine(source_ids=["openalex", "scopus", "wos"])
        result = engine.run(records_by_source)
        # result is a dict with keys: match_table, overlap_matrix,
        #                              divergences, review_queue
    """

    def __init__(
        self,
        source_ids: list[str],
        divergence_threshold: float = DIVERGENCE_THRESHOLD,
        fuzzy_threshold: float = FUZZY_THRESHOLD,
    ):
        self.source_ids = source_ids
        self.divergence_threshold = divergence_threshold
        self.fuzzy_threshold = fuzzy_threshold

    def run(self, records_by_source: dict[str, dict[str, list[dict]]]) -> dict:
        """
        Run full convergence analysis.

        Args:
            records_by_source: {source_id: {e_mec_code: [records]}}

        Returns dict with:
            match_table, overlap_matrix, divergences, review_queue
        """
        all_matches: list[MatchRecord] = []
        overlap_matrix: dict = {}
        divergences: list[DivergenceFlag] = []

        # Collect all institution codes across all sources
        all_e_mec = set()
        for src_records in records_by_source.values():
            all_e_mec.update(src_records.keys())

        for e_mec in sorted(all_e_mec):
            # Resolve institution name from any available record
            inst_name = e_mec
            for src_records in records_by_source.values():
                recs = src_records.get(e_mec, [])
                if recs and recs[0].get("institution_name"):
                    inst_name = recs[0]["institution_name"]
                    break

            # Match every pair of sources for this institution
            for src_a, src_b in combinations(self.source_ids, 2):
                recs_a = records_by_source.get(src_a, {}).get(e_mec, [])
                recs_b = records_by_source.get(src_b, {}).get(e_mec, [])

                if not recs_a and not recs_b:
                    continue

                matches = self._match_pair(recs_a, recs_b, src_a, src_b, e_mec)
                all_matches.extend(matches)

                # Overlap metrics
                n_a, n_b = len(recs_a), len(recs_b)
                n_matched = len(matches)
                overlap_matrix[(src_a, src_b, e_mec)] = {
                    "source_a": src_a,
                    "source_b": src_b,
                    "e_mec_code": e_mec,
                    "institution_name": inst_name,
                    "n_a": n_a,
                    "n_b": n_b,
                    "n_matched": n_matched,
                    "overlap_pct_a": round(n_matched / n_a, 4) if n_a else None,
                    "overlap_pct_b": round(n_matched / n_b, 4) if n_b else None,
                    "overlap_pct_min": round(n_matched / max(n_a, n_b), 4) if max(n_a, n_b) else None,
                }

                # Divergence check
                div = self._check_divergence(
                    e_mec=e_mec,
                    institution_name=inst_name,
                    src_a=src_a, src_b=src_b,
                    n_a=n_a, n_b=n_b,
                )
                if div:
                    divergences.append(div)
                    logger.info(
                        f"[convergence] Divergence: {src_a} vs {src_b} "
                        f"for {e_mec}: {n_a} vs {n_b} "
                        f"({div.discrepancy_pct:.1%})"
                    )

        review_queue = [m for m in all_matches if m.flagged]

        logger.info(
            f"[convergence] Total matches: {len(all_matches)} | "
            f"Flagged for review: {len(review_queue)} | "
            f"Divergences: {len(divergences)}"
        )

        return {
            "match_table": all_matches,
            "overlap_matrix": list(overlap_matrix.values()),
            "divergences": divergences,
            "review_queue": review_queue,
        }

    # ------------------------------------------------------------------
    # Pair matching
    # ------------------------------------------------------------------

    def _match_pair(
        self,
        recs_a: list[dict],
        recs_b: list[dict],
        src_a: str,
        src_b: str,
        e_mec: str,
    ) -> list[MatchRecord]:
        """Match two record lists for a single institution."""
        matches: list[MatchRecord] = []

        # Build DOI index for recs_b
        doi_index: dict[str, dict] = {}
        for r in recs_b:
            doi = _normalise_doi(r.get("doi"))
            if doi:
                doi_index[doi] = r

        # Build title+year index for recs_b
        title_year_index: dict[tuple, dict] = {}
        for r in recs_b:
            key = _title_year_key(r)
            if key:
                title_year_index[key] = r

        matched_b_ids: set[str] = set()

        for rec_a in recs_a:
            match = None

            # Level 1: DOI exact match
            doi_a = _normalise_doi(rec_a.get("doi"))
            if doi_a and doi_a in doi_index:
                rec_b = doi_index[doi_a]
                match = MatchRecord(
                    source_a=src_a, source_b=src_b,
                    record_id_a=rec_a.get("source_record_id", ""),
                    record_id_b=rec_b.get("source_record_id", ""),
                    doi_a=rec_a.get("doi"), doi_b=rec_b.get("doi"),
                    title_a=rec_a.get("title"), title_b=rec_b.get("title"),
                    year_a=rec_a.get("year"), year_b=rec_b.get("year"),
                    e_mec_code=e_mec,
                    match_key="doi",
                    confidence=1.0,
                )

            # Level 2: title + year + first-author
            if not match:
                key_a = _title_year_key(rec_a)
                if key_a and key_a in title_year_index:
                    rec_b = title_year_index[key_a]
                    if rec_b.get("source_record_id") not in matched_b_ids:
                        match = MatchRecord(
                            source_a=src_a, source_b=src_b,
                            record_id_a=rec_a.get("source_record_id", ""),
                            record_id_b=rec_b.get("source_record_id", ""),
                            doi_a=rec_a.get("doi"), doi_b=rec_b.get("doi"),
                            title_a=rec_a.get("title"), title_b=rec_b.get("title"),
                            year_a=rec_a.get("year"), year_b=rec_b.get("year"),
                            e_mec_code=e_mec,
                            match_key="title_year_author",
                            confidence=0.85,
                        )

            # Level 3: fuzzy title (expensive — only if no match yet)
            if not match and rec_a.get("title"):
                best_score = 0.0
                best_b = None
                title_a_norm = _normalise_title(rec_a.get("title", ""))
                for rec_b in recs_b:
                    if rec_b.get("source_record_id") in matched_b_ids:
                        continue
                    if not rec_b.get("title"):
                        continue
                    # Only compare same year ± 1 to limit false positives
                    if rec_a.get("year") and rec_b.get("year"):
                        if abs(rec_a["year"] - rec_b["year"]) > 1:
                            continue
                    score = fuzz.token_sort_ratio(
                        title_a_norm,
                        _normalise_title(rec_b.get("title", ""))
                    )
                    if score >= self.fuzzy_threshold and score > best_score:
                        best_score = score
                        best_b = rec_b

                if best_b:
                    match = MatchRecord(
                        source_a=src_a, source_b=src_b,
                        record_id_a=rec_a.get("source_record_id", ""),
                        record_id_b=best_b.get("source_record_id", ""),
                        doi_a=rec_a.get("doi"), doi_b=best_b.get("doi"),
                        title_a=rec_a.get("title"), title_b=best_b.get("title"),
                        year_a=rec_a.get("year"), year_b=best_b.get("year"),
                        e_mec_code=e_mec,
                        match_key="fuzzy_title",
                        confidence=round(best_score / 100, 2),
                        flagged=True,   # always flagged for review
                    )

            if match:
                matches.append(match)
                matched_b_ids.add(match.record_id_b)

        return matches

    # ------------------------------------------------------------------
    # Divergence detection
    # ------------------------------------------------------------------

    def _check_divergence(
        self, e_mec: str, institution_name: str,
        src_a: str, src_b: str, n_a: int, n_b: int,
    ) -> DivergenceFlag | None:
        if n_a == 0 and n_b == 0:
            return None
        if n_a == 0 or n_b == 0:
            # Complete absence in one source is a divergence
            discrepancy = 1.0
        else:
            discrepancy = abs(n_a - n_b) / max(n_a, n_b)

        if discrepancy < self.divergence_threshold:
            return None

        return DivergenceFlag(
            e_mec_code=e_mec,
            institution_name=institution_name,
            source_a=src_a,
            source_b=src_b,
            count_a=n_a,
            count_b=n_b,
            discrepancy_pct=round(discrepancy, 4),
            direction="a_higher" if n_a > n_b else "b_higher",
        )


# ------------------------------------------------------------------
# Normalisation helpers
# ------------------------------------------------------------------

def _normalise_doi(doi: str | None) -> str | None:
    """Normalise DOI to bare form: 10.xxx/yyy"""
    if not doi:
        return None
    doi = doi.strip().lower()
    doi = re.sub(r"^https?://doi\.org/", "", doi)
    doi = re.sub(r"^doi:", "", doi)
    return doi if doi.startswith("10.") else None


def _normalise_title(title: str) -> str:
    """Normalise title for fuzzy matching."""
    if not title:
        return ""
    # Decompose unicode, remove accents
    title = unicodedata.normalize("NFKD", title)
    title = "".join(c for c in title if not unicodedata.combining(c))
    # Lowercase, remove punctuation
    title = re.sub(r"[^\w\s]", " ", title.lower())
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _title_year_key(record: dict) -> tuple | None:
    """Build (normalised_title, year, first_author_family) key."""
    title = record.get("title")
    year = record.get("year")
    if not title or not year:
        return None

    title_norm = _normalise_title(title)

    # First author family name
    authors = record.get("authors") or []
    first_author = ""
    if authors:
        name = (authors[0].get("name") or "")
        # Handle "Family, Given" or "Given Family" formats
        parts = name.split(",")
        first_author = _normalise_title(parts[0].strip()) if parts else ""

    return (title_norm[:80], int(year), first_author[:20])
