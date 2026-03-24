# Source Fitness Scoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aggregate per-dimension evidence into a source fitness matrix (sources × institution types) with ranked recommendations and a Markdown fitness report.

**Architecture:** Each source gets a `FitnessProfile` built from evidence already collected (coverage results, OA results, convergence/overlap data) plus static expert scores for dimensions not measurable from data alone (Barcelona alignment, governance, licensing). The `FitnessScorer` aggregates sub-dimension scores using weights from `config/scoring_weights.yaml` into a weighted composite, stratified by institution type. Output is a `FitnessMatrix` exported as CSV, SQLite table, and Markdown report.

**Tech Stack:** Python 3.11+, pandas, PyYAML, existing `scoring/` modules, `outputs/dataset/exporter.py`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `scoring/fitness.py` | Create | `FitnessProfile`, `FitnessScorer`, `FitnessMatrix` — core aggregation logic |
| `scoring/static_scores.yaml` | Create | Expert-rated static scores per source for non-measurable dimensions (Barcelona alignment, governance, licensing) |
| `outputs/dataset/exporter.py` | Modify (append) | `export_fitness_matrix()`, `export_fitness_report()` |
| `run_fitness.py` | Create | CLI runner — reads sprint1 + phase2 outputs, runs fitness scoring, exports |
| `tests/scoring/test_fitness.py` | Create | Unit tests for FitnessScorer, FitnessMatrix, static score loading |

---

## Task 1: Static scores YAML

**Files:**
- Create: `scoring/static_scores.yaml`

Static scores encode expert judgement for dimensions that cannot be derived from API data: Barcelona Declaration alignment, governance/sustainability, licensing cost to Brazilian HEIs. Scale 0.0–1.0.

- [ ] **Step 1: Write `scoring/static_scores.yaml`**

```yaml
# Static expert scores per source — dimensions not derivable from API data
# Scale: 0.0 (worst) – 1.0 (best)
# Reviewed: 2026-03 | Next review: 2027-03

sources:
  openalex:
    accessibility:
      barcelona_alignment: 1.0      # signatory, fully open
      api_availability: 1.0         # free, documented, stable
      licensing_model: 1.0          # CC0
      cost_to_br_hei: 1.0           # free
      practical_usability: 0.85     # excellent docs, minor rate limits
    governance:
      ownership_transparency: 0.95  # NumFOCUS / OurResearch, published roadmap
      sustainability_risk: 0.80     # grant-funded but diversified
      community_governance: 0.85    # open governance, community input
      data_portability: 1.0         # full data dumps available
    social_impact:
      policy_citations: 0.30        # not yet tracked
      public_engagement: 0.20       # not tracked
      geographic_social_context: 0.60
    reliability:
      methodological_transparency: 0.95
      reproducibility: 0.90

  scopus:
    accessibility:
      barcelona_alignment: 0.10     # proprietary, opposes open infra
      api_availability: 0.70        # API exists but key required, quota limited
      licensing_model: 0.20         # subscription only
      cost_to_br_hei: 0.20          # expensive; some CAPES coverage
      practical_usability: 0.75
    governance:
      ownership_transparency: 0.60  # Elsevier/RELX, commercial
      sustainability_risk: 0.70     # commercial entity — stable but costly
      community_governance: 0.10    # no community governance
      data_portability: 0.30        # limited bulk export
    social_impact:
      policy_citations: 0.40
      public_engagement: 0.20
      geographic_social_context: 0.40
    reliability:
      methodological_transparency: 0.70
      reproducibility: 0.65

  wos:
    accessibility:
      barcelona_alignment: 0.10
      api_availability: 0.65
      licensing_model: 0.20
      cost_to_br_hei: 0.20
      practical_usability: 0.70
    governance:
      ownership_transparency: 0.55  # Clarivate, private equity backed
      sustainability_risk: 0.60
      community_governance: 0.10
      data_portability: 0.25
    social_impact:
      policy_citations: 0.45
      public_engagement: 0.20
      geographic_social_context: 0.35
    reliability:
      methodological_transparency: 0.75
      reproducibility: 0.70

  dimensions:
    accessibility:
      barcelona_alignment: 0.50     # free tier open; full access proprietary
      api_availability: 0.75
      licensing_model: 0.50
      cost_to_br_hei: 0.60          # free tier sufficient for most uses
      practical_usability: 0.80
    governance:
      ownership_transparency: 0.65  # Digital Science / Holtzbrinck
      sustainability_risk: 0.65
      community_governance: 0.20
      data_portability: 0.50
    social_impact:
      policy_citations: 0.60        # Altmetric integration
      public_engagement: 0.50
      geographic_social_context: 0.55
    reliability:
      methodological_transparency: 0.80
      reproducibility: 0.75

  lens:
    accessibility:
      barcelona_alignment: 0.90     # Cambia open mission, patent data CC0
      api_availability: 0.80
      licensing_model: 0.85
      cost_to_br_hei: 0.85          # free scholarly; patent needs registration
      practical_usability: 0.75
    governance:
      ownership_transparency: 0.85  # Cambia, public benefit mission
      sustainability_risk: 0.70
      community_governance: 0.70
      data_portability: 0.85
    social_impact:
      policy_citations: 0.20
      public_engagement: 0.20
      geographic_social_context: 0.50
    reliability:
      methodological_transparency: 0.80
      reproducibility: 0.80

  inep_microdados:
    accessibility:
      barcelona_alignment: 0.85     # government open data
      api_availability: 0.30        # file download only, no API
      licensing_model: 0.90
      cost_to_br_hei: 1.0
      practical_usability: 0.55     # CSV bulk, complex encoding
    governance:
      ownership_transparency: 1.0   # INEP / MEC
      sustainability_risk: 0.85     # statutory obligation
      community_governance: 0.40
      data_portability: 0.80
    social_impact:
      policy_citations: 0.90        # primary policy source
      public_engagement: 0.70
      geographic_social_context: 0.95
    reliability:
      methodological_transparency: 0.90
      reproducibility: 0.85

  lattes:
    accessibility:
      barcelona_alignment: 0.60     # public but scrape-only; no open API
      api_availability: 0.20        # no official API
      licensing_model: 0.70
      cost_to_br_hei: 0.90
      practical_usability: 0.40     # scraping fragile; LGPD constraints
    governance:
      ownership_transparency: 0.90  # CNPq / MCTI
      sustainability_risk: 0.75
      community_governance: 0.30
      data_portability: 0.35
    social_impact:
      policy_citations: 0.80
      public_engagement: 0.60
      geographic_social_context: 0.90
    reliability:
      methodological_transparency: 0.65
      reproducibility: 0.50         # scraping reproducibility limited
```

- [ ] **Step 2: Verify YAML loads cleanly**

```bash
python3 -c "import yaml; d=yaml.safe_load(open('scoring/static_scores.yaml')); print(list(d['sources'].keys()))"
```
Expected: `['openalex', 'scopus', 'wos', 'dimensions', 'lens', 'inep_microdados', 'lattes']`

---

## Task 2: `FitnessProfile` and `FitnessScorer`

**Files:**
- Create: `scoring/fitness.py`
- Test: `tests/scoring/test_fitness.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/scoring/test_fitness.py
import pytest
from scoring.fitness import FitnessProfile, FitnessScorer, FitnessMatrix

MOCK_COVERAGE = {
    "openalex": {"federal_university": {"institution_coverage": 0.95, "field_coverage": 0.85,
                  "temporal_coverage": 1.0, "language_coverage": 0.70, "record_count": 450}},
}
MOCK_OA = {
    "openalex": {"federal_university": {"oa_rate": 0.72, "diamond_rate": 0.15,
                  "unpaywall_agreement": 0.93}},
}
MOCK_CONVERGENCE = {
    ("openalex", "scopus"): {"overlap_pct": 0.60, "divergence_pct": 0.05},
}

def test_fitness_profile_has_required_keys():
    scorer = FitnessScorer()
    profile = scorer.build_profile(
        source_id="openalex",
        inst_type="federal_university",
        coverage=MOCK_COVERAGE["openalex"]["federal_university"],
        oa=MOCK_OA["openalex"]["federal_university"],
        convergence=MOCK_CONVERGENCE,
    )
    assert set(profile.keys()) >= {"coverage", "data_quality", "reliability",
                                    "accessibility", "social_impact", "governance",
                                    "innovation_link", "composite"}

def test_composite_between_0_and_1():
    scorer = FitnessScorer()
    profile = scorer.build_profile("openalex", "federal_university",
                                    MOCK_COVERAGE["openalex"]["federal_university"],
                                    MOCK_OA["openalex"]["federal_university"],
                                    MOCK_CONVERGENCE)
    assert 0.0 <= profile["composite"] <= 1.0

def test_fitness_matrix_shape():
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(
        coverage_by_source_type=MOCK_COVERAGE,
        oa_by_source_type=MOCK_OA,
        convergence=MOCK_CONVERGENCE,
    )
    assert isinstance(matrix, FitnessMatrix)
    assert len(matrix.rows) >= 1

def test_ranking_per_inst_type():
    scorer = FitnessScorer()
    matrix = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    ranking = matrix.rank_by_inst_type("federal_university")
    assert isinstance(ranking, list)
    assert ranking[0]["source"] == "openalex"   # only source in mock

def test_open_sources_score_higher_accessibility():
    scorer = FitnessScorer()
    oa_profile = scorer.build_profile("openalex", "federal_university",
                                       MOCK_COVERAGE["openalex"]["federal_university"],
                                       MOCK_OA["openalex"]["federal_university"],
                                       MOCK_CONVERGENCE)
    prop_profile = scorer.build_profile("scopus", "federal_university",
                                         MOCK_COVERAGE["openalex"]["federal_university"],
                                         MOCK_OA["openalex"]["federal_university"],
                                         MOCK_CONVERGENCE)
    assert oa_profile["accessibility"] > prop_profile["accessibility"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/scoring/test_fitness.py -v
```
Expected: `ModuleNotFoundError: No module named 'scoring.fitness'`

- [ ] **Step 3: Write `scoring/fitness.py`**

```python
"""
Source fitness scoring.

Aggregates per-dimension evidence into a FitnessMatrix:
  sources × institution_types → weighted composite score

Dimensions:
  1. coverage        — from CoverageResult (dynamic, per institution)
  2. data_quality    — from CoverageResult fields + convergence overlap
  3. reliability     — from convergence inter-source agreement + static scores
  4. accessibility   — static scores (Barcelona alignment, licensing, cost)
  5. social_impact   — from OAResult + static scores
  6. governance      — static scores
  7. innovation_link — from LensConnector patent summary (if available)

Static scores loaded from: scoring/static_scores.yaml
Weights loaded from:       config/scoring_weights.yaml
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import yaml

logger = logging.getLogger(__name__)

_STATIC_PATH  = Path("scoring/static_scores.yaml")
_WEIGHTS_PATH = Path("config/scoring_weights.yaml")

# Institution type vocabulary (maps raw categories to canonical keys)
INST_TYPE_MAP = {
    "universidade federal":  "federal_university",
    "universidade estadual": "state_university",
    "universidade privada":  "private_university",
    "instituto federal":     "federal_institute",
    "universidade comunitária": "community_university",
    "faculdade":             "isolated_faculty",
    # fallback
    "other":                 "other",
}


def _load_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class FitnessProfile:
    """Scores for one source × one institution type."""
    source: str
    inst_type: str
    coverage: float
    data_quality: float
    reliability: float
    accessibility: float
    social_impact: float
    governance: float
    innovation_link: float
    composite: float
    evidence: dict = field(default_factory=dict)   # raw inputs for audit trail

    def keys(self):
        return {"coverage", "data_quality", "reliability", "accessibility",
                "social_impact", "governance", "innovation_link", "composite"}

    def __getitem__(self, key):
        return getattr(self, key)


@dataclass
class FitnessMatrix:
    """All profiles for all source × institution type combinations."""
    rows: list[FitnessProfile] = field(default_factory=list)

    def rank_by_inst_type(self, inst_type: str) -> list[dict]:
        """Return sources ranked by composite score for a given institution type."""
        matching = [r for r in self.rows if r.inst_type == inst_type]
        return sorted(
            [{"source": r.source, "composite": r.composite,
              "coverage": r.coverage, "accessibility": r.accessibility} for r in matching],
            key=lambda x: -x["composite"],
        )

    def to_records(self) -> list[dict]:
        return [
            {
                "source": r.source, "inst_type": r.inst_type,
                "coverage": round(r.coverage, 3),
                "data_quality": round(r.data_quality, 3),
                "reliability": round(r.reliability, 3),
                "accessibility": round(r.accessibility, 3),
                "social_impact": round(r.social_impact, 3),
                "governance": round(r.governance, 3),
                "innovation_link": round(r.innovation_link, 3),
                "composite": round(r.composite, 3),
            }
            for r in self.rows
        ]


class FitnessScorer:
    """
    Aggregates evidence into fitness profiles.

    Usage:
        scorer = FitnessScorer()
        matrix = scorer.build_matrix(
            coverage_by_source_type,
            oa_by_source_type,
            convergence,
            patent_by_source_type,   # optional
        )
    """

    def __init__(
        self,
        static_path: Path = _STATIC_PATH,
        weights_path: Path = _WEIGHTS_PATH,
    ):
        self.static  = _load_yaml(static_path).get("sources", {})
        raw_w        = _load_yaml(weights_path)
        self.dim_w   = raw_w.get("dimension_weights", {})
        self.sub_w   = {k: raw_w[k] for k in raw_w if k not in
                        ("dimension_weights", "confidence_tiers", "divergence_threshold")}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_profile(
        self,
        source_id: str,
        inst_type: str,
        coverage: dict,
        oa: dict,
        convergence: dict,
        patents: dict | None = None,
    ) -> FitnessProfile:
        static = self.static.get(source_id, {})

        cov_score  = self._score_coverage(coverage)
        dq_score   = self._score_data_quality(coverage, convergence, source_id)
        rel_score  = self._score_reliability(convergence, source_id, static)
        acc_score  = self._score_accessibility(static)
        si_score   = self._score_social_impact(oa, static)
        gov_score  = self._score_governance(static)
        inn_score  = self._score_innovation_link(patents)

        weights = self.dim_w
        total_w = sum(weights.values()) or 1.0
        composite = (
            weights.get("coverage", 0.20)      * cov_score +
            weights.get("data_quality", 0.20)  * dq_score +
            weights.get("reliability", 0.15)   * rel_score +
            weights.get("accessibility", 0.20) * acc_score +
            weights.get("social_impact", 0.10) * si_score +
            weights.get("governance", 0.10)    * gov_score +
            weights.get("innovation_link", 0.05) * inn_score
        ) / total_w

        return FitnessProfile(
            source=source_id, inst_type=inst_type,
            coverage=cov_score, data_quality=dq_score, reliability=rel_score,
            accessibility=acc_score, social_impact=si_score,
            governance=gov_score, innovation_link=inn_score,
            composite=min(1.0, max(0.0, composite)),
            evidence={
                "coverage_input": coverage, "oa_input": oa,
                "patents_input": patents,
            },
        )

    def build_matrix(
        self,
        coverage_by_source_type: dict[str, dict[str, dict]],
        oa_by_source_type: dict[str, dict[str, dict]],
        convergence: dict,
        patent_by_source_type: dict | None = None,
    ) -> FitnessMatrix:
        rows: list[FitnessProfile] = []
        for source_id, type_map in coverage_by_source_type.items():
            for inst_type, cov in type_map.items():
                oa      = (oa_by_source_type.get(source_id) or {}).get(inst_type, {})
                patents = ((patent_by_source_type or {}).get(source_id) or {}).get(inst_type)
                profile = self.build_profile(
                    source_id=source_id,
                    inst_type=inst_type,
                    coverage=cov,
                    oa=oa,
                    convergence=convergence,
                    patents=patents,
                )
                rows.append(profile)
        return FitnessMatrix(rows=rows)

    # ------------------------------------------------------------------
    # Dimension scorers
    # ------------------------------------------------------------------

    def _score_coverage(self, cov: dict) -> float:
        w = self.sub_w.get("coverage", {})
        parts = [
            w.get("institutional_coverage", 0.35) * float(cov.get("institution_coverage", 0)),
            w.get("field_coverage", 0.20)          * float(cov.get("field_coverage", 0)),
            w.get("temporal_coverage", 0.15)       * float(cov.get("temporal_coverage", 1.0)),
            w.get("language_coverage", 0.20)       * float(cov.get("language_coverage", 0)),
        ]
        total_w = sum([w.get(k, 0) for k in
                       ("institutional_coverage", "field_coverage",
                        "temporal_coverage", "language_coverage")]) or 1.0
        return min(1.0, sum(parts) / total_w)

    def _score_data_quality(self, cov: dict, convergence: dict, source_id: str) -> float:
        # Completeness: proxy via DOI coverage rate
        doi_rate      = float(cov.get("doi_coverage_rate", 0.5))
        # Disambiguation: proxy via overlap with other sources (high overlap = good ID)
        overlap_vals  = [v.get("overlap_pct", 0) for k, v in convergence.items()
                         if source_id in k]
        avg_overlap   = sum(overlap_vals) / len(overlap_vals) if overlap_vals else 0.5
        # Timeliness: 1.0 if temporal window is current (fixed for now)
        timeliness    = 1.0

        w = self.sub_w.get("data_quality", {})
        total_w = (w.get("completeness", 0.30) + w.get("disambiguation_quality", 0.30) +
                   w.get("timeliness", 0.20)) or 1.0
        return min(1.0, (
            w.get("completeness", 0.30)            * doi_rate +
            w.get("disambiguation_quality", 0.30)  * avg_overlap +
            w.get("timeliness", 0.20)              * timeliness
        ) / total_w)

    def _score_reliability(self, convergence: dict, source_id: str, static: dict) -> float:
        # Inter-source agreement from convergence
        overlap_vals = [v.get("overlap_pct", 0) for k, v in convergence.items()
                        if source_id in k]
        inter_src = sum(overlap_vals) / len(overlap_vals) if overlap_vals else 0.5

        rel_static = static.get("reliability", {})
        meth_transp  = float(rel_static.get("methodological_transparency", 0.5))
        repro        = float(rel_static.get("reproducibility", 0.5))

        w = self.sub_w.get("reliability", {})
        total_w = (w.get("inter_source_agreement", 0.30) +
                   w.get("methodological_transparency", 0.25) +
                   w.get("reproducibility", 0.20)) or 1.0
        return min(1.0, (
            w.get("inter_source_agreement", 0.30)     * inter_src +
            w.get("methodological_transparency", 0.25) * meth_transp +
            w.get("reproducibility", 0.20)             * repro
        ) / total_w)

    def _score_accessibility(self, static: dict) -> float:
        acc = static.get("accessibility", {})
        w   = self.sub_w.get("accessibility", {})
        keys = ("barcelona_alignment", "api_availability", "licensing_model",
                "cost_to_br_hei", "practical_usability")
        total_w = sum(w.get(k, 0.2) for k in keys) or 1.0
        return min(1.0, sum(
            w.get(k, 0.2) * float(acc.get(k, 0.5)) for k in keys
        ) / total_w)

    def _score_social_impact(self, oa: dict, static: dict) -> float:
        oa_rate    = float(oa.get("oa_rate", 0))
        si_static  = static.get("social_impact", {})
        policy     = float(si_static.get("policy_citations", 0.2))
        engagement = float(si_static.get("public_engagement", 0.2))
        geo        = float(si_static.get("geographic_social_context", 0.5))

        w = self.sub_w.get("social_impact", {})
        total_w = (w.get("oa_percentage", 0.20) + w.get("policy_citations", 0.20) +
                   w.get("public_engagement", 0.20) +
                   w.get("geographic_social_context", 0.15)) or 1.0
        return min(1.0, (
            w.get("oa_percentage", 0.20)             * oa_rate +
            w.get("policy_citations", 0.20)          * policy +
            w.get("public_engagement", 0.20)         * engagement +
            w.get("geographic_social_context", 0.15) * geo
        ) / total_w)

    def _score_governance(self, static: dict) -> float:
        gov = static.get("governance", {})
        w   = self.sub_w.get("governance", {})
        keys = ("ownership_transparency", "sustainability_risk",
                "community_governance", "data_portability")
        total_w = sum(w.get(k, 0.25) for k in keys) or 1.0
        return min(1.0, sum(
            w.get(k, 0.25) * float(gov.get(k, 0.5)) for k in keys
        ) / total_w)

    def _score_innovation_link(self, patents: dict | None) -> float:
        if not patents:
            return 0.0
        w   = self.sub_w.get("innovation_link", {})
        total = patents.get("patent_count", 0)
        intl  = patents.get("intl_patent_families", 0)
        links = patents.get("unique_npl_papers", 0)
        # Normalise to 0–1 using soft caps (50 patents, 20 intl, 20 links)
        pat_score   = min(1.0, total / 50)
        intl_score  = min(1.0, intl  / 20)
        link_score  = min(1.0, links / 20)
        # Weight mapping:
        #   patent_publication_links → paper-patent NPL link rate
        #   br_inventor_coverage     → patent count (HEI as assignee)
        #   hei_assignee_coverage    → international family reach
        total_w = (w.get("patent_publication_links", 0.40) +
                   w.get("br_inventor_coverage", 0.30) +
                   w.get("hei_assignee_coverage", 0.30)) or 1.0
        return min(1.0, (
            w.get("patent_publication_links", 0.40) * link_score +
            w.get("br_inventor_coverage", 0.30)     * pat_score +
            w.get("hei_assignee_coverage", 0.30)    * intl_score
        ) / total_w)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/scoring/test_fitness.py -v
```
Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
git add scoring/fitness.py scoring/static_scores.yaml tests/scoring/test_fitness.py
git commit -m "feat: add FitnessScorer with 7-dimension weighted aggregation"
```

---

## Task 3: Fitness export methods in `exporter.py`

**Files:**
- Modify: `outputs/dataset/exporter.py` (append two methods)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/scoring/test_fitness.py

from outputs.dataset.exporter import DatasetExporter
import tempfile, os, pandas as pd

def test_export_fitness_matrix_creates_csv(tmp_path):
    scorer  = FitnessScorer()
    matrix  = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    exp     = DatasetExporter(output_dir=str(tmp_path))
    path    = exp.export_fitness_matrix(matrix, run_id="test")
    assert path.exists()
    df = pd.read_csv(path)
    assert "source" in df.columns
    assert "composite" in df.columns

def test_export_fitness_report_creates_markdown(tmp_path):
    scorer  = FitnessScorer()
    matrix  = scorer.build_matrix(MOCK_COVERAGE, MOCK_OA, MOCK_CONVERGENCE)
    exp     = DatasetExporter(output_dir=str(tmp_path))
    path    = exp.export_fitness_report(matrix, run_id="test")
    assert path.exists()
    content = path.read_text()
    assert "Source Fitness" in content
    assert "openalex" in content
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/scoring/test_fitness.py::test_export_fitness_matrix_creates_csv -v
```
Expected: `AttributeError: 'DatasetExporter' object has no attribute 'export_fitness_matrix'`

- [ ] **Step 3: Add `export_fitness_matrix()` and `export_fitness_report()` to `outputs/dataset/exporter.py`**

```python
def export_fitness_matrix(self, matrix: "FitnessMatrix", run_id: str) -> Path:
    """Export fitness matrix as CSV and write to SQLite."""
    import pandas as pd
    records = matrix.to_records()
    df = pd.DataFrame(records)
    path = self.output_dir / f"fitness_matrix_{run_id}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"Fitness matrix: {path} ({len(df)} rows)")

    # SQLite
    db_path = self.output_dir / f"fitness_{run_id}.db"
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        df.to_sql("fitness_matrix", conn, if_exists="replace", index=False)
    return path

def export_fitness_report(self, matrix: "FitnessMatrix", run_id: str) -> Path:
    """Markdown fitness report — source recommendations per institution type."""
    from datetime import datetime, timezone
    inst_types = sorted({r.inst_type for r in matrix.rows})
    sources    = sorted({r.source for r in matrix.rows})

    lines = [
        "# INEP Bibliometric Tool — Source Fitness Report",
        "",
        f"**Run:** `{run_id}`  ",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "> Composite score = weighted average across 7 dimensions "
        "(Coverage 20%, Data Quality 20%, Reliability 15%, "
        "Accessibility 20%, Social Impact 10%, Governance 10%, Innovation 5%)",
        "",
        "> ★ = recommended primary source for this institution type  "
        "| ☆ = recommended supplementary",
        "",
        "---",
        "",
        "## Composite Fitness Matrix",
        "",
    ]

    # Header
    header = "| Institution Type |" + "".join(f" {s} |" for s in sources)
    sep    = "|---|" + "---|" * len(sources)
    lines += [header, sep]

    for inst_type in inst_types:
        ranking = matrix.rank_by_inst_type(inst_type)
        top     = ranking[0]["source"] if ranking else None
        row = f"| {inst_type} |"
        for s in sources:
            score_rec = next((r for r in ranking if r["source"] == s), None)
            if score_rec:
                score = score_rec["composite"]
                flag  = " ★" if s == top else ""
                row  += f" {score:.2f}{flag} |"
            else:
                row += " — |"
        lines.append(row)

    # Per-institution-type detail
    lines += ["", "---", "", "## Recommendations by Institution Type", ""]
    for inst_type in inst_types:
        ranking = matrix.rank_by_inst_type(inst_type)
        lines.append(f"### {inst_type.replace('_', ' ').title()}")
        lines.append("")
        for i, r in enumerate(ranking, 1):
            badge = "★ Primary" if i == 1 else ("☆ Supplementary" if i == 2 else "")
            lines.append(
                f"{i}. **{r['source']}** — composite {r['composite']:.2f}"
                + (f" | accessibility {r['accessibility']:.2f}" )
                + (f"  `{badge}`" if badge else "")
            )
        lines.append("")

    # Dimension breakdown per source
    lines += ["---", "", "## Dimension Breakdown by Source", ""]
    dims = ["coverage", "data_quality", "reliability", "accessibility",
            "social_impact", "governance", "innovation_link"]
    dim_header = "| Source |" + "".join(f" {d[:8]} |" for d in dims) + " composite |"
    dim_sep    = "|---|" + "---|" * (len(dims) + 1)
    lines += [dim_header, dim_sep]

    for src in sources:
        # Average across inst types for this source
        src_rows = [r for r in matrix.rows if r.source == src]
        if not src_rows:
            continue
        avg = {d: sum(getattr(r, d) for r in src_rows) / len(src_rows) for d in dims}
        avg_composite = sum(r.composite for r in src_rows) / len(src_rows)
        row = f"| {src} |" + "".join(f" {avg[d]:.2f} |" for d in dims)
        row += f" **{avg_composite:.2f}** |"
        lines.append(row)

    lines += [
        "", "---", "",
        "## Barcelona Declaration Alignment Note",
        "",
        "Sources scored above 0.80 on accessibility are aligned with the "
        "[Barcelona Declaration on Open Research Information](https://barcelona-declaration.org/). "
        "SINAES indicator design should prioritise these sources as primary infrastructure.",
        "",
        "---",
        "",
        f"*Generated by inep-bibliometric-tool v0.1.0*",
    ]

    path = self.output_dir / f"fitness_report_{run_id}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Fitness report: {path}")
    return path
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/scoring/test_fitness.py -v
```
Expected: all 7 PASSED

- [ ] **Step 5: Commit**

```bash
git add outputs/dataset/exporter.py tests/scoring/test_fitness.py
git commit -m "feat: add fitness matrix and report export to DatasetExporter"
```

---

## Task 4: `run_fitness.py` CLI runner

**Files:**
- Create: `run_fitness.py`

Reads sprint1 + phase2 CSV outputs (already on disk), reshapes into `coverage_by_source_type` / `oa_by_source_type`, calls `FitnessScorer.build_matrix()`, exports.

- [ ] **Step 1: Write `run_fitness.py`**

```python
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
    "Federal":        "federal_university",
    "Instituto Federal": "federal_institute",
    "Estadual":       "state_university",
    "Pontifícia":     "private_university",
    "PUC":            "private_university",
}

def _infer_inst_type(name: str) -> str:
    for kw, t in _NAME_TO_TYPE.items():
        if kw.lower() in name.lower():
            return t
    return "other"


def _load_coverage(pattern: str) -> dict[str, dict[str, dict]]:
    """Return {source: {inst_type: coverage_dict}}."""
    files = sorted(PROCESSED.glob(pattern))
    if not files:
        logger.warning(f"No coverage files matching {pattern}")
        return {}
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    # Accumulate sums and counts, then divide at the end for correct mean
    sums:   dict[tuple, dict] = {}
    counts: dict[tuple, dict] = {}
    rec_counts: dict[tuple, int] = {}
    numeric_fields = ("institution_coverage", "field_coverage",
                      "language_coverage", "doi_coverage_rate")
    for _, row in df.iterrows():
        src       = row.get("source", "unknown")
        inst_name = row.get("institution_name", "")
        inst_type = _infer_inst_type(inst_name)
        key = (src, inst_type)
        sums.setdefault(key, {f: 0.0 for f in numeric_fields})
        counts.setdefault(key, {f: 0 for f in numeric_fields})
        rec_counts[key] = rec_counts.get(key, 0) + int(row.get("n_records", 0))
        for field in numeric_fields:
            if field in row and pd.notna(row[field]):
                sums[key][field] += float(row[field])
                counts[key][field] += 1
    result: dict[str, dict[str, dict]] = {}
    for (src, inst_type), s in sums.items():
        c = counts[(src, inst_type)]
        result.setdefault(src, {})[inst_type] = {
            "institution_coverage": s["institution_coverage"] / max(c["institution_coverage"], 1),
            "field_coverage":       s["field_coverage"]       / max(c["field_coverage"], 1),
            "temporal_coverage":    1.0,
            "language_coverage":    s["language_coverage"]    / max(c["language_coverage"], 1),
            "doi_coverage_rate":    s["doi_coverage_rate"]    / max(c["doi_coverage_rate"], 1),
            "record_count":         rec_counts[(src, inst_type)],
        }
    return result


def _load_oa(pattern: str) -> dict[str, dict[str, dict]]:
    """Return {source: {inst_type: oa_dict}}."""
    files = sorted(PROCESSED.glob(pattern))
    if not files:
        return {}
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    oa_fields = ("oa_rate", "diamond_rate", "unpaywall_agreement")
    sums:   dict[tuple, dict] = {}
    counts: dict[tuple, dict] = {}
    for _, row in df.iterrows():
        src       = row.get("source", "unknown")
        inst_name = row.get("institution_name", "")
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
        key = (row.get("source_a", ""), row.get("source_b", ""))
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
        logger.error("No coverage data found — run run_sprint1.py or run_phase2.py first")
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
```

- [ ] **Step 2: Smoke test with existing data**

```bash
python run_fitness.py
```
Expected: fitness_matrix CSV + fitness_report MD created in `data/processed/`

- [ ] **Step 3: Verify report content**

```bash
head -40 data/processed/fitness_report_$(date +%Y-%m-%d).md
```
Expected: Header, Barcelona Declaration note, composite matrix table.

- [ ] **Step 4: Commit**

```bash
git add run_fitness.py
git commit -m "feat: add fitness scoring runner"
```

---

## Task 5: Full test run

- [ ] **Step 1: Run full test suite**

```bash
pytest tests/ -v
```
Expected: all existing tests + new fitness tests pass (≥ 130 passing)

- [ ] **Step 2: Commit final**

```bash
git add .
git commit -m "feat: source fitness scoring complete — matrix, report, runner"
```
