# Coverage Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SDG coverage, geographic bias, non-academic coauthorship, deduplication wiring, and Diamond OA classification as computed sub-dimensions in the fitness scorer, and switch the phase 2 run to uncapped 2023-only data.

**Architecture:** Post-run enrichment pass — a new `run_enrichment.py` CLI and four modules under `enrichment/` enrich phase 2 outputs without touching the existing pipeline. The fitness scorer reads new enrichment CSVs as optional kwargs; graceful no-op if files are absent. Connector changes (max_records guard, SDG + affiliation fields) are backward-compatible.

**Tech Stack:** Python 3.11+, pandas, httpx, pytest, PyYAML. No new dependencies.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `config/sample_config.yaml` | Modify | 2023-only, uncapped |
| `config/scoring_weights.yaml` | Modify | Add diamond_oa + nonacademic_coauth sub-weights, rebalance |
| `connectors/api/openalex.py` | Modify | max_records guard, SDG field parsing, affiliation_types |
| `connectors/api/dimensions.py` | Modify | max_records guard, SDG field parsing |
| `connectors/api/scopus.py` | Modify | Attempt SDG field, write flag to source_metadata.json |
| `enrichment/__init__.py` | Create | Empty package marker |
| `enrichment/diamond_oa.py` | Create | Classify OA papers by type from pdf_url + oa_status |
| `enrichment/geographic.py` | Create | Coverage gap + output gap vs INEP Microdados baseline |
| `enrichment/coauthorship.py` | Create | Non-academic coauth detectability, volume, quality |
| `enrichment/sdg.py` | Create | Per-goal SDG rates + matched-set agreement |
| `run_enrichment.py` | Create | CLI entry point: orchestrates all four enrichment modules |
| `scoring/fitness.py` | Modify | New kwargs on build_profile + 4 scoring methods |
| `run_fitness.py` | Modify | Load dedup scores, pass enrichment kwargs |
| `tests/enrichment/test_diamond_oa.py` | Create | ≥6 tests |
| `tests/enrichment/test_geographic.py` | Create | ≥6 tests |
| `tests/enrichment/test_coauthorship.py` | Create | ≥6 tests |
| `tests/enrichment/test_sdg.py` | Create | ≥8 tests |
| `tests/scoring/test_fitness.py` | Modify | ≥4 new tests for new scorer kwargs |

---

## Task 1: Config Changes + Connector max_records Guard

**Files:**
- Modify: `config/sample_config.yaml`
- Modify: `config/scoring_weights.yaml`
- Modify: `connectors/api/openalex.py:L49-59` (`__init__`)
- Modify: `connectors/api/dimensions.py` (`__init__`, find `max_records` assignment)

- [ ] **Step 1: Write failing tests for max_records guard**

```python
# tests/connectors/test_openalex_guard.py
import pytest
from connectors.api.openalex import OpenAlexConnector

def test_max_records_none_becomes_inf():
    conn = OpenAlexConnector(max_records=None)
    assert conn.max_records == float("inf")

def test_max_records_int_unchanged():
    conn = OpenAlexConnector(max_records=200)
    assert conn.max_records == 200

def test_max_records_default_unchanged():
    conn = OpenAlexConnector()
    assert conn.max_records == 500
```

- [ ] **Step 2: Run to verify tests fail**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
pytest tests/connectors/test_openalex_guard.py -v
```
Expected: FAIL — `assert float("inf") == float("inf")` passes trivially if guard already missing, or TypeError.

- [ ] **Step 3: Add max_records guard to OpenAlex `__init__`**

In `connectors/api/openalex.py`, in `__init__`, change the `self.max_records` assignment (currently `self.max_records = max_records`, L~57) to:
```python
self.max_records = max_records if max_records is not None else float("inf")
```

Also change the type hint on the parameter from `int = 500` to `int | None = 500`.

- [ ] **Step 4: Apply same guard to Dimensions connector**

Find the `max_records` assignment in `connectors/api/dimensions.py __init__` and apply:
```python
self.max_records = max_records if max_records is not None else float("inf")
```
Update type hint accordingly.

- [ ] **Step 5: Run tests to verify guard passes**

```bash
pytest tests/connectors/test_openalex_guard.py -v
```
Expected: 3 PASS

- [ ] **Step 6: Update `config/sample_config.yaml`**

Change:
```yaml
# Remove or null out max_records_per_query
max_records_per_query: ~   # null = uncapped
```
And:
```yaml
temporal_window:
  start: 2023
  end: 2023
```

- [ ] **Step 7: Update `config/scoring_weights.yaml`**

Replace the `social_impact` block:
```yaml
social_impact:
  sdg_coverage: 0.25
  oa_percentage: 0.20
  policy_citations: 0.15     # was 0.20
  public_engagement: 0.15    # was 0.20
  geographic_social_context: 0.15
  diamond_oa: 0.10           # NEW
```

Replace the `innovation_link` block:
```yaml
innovation_link:
  npl_link_rate: 0.35        # was 0.40
  patent_count_score: 0.25   # was 0.30
  intl_family_score: 0.30
  nonacademic_coauth: 0.10   # NEW
```

- [ ] **Step 8: Verify YAML parses correctly**

```bash
python3 -c "
import yaml
with open('config/scoring_weights.yaml') as f:
    d = yaml.safe_load(f)
si = d['social_impact']
il = d['innovation_link']
assert abs(sum(si.values()) - 1.0) < 1e-9, f'social_impact sums to {sum(si.values())}'
assert abs(sum(il.values()) - 1.0) < 1e-9, f'innovation_link sums to {sum(il.values())}'
print('YAML weights OK')
"
```
Expected: `YAML weights OK`

- [ ] **Step 9: Run full test suite to confirm no regressions**

```bash
pytest tests/ -q
```
Expected: 176 passed, 1 skipped (existing baseline)

- [ ] **Step 10: Commit**

```bash
git add config/sample_config.yaml config/scoring_weights.yaml \
        connectors/api/openalex.py connectors/api/dimensions.py \
        tests/connectors/test_openalex_guard.py
git commit -m "feat: uncapped 2023 config, max_records guard, YAML weight rebalancing"
```

---

## Task 2: Diamond OA Classifier

**Files:**
- Create: `enrichment/__init__.py`
- Create: `enrichment/diamond_oa.py`
- Create: `tests/enrichment/__init__.py`
- Create: `tests/enrichment/test_diamond_oa.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_diamond_oa.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.diamond_oa import classify_oa, enrich_oa_file, _DIAMOND_PATTERNS

def test_native_diamond_status():
    assert classify_oa("diamond", None) == "diamond"

def test_scielo_url_is_diamond():
    assert classify_oa("gold", "https://www.scielo.br/article/123") == "diamond"

def test_redalyc_url_is_diamond():
    assert classify_oa("green", "https://redalyc.org/pdf/123") == "diamond"

def test_gold_no_diamond_url():
    assert classify_oa("gold", "https://doi.org/10.1234/xyz") == "gold"

def test_closed_is_closed():
    assert classify_oa("closed", None) == "closed"

def test_none_status_is_closed():
    assert classify_oa(None, None) == "closed"

def test_enrich_oa_file_adds_oa_type_column(tmp_path):
    csv = tmp_path / "oa_phase2_2026-03-24.csv"
    df = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "1", "oa_rate": 0.5,
         "oa_status": "gold", "pdf_url": "https://scielo.br/abc"},
        {"source": "openalex", "e_mec_code": "1", "oa_rate": 0.5,
         "oa_status": "gold", "pdf_url": "https://doi.org/10.1"},
    ])
    df.to_csv(csv, index=False)
    enrich_oa_file(csv)
    result = pd.read_csv(csv)
    assert "oa_type" in result.columns
    assert result.iloc[0]["oa_type"] == "diamond"
    assert result.iloc[1]["oa_type"] == "gold"

def test_enrich_oa_file_idempotent(tmp_path):
    csv = tmp_path / "oa_phase2_test.csv"
    df = pd.DataFrame([{"source": "openalex", "oa_status": "gold",
                        "pdf_url": None, "oa_rate": 0.5}])
    df.to_csv(csv, index=False)
    enrich_oa_file(csv)
    enrich_oa_file(csv)  # second call — must not crash or duplicate column
    result = pd.read_csv(csv)
    assert list(result.columns).count("oa_type") == 1
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/enrichment/test_diamond_oa.py -v
```
Expected: FAIL — `ModuleNotFoundError: enrichment.diamond_oa`

- [ ] **Step 3: Create package files and implement diamond_oa.py**

```python
# enrichment/__init__.py
```

```python
# tests/enrichment/__init__.py
```

```python
# enrichment/diamond_oa.py
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
        logger.info("oa_type column already present in %s — skipping", path.name)
        return
    df["oa_type"] = df.apply(
        lambda r: classify_oa(r.get("oa_status"), r.get("pdf_url")), axis=1
    )
    df.to_csv(path, index=False)
    logger.info("Enriched %s with oa_type (%d rows)", path.name, len(df))
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/enrichment/test_diamond_oa.py -v
```
Expected: 8 PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```
Expected: 184+ passed

- [ ] **Step 6: Commit**

```bash
git add enrichment/__init__.py enrichment/diamond_oa.py \
        tests/enrichment/__init__.py tests/enrichment/test_diamond_oa.py
git commit -m "feat(enrichment): add Diamond OA classifier with Scielo/Redalyc heuristics"
```

---

## Task 3: Geographic Bias

**Files:**
- Create: `enrichment/geographic.py`
- Create: `tests/enrichment/test_geographic.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_geographic.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.geographic import (
    compute_coverage_gap, compute_output_gap, compute_geographic_bias_score,
)

_REGISTRY = pd.DataFrame([
    {"e_mec_code": "1", "region": "Norte",   "faculty_with_phd": 100},
    {"e_mec_code": "2", "region": "Norte",   "faculty_with_phd": 200},
    {"e_mec_code": "3", "region": "Sudeste", "faculty_with_phd": 500},
    {"e_mec_code": "4", "region": "Sudeste", "faculty_with_phd": 400},
    {"e_mec_code": "5", "region": "Sul",     "faculty_with_phd": 300},
])
_INDEXED = {"1", "3", "5"}  # source indexed these e_mec codes

def test_coverage_gap_proportional():
    gaps = compute_coverage_gap(_REGISTRY, _INDEXED)
    # Norte: expected=2/5=0.40, observed=1/3=0.333 → gap negative
    assert gaps["Norte"] < 0
    # Sudeste: expected=2/5=0.40, observed=1/3=0.333 → gap negative
    assert "Sudeste" in gaps

def test_coverage_gap_returns_all_regions():
    gaps = compute_coverage_gap(_REGISTRY, _INDEXED)
    assert set(gaps.keys()) == {"Norte", "Sudeste", "Sul"}

def test_output_gap_excludes_zero_phd():
    registry = _REGISTRY.copy()
    registry.loc[0, "faculty_with_phd"] = 0
    pub_counts = {"1": 10, "3": 50, "5": 20}
    gaps = compute_output_gap(registry, pub_counts)
    # e_mec "1" excluded (faculty_with_phd=0), Norte has only e_mec "2" not in indexed
    assert isinstance(gaps, dict)

def test_bias_score_perfect_is_one():
    # All regions perfectly proportional
    registry = pd.DataFrame([
        {"e_mec_code": str(i), "region": r, "faculty_with_phd": 100}
        for i, r in enumerate(["Norte", "Norte", "Sul", "Sul"])
    ])
    indexed = {"0", "2"}  # one per region
    gaps = compute_coverage_gap(registry, indexed)
    score = compute_geographic_bias_score(gaps)
    assert abs(score - 1.0) < 0.01

def test_bias_score_clipped_to_zero_one():
    gaps = {"Norte": -2.0, "Sudeste": 2.0}  # extreme values
    score = compute_geographic_bias_score(gaps)
    assert 0.0 <= score <= 1.0

def test_missing_registry_returns_none():
    from enrichment.geographic import load_and_compute
    result = load_and_compute(registry_path="/nonexistent/path.csv",
                               pub_counts={}, source="openalex")
    assert result is None
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/enrichment/test_geographic.py -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement geographic.py**

```python
# enrichment/geographic.py
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def compute_coverage_gap(registry: pd.DataFrame, indexed: set[str]) -> dict[str, float]:
    """Return {region: observed_rate - expected_rate}. Negative = under-indexed."""
    total = len(registry)
    total_indexed = len(indexed)
    if total == 0 or total_indexed == 0:
        return {}
    result = {}
    for region, grp in registry.groupby("region"):
        expected = len(grp) / total
        observed = len([c for c in grp["e_mec_code"].astype(str) if c in indexed]) / total_indexed
        result[str(region)] = observed - expected
    return result


def compute_output_gap(registry: pd.DataFrame,
                       pub_counts: dict[str, int]) -> dict[str, float]:
    """Return {region: mean(pubs/faculty_with_phd)} excluding zero-faculty rows."""
    result = {}
    valid = registry[registry["faculty_with_phd"].fillna(0) > 0].copy()
    valid["e_mec_str"] = valid["e_mec_code"].astype(str)
    valid["pubs"] = valid["e_mec_str"].map(pub_counts).fillna(0)
    valid["rate"] = valid["pubs"] / valid["faculty_with_phd"]
    for region, grp in valid.groupby("region"):
        result[str(region)] = float(grp["rate"].mean())
    return result


def compute_geographic_bias_score(coverage_gaps: dict[str, float]) -> float:
    """Score 0-1: 1.0 = perfectly proportional, lower = more biased."""
    if not coverage_gaps:
        return 0.0
    raw = 1.0 - sum(abs(v) for v in coverage_gaps.values()) / len(coverage_gaps)
    return max(0.0, min(1.0, raw))


def load_and_compute(registry_path: str, pub_counts: dict[str, int],
                     source: str) -> dict | None:
    """Load registry and return coverage/output gap dict. Returns None if registry absent."""
    path = Path(registry_path)
    if not path.exists():
        logger.warning("Registry not found at %s — geographic_bias skipped for %s", path, source)
        return None
    registry = pd.read_csv(path)
    indexed = set(str(k) for k in pub_counts.keys())
    coverage_gaps = compute_coverage_gap(registry, indexed)
    output_gaps = compute_output_gap(registry, pub_counts)
    bias_score = compute_geographic_bias_score(coverage_gaps)
    return {
        "coverage_gaps": coverage_gaps,
        "output_gaps": output_gaps,
        "geographic_bias_score": bias_score,
    }
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/enrichment/test_geographic.py -v
```
Expected: 6 PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add enrichment/geographic.py tests/enrichment/test_geographic.py
git commit -m "feat(enrichment): add geographic bias coverage gap + output gap module"
```

---

## Task 4: Non-academic Coauthorship + OpenAlex Connector Extension

**Files:**
- Modify: `connectors/api/openalex.py` — `normalize()` method
- Create: `enrichment/coauthorship.py`
- Create: `tests/enrichment/test_coauthorship.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_coauthorship.py
from __future__ import annotations
import pytest
from enrichment.coauthorship import (
    is_nonacademic, compute_coauth_metrics, NON_ACADEMIC_TYPES,
)

def test_non_academic_types_known():
    assert "company" in NON_ACADEMIC_TYPES
    assert "government" in NON_ACADEMIC_TYPES
    assert "education" not in NON_ACADEMIC_TYPES

def test_is_nonacademic_company():
    assert is_nonacademic(["company", "education"]) is True

def test_is_nonacademic_pure_academic():
    assert is_nonacademic(["education"]) is False

def test_is_nonacademic_empty():
    assert is_nonacademic([]) is False

def test_metrics_full_data():
    papers = [
        {"affiliation_types": [["education", "company"]], "ror_resolved": True},
        {"affiliation_types": [["education"]], "ror_resolved": True},
        {"affiliation_types": [["government"]], "ror_resolved": False},
    ]
    m = compute_coauth_metrics(papers)
    assert m["detectability"] == 1.0        # all have affiliation_types
    assert abs(m["volume_rate"] - 2/3) < 0.01  # 2 of 3 have nonacademic
    assert abs(m["quality_score"] - 0.5) < 0.01  # 1 of 2 nonacademic is ROR-resolved

def test_metrics_zero_papers():
    m = compute_coauth_metrics([])
    assert m["detectability"] == 0.0
    assert m["volume_rate"] == 0.0
    assert m["quality_score"] == 0.0

def test_metrics_no_affiliation_types():
    papers = [{"affiliation_types": None, "ror_resolved": False}]
    m = compute_coauth_metrics(papers)
    assert m["detectability"] == 0.0

def test_composite_score_range():
    m = compute_coauth_metrics([
        {"affiliation_types": [["company"]], "ror_resolved": True}
    ])
    score = 0.4 * m["detectability"] + 0.3 * m["volume_rate"] + 0.3 * m["quality_score"]
    assert 0.0 <= score <= 1.0
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/enrichment/test_coauthorship.py -v
```
Expected: FAIL

- [ ] **Step 3: Implement coauthorship.py**

```python
# enrichment/coauthorship.py
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

NON_ACADEMIC_TYPES = {"company", "government", "nonprofit", "facility", "healthcare", "other"}


def is_nonacademic(affil_types: list[str]) -> bool:
    return bool(set(affil_types) & NON_ACADEMIC_TYPES)


def compute_coauth_metrics(papers: list[dict]) -> dict[str, float]:
    """
    Each paper dict expected keys:
      affiliation_types: list[list[str]] | None  (one list of types per author)
      ror_resolved: bool
    Returns: detectability, volume_rate, quality_score, nonacademic_coauth_score
    """
    if not papers:
        return {"detectability": 0.0, "volume_rate": 0.0,
                "quality_score": 0.0, "nonacademic_coauth_score": 0.0}

    n = len(papers)
    n_with_types = sum(1 for p in papers if p.get("affiliation_types"))
    n_nonacademic = sum(
        1 for p in papers
        if p.get("affiliation_types") and
        any(is_nonacademic(a) for a in p["affiliation_types"])
    )
    n_ror_resolved_nonacademic = sum(
        1 for p in papers
        if p.get("affiliation_types") and
        any(is_nonacademic(a) for a in p["affiliation_types"]) and
        p.get("ror_resolved", False)
    )

    detectability = n_with_types / n if n else 0.0
    volume_rate = n_nonacademic / n if n else 0.0
    quality_score = n_ror_resolved_nonacademic / n_nonacademic if n_nonacademic else 0.0

    composite = 0.4 * detectability + 0.3 * volume_rate + 0.3 * quality_score
    return {
        "detectability": detectability,
        "volume_rate": volume_rate,
        "quality_score": quality_score,
        "nonacademic_coauth_score": composite,
    }
```

- [ ] **Step 4: Extend OpenAlex `normalize()` to include `affiliation_types`**

In `connectors/api/openalex.py`, inside `normalize()`, update the authors extraction to include `affiliation_types`:

```python
# In the authors loop inside normalize():
"affiliation_types": list({
    inst.get("type", "unknown")
    for inst in authorship.get("institutions", [])
    if inst.get("type")
}),
```

And set `ror_resolved` per paper as:
```python
"ror_resolved": any(
    inst.get("id") is not None
    for authorship in raw.get("authorships", [])
    for inst in authorship.get("institutions", [])
)
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/enrichment/test_coauthorship.py tests/connectors/ -v
```
Expected: all PASS; existing connector tests unchanged

- [ ] **Step 6: Run full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add enrichment/coauthorship.py tests/enrichment/test_coauthorship.py \
        connectors/api/openalex.py
git commit -m "feat(enrichment): non-academic coauthorship metrics + OpenAlex affiliation_types"
```

---

## Task 5: SDG Coverage Module + Connector Extensions

**Files:**
- Modify: `connectors/api/openalex.py` — `normalize()` SDG field
- Modify: `connectors/api/dimensions.py` — `normalize()` SDG field
- Modify: `connectors/api/scopus.py` — SDG attempt + source_metadata.json flag
- Create: `enrichment/sdg.py`
- Create: `tests/enrichment/test_sdg.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_sdg.py
from __future__ import annotations
import json, os
import pandas as pd
import pytest
from enrichment.sdg import (
    compute_sdg_rates, compute_sdg_agreement, write_sdg_flag,
    SDG_LABELS,
)

_PAPERS_OA = [
    {"id": "W1", "sdgs": [3, 4]},
    {"id": "W2", "sdgs": [4]},
    {"id": "W3", "sdgs": []},
]
_PAPERS_DIM = [
    {"id": "D1", "sdgs": [3]},
    {"id": "D2", "sdgs": [4, 10]},
    {"id": "D3", "sdgs": [4]},
]

def test_sdg_rates_goal_present():
    rates = compute_sdg_rates(_PAPERS_OA)
    assert abs(rates[4]["rate"] - 2/3) < 0.01   # SDG 4 in 2 of 3 papers
    assert rates[3]["rate"] == pytest.approx(1/3)

def test_sdg_rates_missing_goal_is_zero():
    rates = compute_sdg_rates(_PAPERS_OA)
    assert rates.get(1, {}).get("rate", 0.0) == 0.0

def test_sdg_rates_all_goals_present():
    rates = compute_sdg_rates(_PAPERS_OA)
    # At minimum goals mentioned in papers should be present
    assert 3 in rates and 4 in rates

def test_sdg_agreement_on_matched():
    # W1→D1 matched; both tagged SDG3 → agreement
    matched = [{"id_a": "W1", "id_b": "D1"}]
    oa_map = {"W1": {3, 4}, "W2": {4}}
    dim_map = {"D1": {3}, "D2": {4, 10}}
    agreement = compute_sdg_agreement(matched, oa_map, dim_map)
    assert agreement[3]["agreement_rate"] == pytest.approx(1.0)

def test_sdg_agreement_empty_matched():
    agreement = compute_sdg_agreement([], {}, {})
    assert agreement == {}

def test_sdg_labels_covers_all_goals():
    for g in range(1, 18):
        assert g in SDG_LABELS, f"SDG goal {g} missing from SDG_LABELS"

def test_write_sdg_flag_creates_file(tmp_path):
    path = tmp_path / "source_metadata.json"
    write_sdg_flag(path, "scopus", available=False)
    data = json.loads(path.read_text())
    assert data["scopus"]["sdg_available"] is False

def test_write_sdg_flag_is_atomic_and_merges(tmp_path):
    path = tmp_path / "source_metadata.json"
    path.write_text('{"openalex": {"sdg_available": true}}')
    write_sdg_flag(path, "scopus", available=False)
    data = json.loads(path.read_text())
    assert data["openalex"]["sdg_available"] is True
    assert data["scopus"]["sdg_available"] is False
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/enrichment/test_sdg.py -v
```
Expected: FAIL

- [ ] **Step 3: Implement enrichment/sdg.py**

```python
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
    10: "Reduced Inequalities", 11: "Sustainable Cities", 12: "Responsible Consumption",
    13: "Climate Action", 14: "Life Below Water", 15: "Life on Land",
    16: "Peace & Justice", 17: "Partnerships",
}


def compute_sdg_rates(papers: list[dict]) -> dict[int, dict]:
    """
    papers: list of dicts with 'sdgs': list[int]
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
    """
    matched: list of {id_a: openalex_id, id_b: dimensions_id}
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
    logger.info("source_metadata.json updated: %s sdg_available=%s", source, available)
```

- [ ] **Step 4: Add SDG parsing to OpenAlex `normalize()`**

In `connectors/api/openalex.py`, inside `normalize()`, add:
```python
"sdgs": [
    int(sdg.get("id", "").split("/")[-1].replace("sdg-", ""))
    for sdg in raw.get("sustainable_development_goals") or []
    if sdg.get("score", 0) >= 0.4  # OpenAlex confidence threshold
],
```
(The `sustainable_development_goals` field already in the select list — just parse it.)

- [ ] **Step 5: Add SDG parsing to Dimensions `normalize()`**

In `connectors/api/dimensions.py`, inside `normalize()`, add:
```python
"sdgs": [
    int(s.strip().split(" ")[0])  # Dimensions format: "3 Good Health and Well-Being"
    for s in (raw.get("category_sdg") or {}).get("name", [])
    if s.strip()
],
```

- [ ] **Step 6: Add Scopus SDG attempt**

In `connectors/api/scopus.py`, inside `normalize()`, add:
```python
sdg_raw = raw.get("sdg") or []
if not sdg_raw:
    logger.warning("Scopus SDG field not available — sdg_coverage will be 0.0 for scopus")
"sdgs": [],  # Scopus does not expose SDG via standard API; use source_metadata flag
```
The `write_sdg_flag` call happens in `run_enrichment.py` after checking if any Scopus records have sdgs populated.

- [ ] **Step 7: Run SDG tests**

```bash
pytest tests/enrichment/test_sdg.py -v
```
Expected: 8 PASS

- [ ] **Step 8: Run full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 9: Commit**

```bash
git add enrichment/sdg.py tests/enrichment/test_sdg.py \
        connectors/api/openalex.py connectors/api/dimensions.py \
        connectors/api/scopus.py
git commit -m "feat(enrichment): SDG rates + convergence agreement + Scopus flag"
```

---

## Task 6: Wire New Sub-dimensions into Fitness Scorer

**Files:**
- Modify: `scoring/fitness.py` — `build_profile()` + 4 scoring methods
- Modify: `run_fitness.py` — `_load_dedup_scores()` + pass new kwargs
- Modify: `tests/scoring/test_fitness.py` — ≥4 new tests

- [ ] **Step 1: Write failing tests**

Add to `tests/scoring/test_fitness.py`:

```python
def test_dedup_score_wired():
    scorer = FitnessScorer()
    profile = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        dedup_score=0.85,
    )
    assert profile.data_quality > 0.0

def test_sdg_rate_wired():
    scorer = FitnessScorer()
    profile_with = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        sdg_rate=0.40,
    )
    profile_without = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        sdg_rate=0.0,
    )
    assert profile_with.social_impact > profile_without.social_impact

def test_diamond_oa_rate_wired():
    scorer = FitnessScorer()
    p = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        diamond_oa_rate=0.30,
    )
    assert 0.0 <= p.social_impact <= 1.0

def test_nonacademic_coauth_wired():
    scorer = FitnessScorer()
    p_high = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        nonacademic_coauth=0.80,
    )
    p_low = scorer.build_profile(
        "openalex", "federal_university",
        coverage={"institutional_coverage": 0.8, "field_coverage": 0.7,
                  "temporal_coverage": 0.9, "language_coverage": 0.6},
        oa={"oa_rate": 0.5},
        convergence={"inter_source_agreement": 0.7, "doi_rate": 0.9},
        nonacademic_coauth=0.0,
    )
    assert p_high.innovation_link > p_low.innovation_link
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/scoring/test_fitness.py::test_dedup_score_wired \
       tests/scoring/test_fitness.py::test_sdg_rate_wired -v
```
Expected: FAIL — `build_profile()` does not accept `dedup_score` kwarg

- [ ] **Step 3: Update `scoring/fitness.py`**

**`build_profile()` — add new kwargs (after `patents=None`):**
```python
dedup_score: float = 0.0,
sdg_rate: float = 0.0,
diamond_oa_rate: float = 0.0,
geographic_bias: float | None = None,   # None = registry absent (excluded); 0.0 = computed as zero
nonacademic_coauth: float = 0.0,
```

**`_score_coverage()` — add `geographic_bias` sub-dim:**

Use `None` to mean "registry absent, exclude from total_w" vs `0.0` meaning "registry present, source scored zero (maximally biased)":

```python
def _score_coverage(self, cov: dict, geographic_bias: float | None = None) -> float:
    sub_w = self.sub_w.get("coverage", {})
    total_w = 0.0
    score = 0.0
    for key in ("institutional_coverage", "field_coverage",
                "temporal_coverage", "language_coverage"):
        w = sub_w.get(key, 0.0)
        score += w * float(cov.get(key, 0.0))
        total_w += w
    if geographic_bias is not None:   # None = absent (skip); 0.0 = present, scored zero
        w = sub_w.get("geographic_bias", 0.0)
        score += w * geographic_bias
        total_w += w
    return score / total_w if total_w else 0.0
```

**`_score_data_quality()` — add `dedup_score`:**
```python
def _score_data_quality(self, cov: dict, convergence: dict,
                        source_id: str, dedup_score: float = 0.0) -> float:
```
Inside the method, after the existing `timeliness` line, add:
```python
if dedup_score > 0.0:
    w = sub_w.get("deduplication", 0.0)
    score += w * dedup_score
    total_w += w
```

**`_score_social_impact()` — add `sdg_rate` and `diamond_oa_rate`:**
```python
def _score_social_impact(self, oa: dict, static: dict,
                         sdg_rate: float = 0.0,
                         diamond_oa_rate: float = 0.0) -> float:
```
Add `sdg_coverage` and `diamond_oa` into the sub-weight loop — same conditional pattern as dedup_score above.

**`_score_innovation_link()` — add `nonacademic_coauth`:**
```python
def _score_innovation_link(self, patents: dict | None,
                           nonacademic_coauth: float = 0.0) -> float:
```
Add `nonacademic_coauth` sub-weight contribution with same conditional guard.

**Wire through `build_profile()`:**
Pass new kwargs to each scoring call:
```python
coverage_score  = self._score_coverage(coverage, geographic_bias=geographic_bias)
dq_score        = self._score_data_quality(coverage, convergence, source_id, dedup_score=dedup_score)
si_score        = self._score_social_impact(oa, static, sdg_rate=sdg_rate, diamond_oa_rate=diamond_oa_rate)
innov_score     = self._score_innovation_link(patents, nonacademic_coauth=nonacademic_coauth)
```

- [ ] **Step 4: Update `run_fitness.py` — add `_load_dedup_scores()`**

Add after the existing `_load_oa()` function:
```python
def _load_dedup_scores(csv_dir: Path) -> dict[str, float]:
    """Return {source: mean overlap_pct_min across all pairs involving that source}."""
    files = sorted(csv_dir.glob("overlap_phase2_*.csv"))
    if not files:
        return {}
    df = pd.read_csv(files[-1])  # most recent
    scores: dict[str, float] = {}
    for source in pd.unique(df[["source_a", "source_b"]].values.ravel()):
        mask = (df["source_a"] == source) | (df["source_b"] == source)
        scores[str(source)] = float(df.loc[mask, "overlap_pct_min"].mean())
    return scores
```

Call it in `main()` and pass `dedup_score=dedup_scores.get(source_id, 0.0)` to `build_profile()`.

- [ ] **Step 5: Run scorer tests**

```bash
pytest tests/scoring/test_fitness.py -v
```
Expected: all existing + 4 new PASS

- [ ] **Step 6: Run full suite**

```bash
pytest tests/ -q
```
Expected: 200+ passed

- [ ] **Step 7: Commit**

```bash
git add scoring/fitness.py run_fitness.py tests/scoring/test_fitness.py
git commit -m "feat(scoring): wire dedup, sdg, diamond_oa, geographic_bias, nonacademic_coauth into scorer"
```

---

## Task 7: `run_enrichment.py` CLI + Full Integration Test

**Files:**
- Create: `run_enrichment.py`
- No new test file — integration validated by running against real phase 2 outputs

- [ ] **Step 1: Implement `run_enrichment.py`**

```python
# run_enrichment.py
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

from enrichment.coauthorship import compute_coauth_metrics
from enrichment.diamond_oa import enrich_oa_file
from enrichment.geographic import load_and_compute as geo_compute
from enrichment.sdg import compute_sdg_rates, write_sdg_flag

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)

_PROCESSED = Path("data/processed")
_REGISTRY  = Path("registry/institutions.csv")
_METADATA  = _PROCESSED / "source_metadata.json"


def _resolve_date(date_str: str | None, processed: Path) -> str:
    if date_str:
        return date_str
    files = sorted(processed.glob("coverage_phase2_*.csv"), key=lambda f: f.stat().st_mtime)
    if not files:
        logger.error("No phase 2 outputs found. Run run_phase2.py first.")
        sys.exit(1)
    return files[-1].stem.split("phase2_")[-1]


def _pub_counts_from_coverage(coverage_csv: Path, source: str) -> dict[str, int]:
    """Return {e_mec_code: n_records} for the given source from coverage CSV.
    coverage_phase2_*.csv columns include: source, e_mec_code, n_records.
    """
    df = pd.read_csv(coverage_csv)
    subset = df[df["source"] == source][["e_mec_code", "n_records"]].dropna()
    return dict(zip(subset["e_mec_code"].astype(str), subset["n_records"].astype(int)))


def _fetch_papers_for_enrichment(source: str, e_mec_codes: list[str],
                                  config: dict) -> list[dict]:
    """Fetch normalised paper records from source connector for enrichment.

    Returns list of normalised paper dicts (each has 'sdgs', 'affiliation_types',
    'ror_resolved' fields after connector extensions in Tasks 4 and 5).
    Returns [] if connector not available or API key missing.
    """
    import os
    from connectors.api.openalex import OpenAlexConnector
    from connectors.api.dimensions import DimensionsConnector

    spotlight = config.get("spotlight", [])
    papers: list[dict] = []

    if source == "openalex":
        conn = OpenAlexConnector(email=os.getenv("OPENALEX_EMAIL"),
                                 max_records=None)
        for inst in spotlight:
            if str(inst.get("e_mec_code")) not in e_mec_codes:
                continue
            ror = inst.get("ror_id", "")
            if not ror:
                continue
            try:
                records = conn.fetch(ror_id=ror)
                papers.extend(conn.normalize(r) for r in records)
            except Exception as exc:
                logger.warning("OpenAlex fetch failed for %s: %s", ror, exc)

    elif source == "dimensions":
        conn = DimensionsConnector(max_records=None)
        for inst in spotlight:
            if str(inst.get("e_mec_code")) not in e_mec_codes:
                continue
            grid = inst.get("grid_id", "")
            if not grid:
                continue
            try:
                records = conn.fetch(grid_id=grid)
                papers.extend(conn.normalize(r) for r in records)
            except Exception as exc:
                logger.warning("Dimensions fetch failed for %s: %s", grid, exc)

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

    # 3. Coauthorship (OpenAlex API re-query with affiliation_types field)
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
                            "sdg_goal": goal, "sdg_label": f"SDG {goal}",
                            **data,
                        })
            # Scopus: flag as unavailable
            write_sdg_flag(_METADATA, "scopus", available=False)
            if sdg_rows:
                out = _PROCESSED / f"sdg_by_source_type_{date}.csv"
                pd.DataFrame(sdg_rows).to_csv(out, index=False)
                logger.info("Wrote %s", out)

    logger.info("Enrichment complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-test with existing phase 2 outputs**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python run_enrichment.py --skip-coauth --skip-sdg 2>&1
```
Expected: INFO logs, `Enrichment complete.` — no errors

- [ ] **Step 3: Verify output files were created**

```bash
python3 -c "
import pandas as pd, glob, os

# Check oa_type column added
oa_files = sorted(glob.glob('data/processed/oa_phase2_*.csv'))
if oa_files:
    df = pd.read_csv(oa_files[-1])
    assert 'oa_type' in df.columns, 'oa_type column missing'
    print('oa_type values:', df['oa_type'].value_counts().to_dict())

# Check geographic CSV written
geo_files = sorted(glob.glob('data/processed/geographic_coverage_*.csv'))
assert geo_files, 'geographic_coverage_*.csv not written'
df = pd.read_csv(geo_files[-1])
assert 'coverage_gap' in df.columns, 'coverage_gap column missing'
print('geographic rows:', len(df))
print('all output checks OK')
"
```
Expected: `oa_type` present, `geographic_coverage_*.csv` written with `coverage_gap` column

- [ ] **Step 4: Run full test suite — final check**

```bash
pytest tests/ -v 2>&1 | tail -20
```
Expected: 200+ passed, 1 skipped

- [ ] **Step 5: Final commit**

```bash
git add run_enrichment.py
git commit -m "feat: add run_enrichment.py CLI — diamond OA + geographic bias + SDG flag"
```

---

---

## Task 8: Convergence Report Extension

**Files:**
- Modify: `outputs/dataset/exporter.py` — add SDG agreement + geographic coverage sections to phase 2 Markdown report

- [ ] **Step 1: Locate the report generation section in exporter.py**

```bash
grep -n "def export_phase2_report\|## Overlap\|## Divergence" \
  outputs/dataset/exporter.py | head -20
```
Find the method that writes the phase 2 Markdown report and the line where the report ends.

- [ ] **Step 2: Add enrichment sections to the report**

Inside the phase 2 report method (after the existing divergence table section), add:

```python
# SDG agreement section (conditional on enrichment file presence)
import json
from pathlib import Path

_PROCESSED = Path("data/processed")
metadata_path = _PROCESSED / "source_metadata.json"
sdg_files = sorted(_PROCESSED.glob("sdg_by_source_type_*.csv"))
if sdg_files:
    sdg_df = pd.read_csv(sdg_files[-1])
    lines.append("\n## SDG Coverage by Source\n")
    lines.append(sdg_df.to_markdown(index=False))
    lines.append("")

# Scopus SDG caveat
if metadata_path.exists():
    try:
        meta = json.loads(metadata_path.read_text())
        if not meta.get("scopus", {}).get("sdg_available", True):
            lines.append(
                "\n> ⚠ **Scopus SDG data not available** via standard API "
                "(requires SciVal). `sdg_coverage` scored as 0.0 for Scopus.\n"
            )
    except json.JSONDecodeError:
        pass

# Geographic coverage section
geo_files = sorted(_PROCESSED.glob("geographic_coverage_*.csv"))
if geo_files:
    geo_df = pd.read_csv(geo_files[-1])
    lines.append("\n## Geographic Coverage Gap by Source\n")
    lines.append(geo_df.to_markdown(index=False))
    lines.append("")
```

- [ ] **Step 3: Verify report renders without error on existing outputs**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
# Dry-run: import the exporter and confirm no syntax errors
from outputs.dataset.exporter import DatasetExporter
print('exporter imports OK')
"
```
Expected: `exporter imports OK`

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/ -q
```
Expected: all pass (report sections are conditional — no-op if enrichment files absent)

- [ ] **Step 5: Commit**

```bash
git add outputs/dataset/exporter.py
git commit -m "feat(report): add SDG agreement and geographic coverage sections to phase 2 report"
```

---

## Summary

After all 8 tasks:

| What | State |
|---|---|
| 2023 uncapped config | ✅ Live |
| max_records None guard | ✅ Both connectors |
| YAML weights rebalanced | ✅ Verified sum=1.0 |
| Diamond OA classifier | ✅ Tested, in-place, idempotent |
| Geographic bias | ✅ Coverage gap + output gap |
| Coauthorship metrics | ✅ Detectability + volume + quality |
| SDG rates + agreement | ✅ Both sources; Scopus flagged |
| Fitness scorer | ✅ All 5 new sub-dims wired |
| run_enrichment.py | ✅ CLI with per-module skip flags, real API calls |
| Convergence report | ✅ SDG + geographic sections (conditional) |
| Tests | ✅ ≥30 new, all 176+ existing pass |
