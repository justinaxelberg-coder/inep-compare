# Stratified Enrichment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Fresh subagent per task, two-stage spec + quality review between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute every fitness sub-dimension as a (source × inst_type × region) stratified matrix, add a Crossref metadata-validator connector, measure source sensitivity against the OpenAlex baseline, and surface all results in the Dashboard Enrichment tab.

**Architecture:** All enrichment modules write to a unified `(source|inst_type|region|sub_dimension|value|n_papers|confidence_tier)` CSV schema. A new `enrichment/stratified.py` provides shared utilities (stratum extraction, confidence tier assignment, CSV write). The fitness scorer and dashboard read these CSVs as optional enrichment — graceful no-op when absent. The Crossref connector is lightweight (no API key, free) and used only as a metadata validator, not a scored source.

**Tech Stack:** Python 3.11+, pandas, httpx, pytest, PyYAML. No new dependencies.

**Guiding principle:** Every sub-dimension answers "how well does this source cover *this kind of institution in this region*" — not a national average.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `enrichment/stratified.py` | Create | Shared stratum utilities, unified CSV schema, confidence tier logic |
| `connectors/api/crossref.py` | Create | Lightweight metadata validator: funder, license, doc type, ROR coverage |
| `tests/connectors/test_crossref.py` | Create | ≥8 tests |
| `enrichment/sensitivity.py` | Create | OpenAlex-baseline recall per (source × inst_type × region) |
| `tests/enrichment/test_sensitivity.py` | Create | ≥6 tests |
| `enrichment/geographic.py` | Modify | Add inst_type stratification (currently region-only) |
| `tests/enrichment/test_geographic.py` | Modify | Update for inst_type strat |
| `enrichment/disambiguation.py` | Create | ROR resolution rate stratified |
| `tests/enrichment/test_disambiguation.py` | Create | ≥6 tests |
| `enrichment/funder.py` | Create | Funder metadata completeness + Brazilian funder detection |
| `tests/enrichment/test_funder.py` | Create | ≥6 tests |
| `enrichment/policy_docs.py` | Create | Policy document rate stratified |
| `tests/enrichment/test_policy_docs.py` | Create | ≥5 tests |
| `enrichment/coauthorship.py` | Modify | Add inst_type × region stratification |
| `tests/enrichment/test_coauthorship.py` | Modify | Update for strat output |
| `enrichment/sdg.py` | Modify | Add inst_type × region stratification |
| `tests/enrichment/test_sdg.py` | Modify | Update for strat output |
| `enrichment/patents.py` | Create | Patent link rate stratified (Lens + Crossref) |
| `tests/enrichment/test_patents.py` | Create | ≥5 tests |
| `run_enrichment.py` | Modify | Add all new modules, write stratified CSVs |
| `scoring/fitness.py` | Modify | `build_profile()` reads stratified CSVs as optional kwargs |
| `run_fitness.py` | Modify | Load stratified outputs, pass to scorer |
| `dashboard/data_loader.py` | Modify | `load_sensitivity()`, `load_metadata_quality()`, `load_sdg_stratified()`, `load_enrichment_combined()` |
| `dashboard/tabs/enrichment.py` | Create | Tab 4: geographic bias, sensitivity, metadata quality, SDG, Scopus caveat |
| `tests/dashboard/test_enrichment_tab.py` | Create | ≥8 smoke tests |

---

## Sprint 1: Data Infrastructure + Crossref Connector

### Task 1.1: Stratified Output Utilities

**Files:**
- Create: `enrichment/stratified.py`
- Create: `tests/enrichment/test_stratified.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_stratified.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.stratified import (
    STRATIFIED_SCHEMA, assign_confidence_tier, make_stratum_row,
    write_stratified_csv, load_stratified_csv,
)

def test_schema_columns():
    assert STRATIFIED_SCHEMA == [
        "source", "inst_type", "region", "sub_dimension",
        "value", "n_papers", "confidence_tier",
    ]

def test_confidence_tier_reliable():
    assert assign_confidence_tier(250) == "reliable"

def test_confidence_tier_moderate():
    assert assign_confidence_tier(75) == "moderate"

def test_confidence_tier_low():
    assert assign_confidence_tier(25) == "low"

def test_confidence_tier_insufficient():
    assert assign_confidence_tier(5) == "insufficient"

def test_make_stratum_row():
    row = make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312)
    assert row["value"] == 0.85
    assert row["confidence_tier"] == "reliable"

def test_write_and_load_roundtrip(tmp_path):
    rows = [
        make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312),
        make_stratum_row("scopus",   "isolated_faculty",   "Norte",   "sensitivity", 0.41,  47),
    ]
    path = tmp_path / "sensitivity_2026-03-24.csv"
    write_stratified_csv(rows, path)
    df = load_stratified_csv(path)
    assert len(df) == 2
    assert list(df.columns) == STRATIFIED_SCHEMA

def test_write_idempotent(tmp_path):
    rows = [make_stratum_row("openalex", "federal_university", "Sudeste", "sensitivity", 0.85, 312)]
    path = tmp_path / "test.csv"
    write_stratified_csv(rows, path)
    write_stratified_csv(rows, path)  # second write should overwrite, not append
    df = load_stratified_csv(path)
    assert len(df) == 1
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
pytest tests/enrichment/test_stratified.py -v
```
Expected: FAIL — `ModuleNotFoundError: enrichment.stratified`

- [ ] **Step 3: Implement `enrichment/stratified.py`**

```python
# enrichment/stratified.py
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

STRATIFIED_SCHEMA: list[str] = [
    "source", "inst_type", "region", "sub_dimension",
    "value", "n_papers", "confidence_tier",
]

_TIERS = [(200, "reliable"), (50, "moderate"), (10, "low"), (0, "insufficient")]


def assign_confidence_tier(n: int) -> str:
    for threshold, tier in _TIERS:
        if n >= threshold:
            return tier
    return "insufficient"


def make_stratum_row(
    source: str, inst_type: str, region: str,
    sub_dimension: str, value: float, n_papers: int,
) -> dict:
    return {
        "source": source,
        "inst_type": inst_type,
        "region": region,
        "sub_dimension": sub_dimension,
        "value": float(value),
        "n_papers": int(n_papers),
        "confidence_tier": assign_confidence_tier(n_papers),
    }


def write_stratified_csv(rows: list[dict], path: Path) -> None:
    """Write stratified rows to CSV, overwriting if exists."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=STRATIFIED_SCHEMA)
    df.to_csv(path, index=False)
    logger.info("Wrote %d stratified rows to %s", len(df), path.name)


def load_stratified_csv(path: Path) -> pd.DataFrame:
    """Load a stratified CSV; returns empty DataFrame if file absent."""
    path = Path(path)
    if not path.exists():
        logger.warning("Stratified CSV not found: %s", path)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.read_csv(path)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/enrichment/test_stratified.py -v
```
Expected: 8 PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add enrichment/stratified.py tests/enrichment/test_stratified.py
git commit -m "feat(enrichment): add stratified output schema and utilities"
```

---

### Task 1.2: Crossref Connector

**Files:**
- Create: `connectors/api/crossref.py`
- Create: `tests/connectors/test_crossref.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/connectors/test_crossref.py
from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from connectors.api.crossref import CrossrefConnector

_SAMPLE_WORK = {
    "DOI": "10.1234/test",
    "type": "journal-article",
    "funder": [{"name": "CNPq", "DOI": "10.13039/501100003593"}],
    "license": [{"URL": "https://creativecommons.org/licenses/by/4.0/"}],
    "author": [
        {"given": "Maria", "family": "Silva",
         "affiliation": [{"name": "UFPA", "id": [{"id": "https://ror.org/03q9sr818", "id-type": "ROR"}]}]}
    ],
}

def test_init_no_key():
    conn = CrossrefConnector()
    assert conn.source_id == "crossref"

def test_has_funder_true():
    conn = CrossrefConnector()
    assert conn.has_funder(_SAMPLE_WORK) is True

def test_has_funder_false():
    conn = CrossrefConnector()
    assert conn.has_funder({"DOI": "10.1/x", "type": "journal-article"}) is False

def test_has_license():
    conn = CrossrefConnector()
    assert conn.has_license(_SAMPLE_WORK) is True

def test_has_ror_affiliation():
    conn = CrossrefConnector()
    assert conn.has_ror_affiliation(_SAMPLE_WORK) is True

def test_no_ror_affiliation():
    work = {**_SAMPLE_WORK, "author": [{"given": "A", "family": "B", "affiliation": [{"name": "UFPA"}]}]}
    conn = CrossrefConnector()
    assert conn.has_ror_affiliation(work) is False

def test_is_brazilian_funder():
    conn = CrossrefConnector()
    assert conn.is_brazilian_funder("CNPq") is True
    assert conn.is_brazilian_funder("NIH") is False

def test_validate_doi_returns_dict():
    conn = CrossrefConnector()
    with patch.object(conn, "_get_work", return_value=_SAMPLE_WORK):
        result = conn.validate_doi("10.1234/test")
    assert result["doi"] == "10.1234/test"
    assert result["funder_present"] is True
    assert result["license_present"] is True
    assert result["ror_affiliation_present"] is True
    assert result["brazilian_funder"] is True
    assert result["document_type"] == "journal-article"

def test_validate_doi_missing_returns_none():
    conn = CrossrefConnector()
    with patch.object(conn, "_get_work", return_value=None):
        assert conn.validate_doi("10.9999/missing") is None
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/connectors/test_crossref.py -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `connectors/api/crossref.py`**

```python
# connectors/api/crossref.py
from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://api.crossref.org/works"
_MAILTO = "justin.axelberg@usp.br"  # polite pool

_BR_FUNDERS = {
    "cnpq", "capes", "fapesp", "fapemig", "faperj", "finep",
    "fundação de amparo", "conselho nacional", "coordenação de aperfeiçoamento",
}


class CrossrefConnector:
    """Lightweight Crossref metadata validator. No API key required.

    Role: validates funder presence, license declaration, document type,
    and ROR affiliation coverage for a set of DOIs. Not a scored source.
    """

    source_id = "crossref"

    def __init__(self, email: str = _MAILTO, rate_limit_seconds: float = 1.0) -> None:
        self.email = email
        self.rate_limit = rate_limit_seconds

    def _get_work(self, doi: str) -> dict | None:
        url = f"{_BASE}/{doi}"
        try:
            r = httpx.get(url, params={"mailto": self.email}, timeout=10)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            time.sleep(self.rate_limit)
            return r.json().get("message", {})
        except Exception as exc:
            logger.warning("Crossref lookup failed for %s: %s", doi, exc)
            return None

    def has_funder(self, work: dict) -> bool:
        return bool(work.get("funder"))

    def has_license(self, work: dict) -> bool:
        return bool(work.get("license"))

    def has_ror_affiliation(self, work: dict) -> bool:
        for author in work.get("author") or []:
            for aff in author.get("affiliation") or []:
                for id_entry in aff.get("id") or []:
                    if id_entry.get("id-type") == "ROR":
                        return True
        return False

    def is_brazilian_funder(self, funder_name: str) -> bool:
        lower = funder_name.lower()
        return any(br in lower for br in _BR_FUNDERS)

    def validate_doi(self, doi: str) -> dict | None:
        """Return metadata quality dict for one DOI, or None if not found."""
        work = self._get_work(doi)
        if work is None:
            return None
        funders = work.get("funder") or []
        return {
            "doi": doi,
            "funder_present": self.has_funder(work),
            "brazilian_funder": any(
                self.is_brazilian_funder(f.get("name", "")) for f in funders
            ),
            "license_present": self.has_license(work),
            "ror_affiliation_present": self.has_ror_affiliation(work),
            "document_type": work.get("type"),
        }

    def validate_batch(self, dois: list[str]) -> list[dict]:
        """Validate a list of DOIs. Returns only successful results."""
        results = []
        for doi in dois:
            result = self.validate_doi(doi)
            if result:
                results.append(result)
        return results
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/connectors/test_crossref.py -v
```
Expected: 9 PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add connectors/api/crossref.py tests/connectors/test_crossref.py
git commit -m "feat(connectors): add Crossref metadata validator connector"
```

---

## Sprint 2: Coverage & Sensitivity

### Task 2.1: Sensitivity Module (OpenAlex Baseline Recall)

**Files:**
- Create: `enrichment/sensitivity.py`
- Create: `tests/enrichment/test_sensitivity.py`

Context: Sensitivity = papers found by source / papers found by OpenAlex for the same (inst_type × region) stratum. OpenAlex is the denominator. A value of 1.0 = source finds everything OpenAlex finds. Computed from the coverage CSVs already on disk (source × e_mec_code × n_records), joined with crosswalk for inst_type and region.

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_sensitivity.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.sensitivity import (
    compute_sensitivity, aggregate_by_stratum, build_sensitivity_rows,
)

# coverage_df: source, e_mec_code, n_records
_COV = pd.DataFrame([
    {"source": "openalex",   "e_mec_code": "4925", "n_records": 800},
    {"source": "scopus",     "e_mec_code": "4925", "n_records": 560},
    {"source": "dimensions", "e_mec_code": "4925", "n_records": 620},
    {"source": "openalex",   "e_mec_code": "1810", "n_records": 450},
    {"source": "scopus",     "e_mec_code": "1810", "n_records": 180},
    {"source": "dimensions", "e_mec_code": "1810", "n_records": 200},
])

# crosswalk: e_mec_code, inst_type, region
_XW = pd.DataFrame([
    {"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"},
    {"e_mec_code": "1810", "inst_type": "federal_institute",  "region": "Sudeste"},
])

def test_sensitivity_ratio():
    result = compute_sensitivity(_COV, _XW)
    oa_fed = result[(result["source"] == "scopus") &
                    (result["inst_type"] == "federal_university")]
    assert abs(float(oa_fed["sensitivity"].iloc[0]) - 560/800) < 0.01

def test_openalex_sensitivity_is_one():
    result = compute_sensitivity(_COV, _XW)
    oa_rows = result[result["source"] == "openalex"]
    assert (oa_rows["sensitivity"] == 1.0).all()

def test_missing_e_mec_in_crosswalk_excluded():
    cov = _COV.copy()
    cov = pd.concat([cov, pd.DataFrame([{"source": "openalex", "e_mec_code": "9999", "n_records": 100}])])
    result = compute_sensitivity(cov, _XW)
    assert "9999" not in result["e_mec_code"].values

def test_aggregate_by_stratum():
    result = compute_sensitivity(_COV, _XW)
    agg = aggregate_by_stratum(result)
    assert "inst_type" in agg.columns
    assert "region" in agg.columns

def test_build_sensitivity_rows_schema():
    result = compute_sensitivity(_COV, _XW)
    agg = aggregate_by_stratum(result)
    rows = build_sensitivity_rows(agg)
    assert all("sub_dimension" in r for r in rows)
    assert all(r["sub_dimension"] == "sensitivity" for r in rows)

def test_no_openalex_returns_empty():
    cov_no_oa = _COV[_COV["source"] != "openalex"].copy()
    result = compute_sensitivity(cov_no_oa, _XW)
    assert result.empty
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/enrichment/test_sensitivity.py -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `enrichment/sensitivity.py`**

```python
# enrichment/sensitivity.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)


def compute_sensitivity(
    coverage_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute per-(source × e_mec_code) sensitivity using OpenAlex as denominator.

    coverage_df columns: source, e_mec_code, n_records
    crosswalk_df columns: e_mec_code, inst_type, region

    Returns DataFrame with columns:
        source, e_mec_code, inst_type, region, n_source, n_openalex, sensitivity
    """
    if "openalex" not in coverage_df["source"].values:
        logger.warning("OpenAlex not in coverage data — sensitivity cannot be computed")
        return pd.DataFrame()

    oa = (coverage_df[coverage_df["source"] == "openalex"]
          [["e_mec_code", "n_records"]]
          .rename(columns={"n_records": "n_openalex"}))

    # Merge crosswalk
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    oa["e_mec_code"] = oa["e_mec_code"].astype(str)

    base = oa.merge(xw, on="e_mec_code", how="inner")

    rows = []
    for source in coverage_df["source"].unique():
        src = coverage_df[coverage_df["source"] == source][["e_mec_code", "n_records"]].copy()
        src["e_mec_code"] = src["e_mec_code"].astype(str)
        merged = base.merge(src, on="e_mec_code", how="left")
        merged["n_source"] = merged["n_records"].fillna(0).astype(int)
        merged["sensitivity"] = merged.apply(
            lambda r: 1.0 if source == "openalex" else (
                min(1.0, r["n_source"] / r["n_openalex"]) if r["n_openalex"] > 0 else 0.0
            ),
            axis=1,
        )
        merged["source"] = source
        rows.append(merged[["source", "e_mec_code", "inst_type", "region",
                             "n_source", "n_openalex", "sensitivity"]])

    return pd.concat(rows, ignore_index=True)


def aggregate_by_stratum(sensitivity_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-institution sensitivity into (source × inst_type × region) means."""
    if sensitivity_df.empty:
        return pd.DataFrame()
    return (sensitivity_df
            .groupby(["source", "inst_type", "region"])
            .agg(
                sensitivity=("sensitivity", "mean"),
                n_papers=("n_openalex", "sum"),
            )
            .reset_index())


def build_sensitivity_rows(agg_df: pd.DataFrame) -> list[dict]:
    """Convert aggregated sensitivity DataFrame to stratified schema rows."""
    rows = []
    for _, r in agg_df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="sensitivity",
            value=float(r["sensitivity"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/enrichment/test_sensitivity.py -v
```
Expected: 6 PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```

- [ ] **Step 6: Commit**

```bash
git add enrichment/sensitivity.py tests/enrichment/test_sensitivity.py
git commit -m "feat(enrichment): add OpenAlex-baseline sensitivity module"
```

---

### Task 2.2: Update Geographic Module for inst_type Stratification

**Files:**
- Modify: `enrichment/geographic.py`
- Modify: `tests/enrichment/test_geographic.py`

Current state: `geographic.py` stratifies by region only. Needs `(source × inst_type × region)` output using `make_stratum_row`.

- [ ] **Step 1: Add failing tests for inst_type stratification**

Add to `tests/enrichment/test_geographic.py`:

```python
def test_geographic_rows_include_inst_type():
    from enrichment.geographic import compute_coverage_gap_stratified
    registry = pd.DataFrame([
        {"e_mec_code": "1", "region": "Norte",   "inst_type": "federal_university", "faculty_with_phd": 100},
        {"e_mec_code": "2", "region": "Sudeste", "inst_type": "isolated_faculty",   "faculty_with_phd": 20},
    ])
    indexed = {"1"}
    rows = compute_coverage_gap_stratified(registry, indexed, "openalex")
    assert all("inst_type" in r for r in rows)
    assert all("sub_dimension" in r for r in rows)

def test_geographic_sub_dimension_values():
    from enrichment.geographic import compute_coverage_gap_stratified
    registry = pd.DataFrame([
        {"e_mec_code": "1", "region": "Norte", "inst_type": "federal_university", "faculty_with_phd": 100},
    ])
    rows = compute_coverage_gap_stratified(registry, {"1"}, "openalex")
    sub_dims = {r["sub_dimension"] for r in rows}
    assert "geographic_coverage_gap" in sub_dims
```

- [ ] **Step 2: Run to verify new tests fail**

```bash
pytest tests/enrichment/test_geographic.py::test_geographic_rows_include_inst_type -v
```
Expected: FAIL

- [ ] **Step 3: Add `compute_coverage_gap_stratified()` to `enrichment/geographic.py`**

```python
def compute_coverage_gap_stratified(
    registry: pd.DataFrame,
    indexed: set[str],
    source: str,
) -> list[dict]:
    """Return stratified rows for (source × inst_type × region) coverage gap."""
    from enrichment.stratified import make_stratum_row
    rows = []
    total = len(registry)
    total_indexed = len(indexed)
    if total == 0 or total_indexed == 0:
        return rows

    for (inst_type, region), grp in registry.groupby(["inst_type", "region"]):
        expected = len(grp) / total
        grp_codes = set(grp["e_mec_code"].astype(str))
        observed = len(grp_codes & indexed) / total_indexed
        gap = observed - expected  # negative = under-indexed
        # Score: 1.0 = perfectly proportional, 0.0 = maximally biased
        bias_score = max(0.0, min(1.0, 1.0 - abs(gap) * 2))
        rows.append(make_stratum_row(
            source=source,
            inst_type=str(inst_type),
            region=str(region),
            sub_dimension="geographic_coverage_gap",
            value=bias_score,
            n_papers=len(grp),
        ))
    return rows
```

- [ ] **Step 4: Run all geographic tests**

```bash
pytest tests/enrichment/test_geographic.py -v
```
Expected: all PASS

- [ ] **Step 5: Run full suite**

```bash
pytest tests/ -q
```

- [ ] **Step 6: Commit**

```bash
git add enrichment/geographic.py tests/enrichment/test_geographic.py
git commit -m "feat(enrichment): add inst_type stratification to geographic module"
```

---

## Sprint 3: Metadata Quality

### Task 3.1: Disambiguation Quality

**Files:**
- Create: `enrichment/disambiguation.py`
- Create: `tests/enrichment/test_disambiguation.py`

Context: Disambiguation quality = fraction of papers where at least one author has a ROR-resolved institutional affiliation. Stratified by (source × inst_type × region). Data comes from the normalized paper records already fetched (`ror_resolved` field added to OpenAlex normalizer in previous sprint).

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_disambiguation.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.disambiguation import (
    compute_disambiguation_rate, build_disambiguation_rows,
)

_PAPERS = [
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": True},
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": True},
    {"source": "openalex", "e_mec_code": "4925", "ror_resolved": False},
    {"source": "scopus",   "e_mec_code": "4925", "ror_resolved": False},
    {"source": "scopus",   "e_mec_code": "4925", "ror_resolved": False},
]
_XW = pd.DataFrame([
    {"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"},
])

def test_disambiguation_rate_openalex():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    oa_row = result[(result["source"] == "openalex") &
                    (result["inst_type"] == "federal_university")]
    assert abs(float(oa_row["ror_rate"].iloc[0]) - 2/3) < 0.01

def test_disambiguation_rate_scopus_zero():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    sc_row = result[(result["source"] == "scopus") &
                    (result["inst_type"] == "federal_university")]
    assert float(sc_row["ror_rate"].iloc[0]) == 0.0

def test_build_rows_schema():
    result = compute_disambiguation_rate(pd.DataFrame(_PAPERS), _XW)
    rows = build_disambiguation_rows(result)
    assert all(r["sub_dimension"] == "disambiguation_quality" for r in rows)
    assert all(0.0 <= r["value"] <= 1.0 for r in rows)

def test_empty_papers_returns_empty():
    result = compute_disambiguation_rate(pd.DataFrame(), _XW)
    assert result.empty

def test_missing_ror_field_treated_as_false():
    papers = pd.DataFrame([{"source": "openalex", "e_mec_code": "4925"}])
    result = compute_disambiguation_rate(papers, _XW)
    assert float(result.iloc[0]["ror_rate"]) == 0.0

def test_confidence_tier_on_small_n():
    rows = build_disambiguation_rows(pd.DataFrame([{
        "source": "scopus", "inst_type": "isolated_faculty",
        "region": "Norte", "ror_rate": 0.3, "n_papers": 8,
    }]))
    assert rows[0]["confidence_tier"] == "insufficient"
```

- [ ] **Step 2: Run to verify fail**

```bash
pytest tests/enrichment/test_disambiguation.py -v
```

- [ ] **Step 3: Implement `enrichment/disambiguation.py`**

```python
# enrichment/disambiguation.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)


def compute_disambiguation_rate(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, ror_resolved (bool)
    crosswalk_df: e_mec_code, inst_type, region
    Returns: source, inst_type, region, ror_rate, n_papers
    """
    if papers_df.empty or "source" not in papers_df.columns:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["ror_resolved"] = (
        df["ror_resolved"].fillna(False).astype(bool)
        if "ror_resolved" in df.columns
        else pd.Series(False, index=df.index)
    )

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)

    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (merged
            .groupby(["source", "inst_type", "region"])
            .agg(ror_rate=("ror_resolved", "mean"),
                 n_papers=("ror_resolved", "count"))
            .reset_index())


def build_disambiguation_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="disambiguation_quality",
            value=float(r["ror_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/enrichment/test_disambiguation.py -v
```
Expected: 6 PASS

- [ ] **Step 5: Commit**

```bash
git add enrichment/disambiguation.py tests/enrichment/test_disambiguation.py
git commit -m "feat(enrichment): add disambiguation quality stratified module"
```

---

### Task 3.2: Funder Metadata Completeness

**Files:**
- Create: `enrichment/funder.py`
- Create: `tests/enrichment/test_funder.py`

Context: Two scores per stratum — `funder_rate` (any funder present) and `br_funder_rate` (Brazilian funder: CNPq, CAPES, FAPESP, FINEP, FAPERJ, FAPEMIG). Dimensions and OpenAlex both carry funder metadata. Crossref used as reference for gap detection (how often does the source lack funder info that Crossref has?).

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_funder.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.funder import (
    is_brazilian_funder, compute_funder_rates, build_funder_rows,
    BR_FUNDER_KEYWORDS,
)

def test_br_funder_keywords_present():
    assert "cnpq" in BR_FUNDER_KEYWORDS
    assert "capes" in BR_FUNDER_KEYWORDS
    assert "fapesp" in BR_FUNDER_KEYWORDS
    assert "finep" in BR_FUNDER_KEYWORDS

def test_is_br_funder_cnpq():
    assert is_brazilian_funder("Conselho Nacional de Desenvolvimento Científico") is True

def test_is_br_funder_foreign():
    assert is_brazilian_funder("National Institutes of Health") is False

def test_is_br_funder_empty():
    assert is_brazilian_funder("") is False

_PAPERS = pd.DataFrame([
    {"source": "openalex", "e_mec_code": "4925",
     "funding": [{"funder": "CNPq", "funder_id": None, "funder_ror": None}]},
    {"source": "openalex", "e_mec_code": "4925",
     "funding": [{"funder": "NIH",  "funder_id": None, "funder_ror": None}]},
    {"source": "openalex", "e_mec_code": "4925", "funding": []},
    {"source": "scopus",   "e_mec_code": "4925", "funding": []},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])

def test_funder_rate_openalex():
    result = compute_funder_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["funder_rate"] - 2/3) < 0.01

def test_br_funder_rate():
    result = compute_funder_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["br_funder_rate"] - 1/3) < 0.01

def test_build_rows_produces_two_sub_dims():
    result = compute_funder_rates(_PAPERS, _XW)
    rows = build_funder_rows(result)
    sub_dims = {r["sub_dimension"] for r in rows}
    assert "funder_metadata_rate" in sub_dims
    assert "br_funder_rate" in sub_dims
```

- [ ] **Step 2: Run to verify fail**

```bash
pytest tests/enrichment/test_funder.py -v
```

- [ ] **Step 3: Implement `enrichment/funder.py`**

```python
# enrichment/funder.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

BR_FUNDER_KEYWORDS = {
    "cnpq", "capes", "fapesp", "faperj", "fapemig", "fapesb",
    "fapesc", "fapespa", "finep", "fundação de amparo",
    "conselho nacional de desenvolvimento", "coordenação de aperfeiçoamento",
}


def is_brazilian_funder(name: str) -> bool:
    lower = name.lower()
    return any(kw in lower for kw in BR_FUNDER_KEYWORDS)


def compute_funder_rates(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    papers_df: source, e_mec_code, funding (list of dicts with 'funder' key)
    Returns: source, inst_type, region, funder_rate, br_funder_rate, n_papers
    """
    if papers_df.empty:
        return pd.DataFrame()

    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)

    def _has_funder(funding) -> bool:
        if not isinstance(funding, list):
            return False
        return len(funding) > 0

    def _has_br_funder(funding) -> bool:
        if not isinstance(funding, list):
            return False
        return any(is_brazilian_funder(f.get("funder", "")) for f in funding)

    df["has_funder"] = df["funding"].apply(_has_funder)
    df["has_br_funder"] = df["funding"].apply(_has_br_funder)

    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()

    return (merged
            .groupby(["source", "inst_type", "region"])
            .agg(
                funder_rate=("has_funder", "mean"),
                br_funder_rate=("has_br_funder", "mean"),
                n_papers=("has_funder", "count"),
            )
            .reset_index())


def build_funder_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        for sub_dim, col in [("funder_metadata_rate", "funder_rate"),
                              ("br_funder_rate", "br_funder_rate")]:
            rows.append(make_stratum_row(
                source=r["source"],
                inst_type=r["inst_type"],
                region=r["region"],
                sub_dimension=sub_dim,
                value=float(r[col]),
                n_papers=int(r["n_papers"]),
            ))
    return rows
```

- [ ] **Step 4: Run tests and full suite**

```bash
pytest tests/enrichment/test_funder.py -v && pytest tests/ -q
```

- [ ] **Step 5: Commit**

```bash
git add enrichment/funder.py tests/enrichment/test_funder.py
git commit -m "feat(enrichment): add funder metadata completeness module with Brazilian funder detection"
```

---

### Task 3.3: Policy Document Rate

**Files:**
- Create: `enrichment/policy_docs.py`
- Create: `tests/enrichment/test_policy_docs.py`

Context: Fraction of papers per stratum classified as policy documents. Dimensions has the richest `document_type` taxonomy including policy reports, working papers, government documents. OpenAlex has `type` (preprint, article, etc.) but no policy category. Flag as Overton-pending in output.

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_policy_docs.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.policy_docs import (
    is_policy_document, compute_policy_rates, build_policy_rows,
    POLICY_DOC_TYPES,
)

def test_policy_types_populated():
    assert len(POLICY_DOC_TYPES) >= 3

def test_is_policy_report():
    assert is_policy_document("policy_report") is True

def test_is_not_policy_article():
    assert is_policy_document("journal-article") is False

def test_is_none_not_policy():
    assert is_policy_document(None) is False

_PAPERS = pd.DataFrame([
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "policy_report"},
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "journal-article"},
    {"source": "dimensions", "e_mec_code": "4925", "document_type": "working_paper"},
    {"source": "openalex",   "e_mec_code": "4925", "document_type": "article"},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])

def test_policy_rate_dimensions():
    result = compute_policy_rates(_PAPERS, _XW)
    dim = result[result["source"] == "dimensions"].iloc[0]
    assert abs(dim["policy_rate"] - 2/3) < 0.01

def test_policy_rate_openalex_zero():
    result = compute_policy_rates(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert oa["policy_rate"] == 0.0

def test_build_rows_flagged_overton_pending():
    result = compute_policy_rates(_PAPERS, _XW)
    rows = build_policy_rows(result)
    # Metadata note should flag Overton
    assert all(r["sub_dimension"] == "policy_document_rate" for r in rows)
```

- [ ] **Step 2: Implement `enrichment/policy_docs.py`**

```python
# enrichment/policy_docs.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

# Dimensions document types classified as policy-relevant
# Overton cross-reference pending when access available
POLICY_DOC_TYPES = {
    "policy_report", "policy_brief", "working_paper",
    "government_document", "report", "legislation",
    "clinical_guideline", "standard",
}


def is_policy_document(doc_type: str | None) -> bool:
    if not doc_type:
        return False
    return doc_type.lower().replace(" ", "_") in POLICY_DOC_TYPES


def compute_policy_rates(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    if papers_df.empty:
        return pd.DataFrame()
    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["is_policy"] = df["document_type"].apply(is_policy_document)
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()
    return (merged
            .groupby(["source", "inst_type", "region"])
            .agg(policy_rate=("is_policy", "mean"),
                 n_papers=("is_policy", "count"))
            .reset_index())


def build_policy_rows(df: pd.DataFrame) -> list[dict]:
    # NOTE: Overton cross-reference pending — policy_document_rate uses
    # source-declared document_type only. Validate against Overton when available.
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="policy_document_rate",
            value=float(r["policy_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
```

- [ ] **Step 3: Run tests and full suite**

```bash
pytest tests/enrichment/test_policy_docs.py -v && pytest tests/ -q
```

- [ ] **Step 4: Commit**

```bash
git add enrichment/policy_docs.py tests/enrichment/test_policy_docs.py
git commit -m "feat(enrichment): add policy document rate module (Overton-pending)"
```

---

## Sprint 4: Innovation & Impact

### Task 4.1: Stratify Existing Coauthorship + SDG Modules

**Files:**
- Modify: `enrichment/coauthorship.py`
- Modify: `enrichment/sdg.py`
- Modify: `tests/enrichment/test_coauthorship.py`
- Modify: `tests/enrichment/test_sdg.py`

- [ ] **Step 1: Add failing tests for stratified coauthorship output**

Add to `tests/enrichment/test_coauthorship.py`:

```python
def test_compute_coauth_stratified_schema():
    from enrichment.coauthorship import compute_coauth_stratified
    papers = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "4925",
         "affiliation_types": [["company", "education"]], "ror_resolved": True},
        {"source": "openalex", "e_mec_code": "4925",
         "affiliation_types": [["education"]], "ror_resolved": True},
    ])
    xw = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])
    rows = compute_coauth_stratified(papers, xw)
    assert all("sub_dimension" in r for r in rows)
    assert any(r["sub_dimension"] == "nonacademic_coauth" for r in rows)
```

- [ ] **Step 2: Add `compute_coauth_stratified()` to `enrichment/coauthorship.py`**

```python
def compute_coauth_stratified(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> list[dict]:
    """Stratified non-academic coauthorship rows."""
    from enrichment.stratified import make_stratum_row
    if papers_df.empty:
        return []
    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    rows = []
    for (source, inst_type, region), grp in merged.groupby(["source", "inst_type", "region"]):
        papers_list = grp.to_dict("records")
        metrics = compute_coauth_metrics(papers_list)
        rows.append(make_stratum_row(
            source=str(source), inst_type=str(inst_type), region=str(region),
            sub_dimension="nonacademic_coauth",
            value=metrics["nonacademic_coauth_score"],
            n_papers=len(papers_list),
        ))
    return rows
```

- [ ] **Step 3: Add failing tests for stratified SDG output**

Add to `tests/enrichment/test_sdg.py`:

```python
def test_compute_sdg_stratified_schema():
    from enrichment.sdg import compute_sdg_stratified
    papers = pd.DataFrame([
        {"source": "openalex", "e_mec_code": "4925", "sdgs": [3, 4]},
        {"source": "openalex", "e_mec_code": "4925", "sdgs": [4]},
    ])
    xw = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])
    rows = compute_sdg_stratified(papers, xw)
    assert all("sub_dimension" in r for r in rows)
    assert all(r["sub_dimension"].startswith("sdg_") for r in rows)
```

- [ ] **Step 4: Add `compute_sdg_stratified()` to `enrichment/sdg.py`**

```python
def compute_sdg_stratified(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> list[dict]:
    """Stratified SDG rows — one row per (source × inst_type × region × sdg_goal)."""
    from enrichment.stratified import make_stratum_row
    if papers_df.empty:
        return []
    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    rows = []
    for (source, inst_type, region), grp in merged.groupby(["source", "inst_type", "region"]):
        papers_list = grp.to_dict("records")
        rates = compute_sdg_rates(papers_list)
        n = len(papers_list)
        for goal, data in rates.items():
            rows.append(make_stratum_row(
                source=str(source), inst_type=str(inst_type), region=str(region),
                sub_dimension=f"sdg_{goal:02d}",
                value=data["rate"],
                n_papers=n,
            ))
    return rows
```

- [ ] **Step 5: Run all enrichment tests**

```bash
pytest tests/enrichment/ -v
```
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add enrichment/coauthorship.py enrichment/sdg.py \
        tests/enrichment/test_coauthorship.py tests/enrichment/test_sdg.py
git commit -m "feat(enrichment): add inst_type×region stratification to coauthorship and SDG modules"
```

---

### Task 4.2: Patent Link Rate Module

**Files:**
- Create: `enrichment/patents.py`
- Create: `tests/enrichment/test_patents.py`

Context: `patent_citations` field is already on normalized OpenAlex records (populated by Lens connector). For now: if Lens API key absent, use Crossref reference deposit as partial signal. Flag Derwent as future cross-reference.

- [ ] **Step 1: Write failing tests**

```python
# tests/enrichment/test_patents.py
from __future__ import annotations
import pandas as pd
import pytest
from enrichment.patents import (
    has_patent_link, compute_patent_link_rate, build_patent_rows,
)

_PAPERS = pd.DataFrame([
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": ["US1234"]},
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": []},
    {"source": "openalex", "e_mec_code": "4925", "patent_citations": None},
    {"source": "scopus",   "e_mec_code": "4925", "patent_citations": []},
])
_XW = pd.DataFrame([{"e_mec_code": "4925", "inst_type": "federal_university", "region": "Sudeste"}])

def test_has_patent_link_true():
    assert has_patent_link(["US1234"]) is True

def test_has_patent_link_empty():
    assert has_patent_link([]) is False

def test_has_patent_link_none():
    assert has_patent_link(None) is False

def test_patent_link_rate():
    result = compute_patent_link_rate(_PAPERS, _XW)
    oa = result[result["source"] == "openalex"].iloc[0]
    assert abs(oa["patent_rate"] - 1/3) < 0.01

def test_build_rows_derwent_flag():
    result = compute_patent_link_rate(_PAPERS, _XW)
    rows = build_patent_rows(result)
    # sub_dimension should be patent_link_rate
    assert all(r["sub_dimension"] == "patent_link_rate" for r in rows)
    # NOTE: Derwent cross-reference pending — see enrichment/patents.py
```

- [ ] **Step 2: Implement `enrichment/patents.py`**

```python
# enrichment/patents.py
from __future__ import annotations

import logging

import pandas as pd

from enrichment.stratified import make_stratum_row

logger = logging.getLogger(__name__)

# NOTE: Patent link rate currently derived from `patent_citations` field
# populated by The Lens connector (requires LENS_API_KEY).
# Derwent Innovation cross-reference pending when access available.
# Crossref reference deposit provides partial signal where Lens absent.


def has_patent_link(patent_citations) -> bool:
    if not patent_citations:
        return False
    return len(patent_citations) > 0


def compute_patent_link_rate(
    papers_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
) -> pd.DataFrame:
    if papers_df.empty:
        return pd.DataFrame()
    df = papers_df.copy()
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    df["has_patent"] = df["patent_citations"].apply(has_patent_link)
    xw = crosswalk_df[["e_mec_code", "inst_type", "region"]].copy()
    xw["e_mec_code"] = xw["e_mec_code"].astype(str)
    merged = df.merge(xw, on="e_mec_code", how="inner")
    if merged.empty:
        return pd.DataFrame()
    return (merged
            .groupby(["source", "inst_type", "region"])
            .agg(patent_rate=("has_patent", "mean"),
                 n_papers=("has_patent", "count"))
            .reset_index())


def build_patent_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(make_stratum_row(
            source=r["source"],
            inst_type=r["inst_type"],
            region=r["region"],
            sub_dimension="patent_link_rate",
            value=float(r["patent_rate"]),
            n_papers=int(r["n_papers"]),
        ))
    return rows
```

- [ ] **Step 3: Run tests and full suite**

```bash
pytest tests/enrichment/test_patents.py -v && pytest tests/ -q
```

- [ ] **Step 4: Commit**

```bash
git add enrichment/patents.py tests/enrichment/test_patents.py
git commit -m "feat(enrichment): add patent link rate module (Derwent cross-reference pending)"
```

---

## Sprint 5: Orchestration, Scorer & Dashboard

### Task 5.1: Update `run_enrichment.py` to Write All Stratified CSVs

**Files:**
- Modify: `run_enrichment.py`

For each enrichment module, call the stratified function and write a dated CSV using `write_stratified_csv`. All outputs land in `data/processed/`.

- [ ] **Step 1: Update `run_enrichment.py` to call all stratified modules**

Add these imports at the top of `run_enrichment.py` alongside existing imports:

```python
from enrichment.sensitivity import compute_sensitivity, aggregate_by_stratum, build_sensitivity_rows
from enrichment.disambiguation import compute_disambiguation_rate, build_disambiguation_rows
from enrichment.funder import compute_funder_rates, build_funder_rows
from enrichment.policy_docs import compute_policy_rates, build_policy_rows
from enrichment.coauthorship import compute_coauth_stratified
from enrichment.sdg import compute_sdg_stratified
from enrichment.patents import compute_patent_link_rate, build_patent_rows
from enrichment.stratified import write_stratified_csv
```

Add a `_load_crosswalk()` helper that reads `registry/crosswalk_enriched.csv` and returns a DataFrame with columns `e_mec_code, inst_type, region`. Note: `crosswalk_enriched.csv` has a `sinaes_type` column — rename it to `inst_type` here:

```python
def _load_crosswalk() -> pd.DataFrame:
    path = Path("registry/crosswalk_enriched.csv")
    if not path.exists():
        logger.warning("crosswalk_enriched.csv not found — stratified enrichment will be empty")
        return pd.DataFrame(columns=["e_mec_code", "inst_type", "region"])
    df = pd.read_csv(path)
    df["e_mec_code"] = df["e_mec_code"].astype(str)
    # crosswalk_enriched uses sinaes_type; rename for enrichment modules
    if "sinaes_type" in df.columns and "inst_type" not in df.columns:
        df = df.rename(columns={"sinaes_type": "inst_type"})
    return df[["e_mec_code", "inst_type", "region"]].dropna()
```

Add a `_to_papers_df(papers)` helper that converts the list returned by `_fetch_papers_for_enrichment` to a DataFrame, ensuring all expected columns are present with safe defaults. These are the fields the enrichment modules require — all present on OpenAlex/Dimensions normalised records after Task 4 connector extensions:

```python
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
```

Add the following block at the end of `main()`, after the existing SDG section, still inside `if not args.skip_coauth` and similar guards:

```python
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
```

- [ ] **Step 2: Smoke-test against existing phase 2 outputs**

```bash
cd "/Users/administrador/Downloads/INEP comparer"
python run_enrichment.py --skip-coauth --skip-sdg 2>&1 | tail -10
```
Expected: INFO logs, no errors

- [ ] **Step 3: Verify stratified CSVs written**

```bash
python3 -c "
import glob, pandas as pd
for pat in ['sensitivity', 'geographic_coverage', 'disambiguation', 'funder', 'policy_docs']:
    files = sorted(glob.glob(f'data/processed/{pat}_*.csv'))
    if files:
        df = pd.read_csv(files[-1])
        print(f'{pat}: {len(df)} rows')
    else:
        print(f'{pat}: NOT FOUND (may need API run)')
"
```

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add run_enrichment.py
git commit -m "feat: wire all stratified enrichment modules into run_enrichment.py"
```

---

### Task 5.2: Dashboard Enrichment Tab

**Files:**
- Modify: `dashboard/data_loader.py` — add `load_sensitivity()`, `load_metadata_quality()`, `load_sdg_stratified()`
- Create: `dashboard/tabs/enrichment.py`
- Create: `tests/dashboard/test_enrichment_tab.py`

Note: `app.py` currently imports `enrichment_tab` and calls the old 3-arg `enrichment_tab.layout(_geo_df, _sdg_df, _metadata)`. This task updates `app.py` to replace those calls with `load_enrichment_combined()` and `enrichment_tab.layout(_enrichment_df, _metadata)`, and adds the missing loaders and tab implementation.

- [ ] **Step 1: Add loaders to `dashboard/data_loader.py`**

```python
def load_sensitivity(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load most recent sensitivity_*.csv. Returns empty DataFrame if absent."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("sensitivity_*.csv"))
    if not files:
        logger.warning("No sensitivity_*.csv found in %s", csv_dir)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.read_csv(files[-1])

def load_metadata_quality(csv_dir: Path | None = None) -> pd.DataFrame:
    """Concatenate disambiguation, funder, and policy_docs stratified CSVs."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    frames = []
    for pat in ["disambiguation_*.csv", "funder_*.csv", "policy_docs_*.csv"]:
        files = sorted(Path(csv_dir).glob(pat))
        if files:
            frames.append(pd.read_csv(files[-1]))
    if not frames:
        logger.warning("No metadata quality CSVs found in %s", csv_dir)
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    return pd.concat(frames, ignore_index=True)

def load_sdg_stratified(csv_dir: Path | None = None) -> pd.DataFrame:
    """Load most recent sdg_stratified_*.csv (unified schema).
    Falls back to sdg_by_source_type_*.csv if stratified version absent.
    Returns empty DataFrame if neither found.
    """
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(Path(csv_dir).glob("sdg_stratified_*.csv"))
    if files:
        return pd.read_csv(files[-1])
    # Legacy fallback
    legacy = sorted(Path(csv_dir).glob("sdg_by_source_type_*.csv"))
    if legacy:
        logger.info("Using legacy SDG file — re-run enrichment to get stratified version")
        return pd.read_csv(legacy[-1])
    return pd.DataFrame(columns=STRATIFIED_SCHEMA)
```

`STRATIFIED_SCHEMA = ["source","inst_type","region","sub_dimension","value","n_papers","confidence_tier"]` — add as a module-level constant at the top of `data_loader.py`.

Also add `load_enrichment_combined()` which merges all enrichment sources into one DataFrame for the store:

```python
def load_enrichment_combined(csv_dir: Path | None = None) -> pd.DataFrame:
    """Merge geographic, sensitivity, SDG, and metadata quality into one stratified DataFrame."""
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    frames = []
    # Geographic — may use old region-only schema; add inst_type="all" if column absent
    geo_files = sorted(Path(csv_dir).glob("geographic_coverage_*.csv"))
    if geo_files:
        g = pd.read_csv(geo_files[-1])
        if "inst_type" not in g.columns:
            g["inst_type"] = "all"
        # Map old column names to stratified schema if needed
        if "sub_dimension" not in g.columns:
            g = g.rename(columns={"geographic_bias_score": "value"})
            g["sub_dimension"] = "geographic_coverage_gap"
            g["n_papers"] = 0
            g["confidence_tier"] = "low"
        frames.append(g[STRATIFIED_SCHEMA] if all(c in g.columns for c in STRATIFIED_SCHEMA) else g)
    for loader in [load_sensitivity, load_metadata_quality, load_sdg_stratified]:
        frames.append(loader(csv_dir))
    if not frames:
        return pd.DataFrame(columns=STRATIFIED_SCHEMA)
    result = pd.concat(frames, ignore_index=True)
    return result[[c for c in STRATIFIED_SCHEMA if c in result.columns]]
```

**`app.py` integration:** Update `app.py` to call `load_enrichment_combined()` instead of `load_geographic()`, `load_sdg()`, and no longer pass them separately. The enrichment tab's `layout()` signature becomes:

```python
def layout(combined_df: pd.DataFrame, metadata: dict) -> html.Div:
```

Update `app.py` accordingly:
```python
from dashboard.data_loader import load_enrichment_combined, load_source_metadata
_enrichment_df = load_enrichment_combined(csv_dir=_PROCESSED)
_metadata       = load_source_metadata(processed_dir=_PROCESSED)
# In render_tab:
if tab == "tab-enrichment":
    return enrichment_tab.layout(_enrichment_df, _metadata)
```

- [ ] **Step 2: Write failing tests**

```python
# tests/dashboard/test_enrichment_tab.py
from __future__ import annotations
import pandas as pd
import pytest
from dashboard.tabs.enrichment import layout, register_callbacks
import dash

_COMBINED = pd.DataFrame([
    {"source": "openalex", "inst_type": "federal_university", "region": "Sudeste",
     "sub_dimension": "geographic_coverage_gap", "value": 0.8, "n_papers": 500,
     "confidence_tier": "reliable"},
    {"source": "scopus", "inst_type": "isolated_faculty", "region": "Norte",
     "sub_dimension": "sensitivity", "value": 0.41, "n_papers": 47,
     "confidence_tier": "moderate"},
])
_META = {"scopus": {"sdg_available": False}}

def test_layout_renders():
    result = layout(_COMBINED, _META)
    assert result is not None

def test_layout_empty_df():
    result = layout(pd.DataFrame(), {})
    assert result is not None

def test_register_callbacks_no_error():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)  # should not raise

def test_scopus_sdg_caveat_shown():
    result = layout(_COMBINED, {"scopus": {"sdg_available": False}})
    assert "Scopus" in str(result)

def test_chart_component_ids_present():
    result = str(layout(_COMBINED, _META))
    assert "enrichment-heatmap" in result
    assert "enrichment-bar-chart" in result

def test_layout_with_combined_data():
    result = layout(_COMBINED, _META)
    assert result is not None

def test_no_crash_missing_columns():
    bad_df = pd.DataFrame([{"source": "openalex"}])
    result = layout(bad_df, {})
    assert result is not None

def test_callbacks_registered():
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    register_callbacks(app)
    cb_ids = [str(cb) for cb in app.callback_map.keys()]
    assert any("enrichment" in cb_id for cb_id in cb_ids)
```

- [ ] **Step 3: Run to verify tests fail**

```bash
pytest tests/dashboard/test_enrichment_tab.py -v
```
Expected: FAIL — `ModuleNotFoundError: dashboard.tabs.enrichment`

- [ ] **Step 4: Implement `dashboard/tabs/enrichment.py`**

**IMPORTANT:** This is a complete new file, not an update. Do not preserve any existing `enrichment.py` code. All chart helpers must use the unified stratified schema columns: `source, inst_type, region, sub_dimension, value, n_papers, confidence_tier`. The old geographic schema (`coverage_gap`, `output_gap`, `geographic_bias_score`) is no longer used in this tab — the loader normalises it.

Tab structure:
- Header: "Enrichment — Source Quality by Institution Type & Region"
- Scopus SDG caveat banner (from `source_metadata.json` via `_metadata` dict, conditional on `metadata.get("scopus", {}).get("sdg_available") is False`)
- `html.Div(id="enrichment-caveat-notes")` — Overton pending, Derwent pending static notes
- `dcc.Dropdown(id="enrichment-sub-dim-filter")` — select sub_dimension from all available in combined df
- `dcc.Dropdown(id="enrichment-source-filter")` — multi-select source, `multi=True`
- `dcc.Graph(id="enrichment-heatmap")` — (inst_type × region) heatmap, colour by value, for selected sub_dim + source
- `dcc.Graph(id="enrichment-bar-chart")` — bar chart: x=`inst_type`, y=`value`, colour=`region`, for selected filters
- `dcc.Store(id="enrichment-data-store")` — combined df (geo + sensitivity + sdg + metadata quality) serialised via `to_json(orient="records")`

Callbacks (in `register_callbacks(app)`):
- `update_enrichment_charts(sub_dim, sources, data_json)` → Output: heatmap figure, bar chart figure
  - `data_json` must be `State`, not `Input` (static after load)
  - Deserialise, coerce `value` and `n_papers` to numeric
  - Filter by sub_dim and sources
  - Guard for insufficient confidence tier: grey out cells with `confidence_tier == "insufficient"` in heatmap text
  - NaN guard on pivot: `v == v` pattern from fitness tab

Private helpers:
- `_heatmap_figure(df)` — pivot inst_type × region, dark theme, NaN → empty string in text
- `_bar_figure(df)` — grouped bar, dark theme, x=inst_type, colour per region
- `_empty_figure(title)` — consistent empty state

Follow dark theme (`paper_bgcolor="#303030"`, `plot_bgcolor="#303030"`, `font={"color":"#ffffff"}`), NaN guards (`v == v`), dtype coercion after `pd.read_json`, and `State` for static stores — all established in earlier tabs.

- [ ] **Step 5: Run tests**

```bash
pytest tests/dashboard/test_enrichment_tab.py -v
```
Expected: 8 PASS

- [ ] **Step 6: Run full suite — final check**

```bash
pytest tests/ -v 2>&1 | tail -20
```
Expected: 230+ passed

- [ ] **Step 7: Commit**

```bash
git add dashboard/data_loader.py dashboard/tabs/enrichment.py \
        tests/dashboard/test_enrichment_tab.py
git commit -m "feat(dashboard): add Enrichment tab with stratified geographic, sensitivity, SDG, metadata quality views"
```

---

## Summary

| Sprint | Tasks | New Tests | What it delivers |
|---|---|---|---|
| 1 — Infrastructure | 1.1 Stratified utils, 1.2 Crossref connector | ~17 | Unified CSV schema, free metadata validator |
| 2 — Coverage & Sensitivity | 2.1 Sensitivity, 2.2 Geographic strat | ~8 | OpenAlex-baseline recall per stratum |
| 3 — Metadata Quality | 3.1 Disambiguation, 3.2 Funder, 3.3 Policy docs | ~17 | ROR resolution, funder completeness, policy document rate |
| 4 — Innovation & Impact | 4.1 Coauth+SDG strat, 4.2 Patents | ~10 | Stratified coauthorship, SDG, patent links (Derwent-pending) |
| 5 — Integration | 5.1 run_enrichment, 5.2 Dashboard tab | ~8 | Full pipeline wired, Enrichment tab live |

**Pending integrations (flagged in code, not blocking):**
- Overton: `policy_document_rate` — cross-reference when access available
- Derwent: `patent_link_rate` — cross-reference when access available
- The Lens: `patent_citations` on paper records — requires `LENS_API_KEY`
- Crossref gap scoring: compare source funder metadata vs Crossref for same DOIs — wired in `run_enrichment.py`, full batch run needed
