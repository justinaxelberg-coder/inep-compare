# Record Reliability and Usable Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a record-first reliability pipeline that classifies canonical works as `integration_ready`, `reviewable_disputed`, or `not_integration_ready`, then rolls those judgments up into standalone source comparison outputs for INEP.

**Architecture:** Add a new reliability pipeline alongside the existing fitness scorer rather than forcing this methodology into the current composite score. The pipeline will read exported records plus convergence matches, resolve canonical works using the spec’s matching hierarchy, enrich DOI-backed works with Crossref corroboration, derive source-attributed reliability flags, and write share-based source summaries plus a dedicated report and dashboard tab. Current `scoring/fitness.py` remains unchanged in V1 so the new method stays descriptive, auditable, and reversible.

**Tech Stack:** Python 3.11+, pandas, pytest, Dash, Plotly, existing Crossref connector, existing convergence matcher/exporter stack. No new dependencies.

**Guiding constraints:**
- Treat reliability as descriptive evidence, not a new composite source score.
- Count coverage at the canonical-work level, not raw duplicate record counts.
- Keep source accountability explicit: every dispute and missing-evidence flag must be attributable back to the source that introduced it.
- Prefer pure rule helpers and thin orchestration so the methodology is testable.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `convergence/reliability_rules.py` | Create | Pure functions for evidence ladders, DOI-expected logic, flag derivation, outcome-state derivation, confidence-band derivation |
| `tests/convergence/test_reliability_rules.py` | Create | Unit tests for rule logic and edge cases |
| `convergence/external_validation.py` | Create | Crossref-backed corroboration helpers and validator payload normalization |
| `tests/convergence/test_external_validation.py` | Create | Tests for validator normalization and corroboration behavior |
| `connectors/api/crossref.py` | Modify | Expand validation payload to include title/year/type signals needed for reliability corroboration |
| `tests/connectors/test_crossref.py` | Modify | Cover expanded Crossref payload |
| `convergence/reliability.py` | Create | Canonical ID assignment, source-record attribution rows, canonical-work summaries, source-level rollups |
| `tests/convergence/test_reliability.py` | Create | Canonical-work, duplicate, attribution, denominator, and rollup tests |
| `outputs/reports/reliability.py` | Create | Markdown report builder for usable coverage, disputed coverage, DOI gaps, and top downgrade reasons |
| `outputs/dataset/exporter.py` | Modify | Export reliability CSVs and delegate Markdown creation to report builder |
| `tests/outputs/test_reliability_exporter.py` | Create | CSV/report export tests |
| `run_reliability.py` | Create | Orchestrate records loading, JSON field deserialization, convergence loading, Crossref validation, reliability evaluation, and exports |
| `dashboard/data_loader.py` | Modify | Load reliability outputs with stable schemas |
| `dashboard/tabs/reliability.py` | Create | Dedicated dashboard tab for source-level reliability shares, record-type breakdowns, DOI gaps, and flag views |
| `dashboard/tabs/__init__.py` | Modify | Export reliability tab module |
| `dashboard/app.py` | Modify | Register and render the new Reliability tab |
| `tests/dashboard/test_data_loader.py` | Modify | Cover reliability loaders and empty-state behavior |
| `tests/dashboard/test_reliability_tab.py` | Create | Smoke tests for reliability tab figures and empty states |

**Explicit non-goals for this plan:**
- Do not rewrite `scoring/fitness.py` or change composite weights in V1.
- Do not add source-combination optimization.
- Do not implement record-type-specific external validators beyond the Crossref-backed article/repository path in V1; preserve hooks for later extension.

---

## Output Contracts

Define these schemas before writing implementation code so the runner, exporter, report, and dashboard all use the same units.

### `source_record_reliability_<run_id>.csv`

One row per `(source, source_record_id)`.

Required columns:

- `canonical_work_id`
- `source`
- `source_record_id`
- `record_type`
- `match_basis`
- `outcome_state`
- `confidence_band`
- `has_external_corroboration`
- `has_major_conflict`
- `introduced_major_conflict`
- `introduced_weak_author_identity`
- `introduced_weak_institution_linkage`
- `introduced_doi_expected_missing`
- `flags`

Purpose: source attribution. This is the table that answers which source introduced the conflict or weak evidence.

### `canonical_work_reliability_<run_id>.csv`

One row per `canonical_work_id`.

Required columns:

- `canonical_work_id`
- `record_type`
- `n_sources`
- `sources`
- `outcome_state`
- `confidence_band`
- `has_external_corroboration`
- `has_major_conflict`
- `conflict_fields`
- `flags`

Purpose: canonical-work-level decision table. Coverage and usable coverage must derive from this unit, not raw source-record rows.
This table is also the source of truth for canonical `record_type`. All record-type and DOI-gap shares must derive from this table, not from conflicting source-record labels.

### `source_reliability_summary_<run_id>.csv`

One row per `(source, record_type)` plus one `record_type="__all__"` row per source.

Required columns:

- `source`
- `record_type`
- `canonical_works`
- `integration_ready_share`
- `reviewable_disputed_share`
- `not_integration_ready_share`
- `high_confidence_share`
- `medium_confidence_share`
- `low_confidence_share`
- `external_corroboration_share`
- `major_conflict_share`
- `doi_expected_missing_share`

Denominators:

- overall rows: all unique canonical works surfaced by that source
- `record_type` rows: all unique canonical works of that type surfaced by that source
- `doi_expected_missing_share`: only canonical works surfaced by that source whose type is in the DOI-expected set

### `source_reliability_flags_<run_id>.csv`

One row per `(source, record_type, flag)`.

Required columns:

- `source`
- `record_type`
- `flag`
- `n_works`
- `denominator`
- `share`

Purpose: normalized downgrade-reasons table. Reports and dashboards should derive “top downgrade reasons” from this output rather than embedding lossy pre-ranked text in the summary CSV.

---

## Task 1: Reliability Rule Engine

**Files:**
- Create: `convergence/reliability_rules.py`
- Create: `tests/convergence/test_reliability_rules.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/convergence/test_reliability_rules.py
from convergence.reliability_rules import (
    AUTHOR_GOLD,
    INSTITUTION_GOLD,
    LOCATOR_INTERNAL,
    LOCATOR_UNIVERSAL,
    classify_author_strength,
    classify_institution_strength,
    classify_locator_strength,
    confidence_band_for_work,
    flags_for_work,
    is_doi_expected,
    outcome_state_for_work,
)


def test_doi_expected_articles_true():
    assert is_doi_expected("journal_article") is True
    assert is_doi_expected("conference_paper") is True


def test_doi_expected_thesis_false():
    assert is_doi_expected("thesis") is False
    assert is_doi_expected("book_chapter") is False


def test_locator_strength_ranks_internal_below_universal():
    assert classify_locator_strength({"doi": "10.1234/x"}) == LOCATOR_UNIVERSAL
    assert classify_locator_strength({"source_record_id": "oa_123"}) == LOCATOR_INTERNAL


def test_author_strength_prefers_orcid():
    work = {"authors": [{"orcid": "0000-0001-2345-6789", "name": "Maria Silva"}]}
    assert classify_author_strength(work) == AUTHOR_GOLD


def test_institution_strength_prefers_ror():
    work = {"institutions": [{"ror": "https://ror.org/03yrm5c26", "name": "UFPA"}]}
    assert classify_institution_strength(work) == INSTITUTION_GOLD


def test_flags_mark_missing_doi_when_expected():
    flags = flags_for_work(
        {
            "record_type": "journal_article",
            "title": "Paper",
            "year": 2023,
            "source_record_id": "oa_1",
            "authors": [{"orcid": "0000-0001-2345-6789"}],
            "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            "provenance_url": "https://example.org/work/1",
            "canonical_work_id": "cw_1",
        },
        has_external_corroboration=False,
        has_major_conflict=False,
    )
    assert "doi_expected_missing" in flags


def test_outcome_state_reviewable_disputed_when_conflicted_but_verifiable():
    outcome = outcome_state_for_work(
        flags={"major_conflict"},
        has_external_corroboration=True,
        has_verifiable_author=True,
        has_verifiable_institution=True,
        has_stable_locator=True,
        work_identity_resolved=True,
    )
    assert outcome == "reviewable_disputed"


def test_high_confidence_requires_external_corroboration():
    band = confidence_band_for_work(
        flags=set(),
        has_external_corroboration=False,
        has_major_conflict=False,
        locator_strength=LOCATOR_UNIVERSAL,
        author_strength=AUTHOR_GOLD,
        institution_strength=INSTITUTION_GOLD,
    )
    assert band != "high"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/convergence/test_reliability_rules.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'convergence.reliability_rules'`

- [ ] **Step 3: Implement the pure rule module**

```python
# convergence/reliability_rules.py
from __future__ import annotations

DOI_EXPECTED_TYPES = {"journal_article", "conference_paper", "repository_postprint", "report"}

LOCATOR_NONE = "none"
LOCATOR_INTERNAL = "internal"
LOCATOR_EXTERNAL = "external"
LOCATOR_UNIVERSAL = "universal"

AUTHOR_GOLD = "orcid"
AUTHOR_INTERNAL = "internal_id"
AUTHOR_WEAK = "string"
AUTHOR_NONE = "none"

INSTITUTION_GOLD = "ror"
INSTITUTION_WEAK = "name_match"
INSTITUTION_NONE = "none"


def is_doi_expected(record_type: str | None) -> bool:
    return (record_type or "").strip().lower() in DOI_EXPECTED_TYPES


def classify_locator_strength(work: dict) -> str:
    if work.get("doi") or work.get("isbn") or work.get("patent_number"):
        return LOCATOR_UNIVERSAL
    if work.get("external_ids"):
        return LOCATOR_EXTERNAL
    if work.get("source_record_id"):
        return LOCATOR_INTERNAL
    return LOCATOR_NONE


def classify_author_strength(work: dict) -> str:
    for author in work.get("authors") or []:
        if author.get("orcid"):
            return AUTHOR_GOLD
    for author in work.get("authors") or []:
        if author.get("author_id"):
            return AUTHOR_INTERNAL
    for author in work.get("authors") or []:
        if author.get("name"):
            return AUTHOR_WEAK
    return AUTHOR_NONE


def classify_institution_strength(work: dict) -> str:
    for inst in work.get("institutions") or []:
        if inst.get("ror"):
            return INSTITUTION_GOLD
    for inst in work.get("institutions") or []:
        if inst.get("matched_name") or inst.get("name"):
            return INSTITUTION_WEAK
    return INSTITUTION_NONE
```

- [ ] **Step 4: Run the new unit tests**

Run: `pytest tests/convergence/test_reliability_rules.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add convergence/reliability_rules.py tests/convergence/test_reliability_rules.py
git commit -m "feat(reliability): add rule engine for work-level reliability"
```

---

## Task 2: External Corroboration Adapter

**Files:**
- Create: `convergence/external_validation.py`
- Modify: `connectors/api/crossref.py`
- Modify: `tests/connectors/test_crossref.py`
- Create: `tests/convergence/test_external_validation.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/convergence/test_external_validation.py
from convergence.external_validation import (
    external_corroboration_for_work,
    normalise_crossref_validation,
)


def test_normalise_crossref_validation_keeps_core_fields():
    payload = normalise_crossref_validation(
        {
            "doi": "10.1234/x",
            "document_type": "journal-article",
            "title": "Observed work title",
            "published_year": 2023,
            "ror_affiliation_present": True,
        }
    )
    assert payload["record_type"] == "journal_article"
    assert payload["title"] == "Observed work title"
    assert payload["year"] == 2023


def test_external_corroboration_flags_major_conflict_on_type():
    work = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "journal_article"}
    crossref = {"doi": "10.1234/x", "title": "Observed work title", "year": 2023, "record_type": "book"}
    result = external_corroboration_for_work(work, crossref)
    assert result["has_major_conflict"] is True
```

```python
# tests/connectors/test_crossref.py
def test_validate_doi_returns_title_year_and_type():
    conn = CrossrefConnector()
    work = {
        **_SAMPLE_WORK,
        "title": ["Observed work title"],
        "published": {"date-parts": [[2023, 1, 1]]},
    }
    with patch.object(conn, "_get_work", return_value=work):
        result = conn.validate_doi("10.1234/test")
    assert result["title"] == "Observed work title"
    assert result["published_year"] == 2023
    assert result["document_type"] == "journal-article"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/convergence/test_external_validation.py tests/connectors/test_crossref.py -v`

Expected: FAIL because `convergence.external_validation` does not exist and `CrossrefConnector.validate_doi()` lacks the new fields

- [ ] **Step 3: Implement Crossref normalization and corroboration helpers**

```python
# convergence/external_validation.py
from __future__ import annotations

import re


def _normalise_record_type(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("-", "_").strip().lower()


def normalise_crossref_validation(payload: dict | None) -> dict:
    if not payload:
        return {}
    return {
        "doi": payload.get("doi"),
        "record_type": _normalise_record_type(payload.get("document_type")),
        "title": payload.get("title"),
        "year": payload.get("published_year"),
        "ror_affiliation_present": bool(payload.get("ror_affiliation_present")),
    }


def external_corroboration_for_work(work: dict, crossref_payload: dict | None) -> dict:
    ref = normalise_crossref_validation(crossref_payload)
    if not ref:
        return {"has_external_corroboration": False, "has_major_conflict": False, "conflict_fields": []}
    conflict_fields = []
    if ref.get("record_type") and work.get("record_type") and ref["record_type"] != work["record_type"]:
        conflict_fields.append("record_type")
    if ref.get("year") and work.get("year") and abs(int(ref["year"]) - int(work["year"])) > 1:
        conflict_fields.append("publication_year")
    has_external_corroboration = bool(work.get("doi") and ref.get("doi") and work["doi"] == ref["doi"])
    return {
        "has_external_corroboration": has_external_corroboration,
        "has_major_conflict": bool(conflict_fields),
        "conflict_fields": conflict_fields,
    }
```

```python
# connectors/api/crossref.py
    def validate_doi(self, doi: str) -> dict | None:
        work = self._get_work(doi)
        if work is None:
            return None
        titles = work.get("title") or []
        published = work.get("published") or work.get("published-print") or {}
        date_parts = published.get("date-parts") or [[None]]
        year = date_parts[0][0] if date_parts and date_parts[0] else None
        return {
            "doi": doi,
            "funder_present": self.has_funder(work),
            "license_present": self.has_license(work),
            "ror_affiliation_present": self.has_ror_affiliation(work),
            "document_type": work.get("type"),
            "title": titles[0] if titles else None,
            "published_year": year,
        }
```

- [ ] **Step 4: Run the focused tests**

Run: `pytest tests/convergence/test_external_validation.py tests/connectors/test_crossref.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add convergence/external_validation.py connectors/api/crossref.py tests/convergence/test_external_validation.py tests/connectors/test_crossref.py
git commit -m "feat(reliability): add external corroboration adapter"
```

---

## Task 3: Canonical Work Tables and Source Attribution

**Files:**
- Create: `convergence/reliability.py`
- Create: `tests/convergence/test_reliability.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/convergence/test_reliability.py
import pandas as pd

from convergence.reliability import (
    build_canonical_work_summary,
    build_source_record_reliability_table,
    build_source_reliability_summary,
    canonical_ids_from_records,
)


def test_canonical_ids_use_matches_df_before_fallback_key():
    records_by_source = {
        "openalex": [{"source_record_id": "oa_1", "doi": None, "external_ids": ["pmid:123"], "title": "Paper", "year": 2023, "record_type": "journal_article", "authors": [{"orcid": "0000-0001"}], "institutions": [{"ror": "https://ror.org/03yrm5c26"}]}],
        "scopus": [{"source_record_id": "sc_1", "doi": None, "external_ids": ["pmid:123"], "title": "Paper", "year": 2023, "record_type": "journal_article", "authors": [{"orcid": "0000-0001"}], "institutions": [{"ror": "https://ror.org/03yrm5c26"}]}],
    }
    matches_df = pd.DataFrame([{"source_a": "openalex", "source_b": "scopus", "record_id_a": "oa_1", "record_id_b": "sc_1", "match_key": "title_year_author", "confidence": 0.85}])
    mapping = canonical_ids_from_records(records_by_source, matches_df)
    assert mapping[("openalex", "oa_1")]["match_basis"] == "convergence_match"
    assert mapping[("openalex", "oa_1")]["canonical_work_id"] == mapping[("scopus", "sc_1")]["canonical_work_id"]


def test_source_record_table_keeps_attribution_fields():
    source_record_df = build_source_record_reliability_table(
        pd.DataFrame([{
            "canonical_work_id": "cw_1",
            "source": "scopus",
            "source_record_id": "sc_1",
            "record_type": "journal_article",
            "match_basis": "doi",
            "outcome_state": "reviewable_disputed",
            "confidence_band": "low",
            "flags": ["major_conflict", "doi_expected_missing"],
            "introduced_major_conflict": True,
            "introduced_weak_author_identity": False,
            "introduced_weak_institution_linkage": False,
            "introduced_doi_expected_missing": True,
            "has_external_corroboration": True,
            "has_major_conflict": True,
        }])
    )
    row = source_record_df.iloc[0]
    assert row["introduced_major_conflict"] is True
    assert row["introduced_doi_expected_missing"] is True


def test_source_summary_counts_unique_canonical_works_not_rows():
    work_df = pd.DataFrame(
        [
            {"canonical_work_id": "cw_1", "source": "openalex", "source_record_id": "oa_1", "record_type": "journal_article", "match_basis": "doi", "outcome_state": "integration_ready", "confidence_band": "high", "flags": [], "introduced_major_conflict": False, "introduced_weak_author_identity": False, "introduced_weak_institution_linkage": False, "introduced_doi_expected_missing": False, "has_external_corroboration": True, "has_major_conflict": False},
            {"canonical_work_id": "cw_1", "source": "openalex", "source_record_id": "oa_1_dup", "record_type": "journal_article", "match_basis": "doi", "outcome_state": "integration_ready", "confidence_band": "high", "flags": [], "introduced_major_conflict": False, "introduced_weak_author_identity": False, "introduced_weak_institution_linkage": False, "introduced_doi_expected_missing": False, "has_external_corroboration": True, "has_major_conflict": False},
            {"canonical_work_id": "cw_2", "source": "openalex", "source_record_id": "oa_2", "record_type": "thesis", "match_basis": "fallback", "outcome_state": "not_integration_ready", "confidence_band": "low", "flags": ["unverifiable_author_identity"], "introduced_major_conflict": False, "introduced_weak_author_identity": True, "introduced_weak_institution_linkage": True, "introduced_doi_expected_missing": False, "has_external_corroboration": False, "has_major_conflict": False},
        ]
    )
    summary = build_source_reliability_summary(work_df)
    overall = summary[(summary["source"] == "openalex") & (summary["record_type"] == "__all__")].iloc[0]
    assert overall["canonical_works"] == 2


def test_canonical_work_summary_dedupes_sources():
    work_df = pd.DataFrame(
        [
            {"canonical_work_id": "cw_1", "source": "openalex", "source_record_id": "oa_1", "record_type": "journal_article", "match_basis": "doi", "outcome_state": "integration_ready", "confidence_band": "high", "flags": [], "introduced_major_conflict": False, "introduced_weak_author_identity": False, "introduced_weak_institution_linkage": False, "introduced_doi_expected_missing": False, "has_external_corroboration": True, "has_major_conflict": False},
            {"canonical_work_id": "cw_1", "source": "scopus", "source_record_id": "sc_1", "record_type": "journal_article", "match_basis": "doi", "outcome_state": "reviewable_disputed", "confidence_band": "low", "flags": ["major_conflict"], "introduced_major_conflict": True, "introduced_weak_author_identity": False, "introduced_weak_institution_linkage": False, "introduced_doi_expected_missing": False, "has_external_corroboration": True, "has_major_conflict": True},
        ]
    )
    canonical = build_canonical_work_summary(work_df)
    assert len(canonical) == 1
    assert canonical.iloc[0]["n_sources"] == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/convergence/test_reliability.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'convergence.reliability'`

- [ ] **Step 3: Implement canonical ID assignment and rollups**

```python
# convergence/reliability.py
from __future__ import annotations

import hashlib

import pandas as pd

from convergence.external_validation import external_corroboration_for_work
from convergence.reliability_rules import (
    classify_author_strength,
    classify_institution_strength,
    classify_locator_strength,
    confidence_band_for_work,
    flags_for_work,
    outcome_state_for_work,
)


def _fallback_canonical_key(record: dict) -> str:
    if record.get("doi"):
        return f"doi::{record['doi'].strip().lower()}"
    external_ids = sorted(record.get("external_ids") or [])
    if external_ids:
        return f"external::{external_ids[0].strip().lower()}"
    raw = "|".join(
        [
            str(record.get("title") or "").strip().lower(),
            str(record.get("year") or ""),
            str((record.get("authors") or [{}])[0].get("orcid") or (record.get("authors") or [{}])[0].get("name") or "").strip().lower(),
            str((record.get("institutions") or [{}])[0].get("ror") or (record.get("institutions") or [{}])[0].get("name") or "").strip().lower(),
        ]
    )
    return "fallback::" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def canonical_ids_from_records(records_by_source: dict[str, list[dict]], matches_df: pd.DataFrame | None) -> dict[tuple[str, str], dict]:
    mapping: dict[tuple[str, str], dict] = {}
    if matches_df is not None and not matches_df.empty:
        for idx, row in matches_df.reset_index(drop=True).iterrows():
            canonical_id = f"match::{idx}"
            mapping[(str(row["source_a"]), str(row["record_id_a"]))] = {"canonical_work_id": canonical_id, "match_basis": "convergence_match"}
            mapping[(str(row["source_b"]), str(row["record_id_b"]))] = {"canonical_work_id": canonical_id, "match_basis": "convergence_match"}
    for source, records in records_by_source.items():
        for record in records:
            key = (source, str(record.get("source_record_id", "")))
            mapping.setdefault(
                key,
                {
                    "canonical_work_id": _fallback_canonical_key(record),
                    "match_basis": "doi" if record.get("doi") else ("external_id" if record.get("external_ids") else "fallback"),
                },
            )
    return mapping
```

- [ ] **Step 4: Extend the module to build the three in-memory tables**

Add these functions and keep their outputs aligned to the Output Contracts section:

```python
def build_source_record_reliability_table(work_df: pd.DataFrame) -> pd.DataFrame:
    return work_df[[
        "canonical_work_id", "source", "source_record_id", "record_type", "match_basis",
        "outcome_state", "confidence_band", "has_external_corroboration", "has_major_conflict",
        "introduced_major_conflict", "introduced_weak_author_identity",
        "introduced_weak_institution_linkage", "introduced_doi_expected_missing", "flags",
    ]].copy()


def build_canonical_work_summary(work_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for canonical_work_id, grp in work_df.groupby("canonical_work_id"):
        rows.append(
            {
                "canonical_work_id": canonical_work_id,
                "record_type": grp["record_type"].mode().iloc[0],
                "n_sources": grp["source"].nunique(),
                "sources": sorted(grp["source"].unique().tolist()),
                "outcome_state": "reviewable_disputed" if grp["has_major_conflict"].any() else grp["outcome_state"].mode().iloc[0],
                "confidence_band": "low" if grp["has_major_conflict"].any() else grp["confidence_band"].mode().iloc[0],
                "has_external_corroboration": grp["has_external_corroboration"].any(),
                "has_major_conflict": grp["has_major_conflict"].any(),
                "conflict_fields": ["major_conflict"] if grp["has_major_conflict"].any() else [],
                "flags": sorted({flag for flags in grp["flags"] for flag in flags}),
            }
        )
    return pd.DataFrame(rows)


def build_source_reliability_summary(source_record_df: pd.DataFrame, canonical_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if source_record_df.empty or canonical_df.empty:
        return pd.DataFrame()
    source_work_view = (
        source_record_df.drop_duplicates(subset=["source", "canonical_work_id"])[["source", "canonical_work_id"]]
        .merge(
            canonical_df[["canonical_work_id", "record_type", "outcome_state", "confidence_band", "has_external_corroboration", "has_major_conflict", "flags"]],
            on="canonical_work_id",
            how="left",
        )
    )
    for source, src_df in source_work_view.groupby("source"):
        grouped = list(src_df.groupby("record_type")) + [("__all__", src_df)]
        for record_type, grp in grouped:
            doi_gap = grp["flags"].apply(lambda flags: "doi_expected_missing" in flags)
            rows.append(
                {
                    "source": source,
                    "record_type": record_type,
                    "canonical_works": grp["canonical_work_id"].nunique(),
                    "integration_ready_share": (grp["outcome_state"] == "integration_ready").mean(),
                    "reviewable_disputed_share": (grp["outcome_state"] == "reviewable_disputed").mean(),
                    "not_integration_ready_share": (grp["outcome_state"] == "not_integration_ready").mean(),
                    "high_confidence_share": (grp["confidence_band"] == "high").mean(),
                    "medium_confidence_share": (grp["confidence_band"] == "medium").mean(),
                    "low_confidence_share": (grp["confidence_band"] == "low").mean(),
                    "external_corroboration_share": grp["has_external_corroboration"].astype(float).mean(),
                    "major_conflict_share": grp["has_major_conflict"].astype(float).mean(),
                    "doi_expected_missing_share": float(doi_gap.mean()) if len(grp) else 0.0,
                }
            )
    return pd.DataFrame(rows)
```

- [ ] **Step 5: Run the focused tests**

Run: `pytest tests/convergence/test_reliability.py -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add convergence/reliability.py tests/convergence/test_reliability.py
git commit -m "feat(reliability): add canonical work and attribution tables"
```

---

## Task 4: Runner, Exporter, and Report

**Files:**
- Create: `run_reliability.py`
- Create: `outputs/reports/reliability.py`
- Modify: `outputs/dataset/exporter.py`
- Create: `tests/outputs/test_reliability_exporter.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/outputs/test_reliability_exporter.py
from pathlib import Path

import pandas as pd

from outputs.dataset.exporter import DatasetExporter


def test_export_reliability_outputs_writes_expected_files(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)
    source_record_df = pd.DataFrame([{
        "canonical_work_id": "cw_1",
        "source": "openalex",
        "source_record_id": "oa_1",
        "record_type": "journal_article",
        "match_basis": "doi",
        "outcome_state": "integration_ready",
        "confidence_band": "high",
        "has_external_corroboration": True,
        "has_major_conflict": False,
        "introduced_major_conflict": False,
        "introduced_weak_author_identity": False,
        "introduced_weak_institution_linkage": False,
        "introduced_doi_expected_missing": False,
        "flags": [],
    }])
    canonical_df = pd.DataFrame([{
        "canonical_work_id": "cw_1",
        "record_type": "journal_article",
        "n_sources": 1,
        "sources": ["openalex"],
        "outcome_state": "integration_ready",
        "confidence_band": "high",
        "has_external_corroboration": True,
        "has_major_conflict": False,
        "conflict_fields": [],
        "flags": [],
    }])
    summary_df = pd.DataFrame([{
        "source": "openalex",
        "record_type": "__all__",
        "canonical_works": 1,
        "integration_ready_share": 1.0,
        "reviewable_disputed_share": 0.0,
        "not_integration_ready_share": 0.0,
        "high_confidence_share": 1.0,
        "medium_confidence_share": 0.0,
        "low_confidence_share": 0.0,
        "external_corroboration_share": 1.0,
        "major_conflict_share": 0.0,
        "doi_expected_missing_share": 0.0,
    }])
    paths = exporter.export_reliability_outputs(source_record_df, canonical_df, summary_df, run_id="2026-03-25")
    assert set(paths) == {"source_records", "canonical", "summary", "flags", "report"}
    for path in paths.values():
        assert path.exists()


def test_report_mentions_top_downgrade_reasons(tmp_path):
    exporter = DatasetExporter(output_dir=tmp_path)
    source_record_df = pd.DataFrame([{
        "canonical_work_id": "cw_1",
        "source": "openalex",
        "source_record_id": "oa_1",
        "record_type": "journal_article",
        "match_basis": "doi",
        "outcome_state": "reviewable_disputed",
        "confidence_band": "low",
        "has_external_corroboration": True,
        "has_major_conflict": True,
        "introduced_major_conflict": True,
        "introduced_weak_author_identity": False,
        "introduced_weak_institution_linkage": False,
        "introduced_doi_expected_missing": False,
        "flags": ["major_conflict"],
    }])
    canonical_df = pd.DataFrame([{
        "canonical_work_id": "cw_1",
        "record_type": "journal_article",
        "n_sources": 1,
        "sources": ["openalex"],
        "outcome_state": "reviewable_disputed",
        "confidence_band": "low",
        "has_external_corroboration": True,
        "has_major_conflict": True,
        "conflict_fields": ["publication_year"],
        "flags": ["major_conflict"],
    }])
    summary_df = pd.DataFrame([{
        "source": "openalex",
        "record_type": "__all__",
        "canonical_works": 1,
        "integration_ready_share": 0.0,
        "reviewable_disputed_share": 1.0,
        "not_integration_ready_share": 0.0,
        "high_confidence_share": 0.0,
        "medium_confidence_share": 0.0,
        "low_confidence_share": 1.0,
        "external_corroboration_share": 1.0,
        "major_conflict_share": 1.0,
        "doi_expected_missing_share": 0.0,
    }])
    text = exporter.export_reliability_outputs(source_record_df, canonical_df, summary_df, run_id="2026-03-25")["report"].read_text()
    assert "Top Downgrade Reasons" in text
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/outputs/test_reliability_exporter.py -v`

Expected: FAIL because `DatasetExporter` has no `export_reliability_outputs()`

- [ ] **Step 3: Implement exporter and report builder**

```python
# outputs/reports/reliability.py
from __future__ import annotations

from datetime import datetime, timezone


def build_reliability_report(summary_df, flags_df, run_id: str) -> str:
    lines = [
        "# INEP Bibliometric Tool — Record Reliability Report",
        "",
        f"**Run:** `{run_id}`  ",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "> Reliability is reported as usable coverage, disputed coverage, and verification-risk shares. It is descriptive evidence and does not overwrite the composite fitness matrix in V1.",
        "",
        "## Source Summary",
        "",
        summary_df.to_markdown(index=False) if not summary_df.empty else "_No reliability summary available._",
        "",
        "## Top Downgrade Reasons",
        "",
        flags_df.sort_values(['source', 'record_type', 'share'], ascending=[True, True, False]).to_markdown(index=False)
        if not flags_df.empty else "_No downgrade reasons available._",
        "",
    ]
    return "\n".join(lines)
```

```python
# outputs/dataset/exporter.py
    def export_reliability_outputs(
        self,
        source_record_df: pd.DataFrame,
        canonical_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        run_id: str,
    ) -> dict[str, Path]:
        from outputs.reports.reliability import build_reliability_report

        source_record_path = self.output_dir / f"source_record_reliability_{run_id}.csv"
        canonical_path = self.output_dir / f"canonical_work_reliability_{run_id}.csv"
        summary_path = self.output_dir / f"source_reliability_summary_{run_id}.csv"
        flags_path = self.output_dir / f"source_reliability_flags_{run_id}.csv"
        report_path = self.output_dir / f"source_reliability_report_{run_id}.md"

        source_record_df.to_csv(source_record_path, index=False)
        canonical_df.to_csv(canonical_path, index=False)
        summary_df.to_csv(summary_path, index=False)

        deduped = source_record_df.drop_duplicates(subset=["source", "canonical_work_id"]).copy()
        exploded = deduped.copy()
        exploded["flags"] = exploded["flags"].apply(lambda flags: flags if isinstance(flags, list) else [])
        exploded = exploded.explode("flags").dropna(subset=["flags"])
        if exploded.empty:
            flags_df = pd.DataFrame(columns=["source", "record_type", "flag", "n_works", "denominator", "share"])
        else:
            flags_df = exploded.groupby(["source", "record_type", "flags"]).size().rename("n_works").reset_index()
            totals = deduped.groupby(["source", "record_type"])["canonical_work_id"].nunique().rename("denominator").reset_index()
            flags_df = flags_df.merge(totals, on=["source", "record_type"], how="left")
            flags_df["share"] = flags_df["n_works"] / flags_df["denominator"]
            flags_df = flags_df.rename(columns={"flags": "flag"})
        flags_df.to_csv(flags_path, index=False)

        report_path.write_text(build_reliability_report(summary_df, flags_df, run_id), encoding="utf-8")
        return {
            "source_records": source_record_path,
            "canonical": canonical_path,
            "summary": summary_path,
            "flags": flags_path,
            "report": report_path,
        }
```

- [ ] **Step 4: Implement the runner with fail-fast input handling**

```python
# run_reliability.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from connectors.api.crossref import CrossrefConnector
from convergence.reliability import (
    build_canonical_work_summary,
    build_source_record_reliability_table,
    build_source_reliability_summary,
)
from outputs.dataset.exporter import DatasetExporter

PROCESSED = Path("data/processed")


def _deserialise_nested(value):
    if not isinstance(value, str):
        return value
    value = value.strip()
    if not value or value[0] not in "[{":
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _load_latest_records() -> dict[str, list[dict]]:
    records_by_source = {}
    for path in sorted(PROCESSED.glob("records_*.parquet")):
        source = path.stem.split("_")[1]
        df = pd.read_parquet(path)
        for col in ["authors", "institutions", "fields", "funding", "patent_citations", "external_ids"]:
            if col in df.columns:
                df[col] = df[col].apply(_deserialise_nested)
        records_by_source[source] = df.to_dict(orient="records")
    if not records_by_source:
        raise FileNotFoundError("No records_*.parquet files found in data/processed. Run the harvest/export pipeline first.")
    return records_by_source
```

Continue the runner so it loads the most recent `matches_phase2_*.parquet`, builds the source-record table first, then builds the canonical-work table from it, and finally derives summary and flag outputs from the canonical-work table plus source attribution.

- [ ] **Step 5: Run focused tests and a smoke run**

Run: `pytest tests/outputs/test_reliability_exporter.py -v`

Expected: PASS

Run: `python run_reliability.py --run-id 2026-03-25-smoke`

Expected: writes `source_record_reliability_2026-03-25-smoke.csv`, `canonical_work_reliability_2026-03-25-smoke.csv`, `source_reliability_summary_2026-03-25-smoke.csv`, `source_reliability_flags_2026-03-25-smoke.csv`, and `source_reliability_report_2026-03-25-smoke.md`

- [ ] **Step 6: Commit**

```bash
git add run_reliability.py outputs/reports/reliability.py outputs/dataset/exporter.py tests/outputs/test_reliability_exporter.py
git commit -m "feat(reliability): export source and canonical reliability outputs"
```

---

## Task 5: Dashboard Loaders and Reliability Tab

**Files:**
- Modify: `dashboard/data_loader.py`
- Modify: `dashboard/app.py`
- Modify: `dashboard/tabs/__init__.py`
- Create: `dashboard/tabs/reliability.py`
- Modify: `tests/dashboard/test_data_loader.py`
- Create: `tests/dashboard/test_reliability_tab.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/dashboard/test_reliability_tab.py
import pandas as pd

from dashboard.tabs import reliability as reliability_tab


def test_layout_renders_empty_state_when_no_data():
    layout = reliability_tab.layout(pd.DataFrame(), pd.DataFrame())
    assert layout is not None


def test_outcome_share_chart_uses_expected_columns():
    summary_df = pd.DataFrame([{
        "source": "openalex",
        "record_type": "__all__",
        "canonical_works": 10,
        "integration_ready_share": 0.6,
        "reviewable_disputed_share": 0.2,
        "not_integration_ready_share": 0.2,
        "high_confidence_share": 0.4,
        "medium_confidence_share": 0.4,
        "low_confidence_share": 0.2,
        "external_corroboration_share": 0.6,
        "major_conflict_share": 0.2,
        "doi_expected_missing_share": 0.1,
    }])
    fig = reliability_tab._outcome_share_figure(summary_df)
    assert len(fig.data) == 3


def test_record_type_breakdown_figure_filters_out___all__():
    summary_df = pd.DataFrame([
        {"source": "openalex", "record_type": "__all__", "canonical_works": 10, "integration_ready_share": 0.6, "reviewable_disputed_share": 0.2, "not_integration_ready_share": 0.2, "high_confidence_share": 0.4, "medium_confidence_share": 0.4, "low_confidence_share": 0.2, "external_corroboration_share": 0.6, "major_conflict_share": 0.2, "doi_expected_missing_share": 0.1},
        {"source": "openalex", "record_type": "journal_article", "canonical_works": 8, "integration_ready_share": 0.75, "reviewable_disputed_share": 0.125, "not_integration_ready_share": 0.125, "high_confidence_share": 0.5, "medium_confidence_share": 0.375, "low_confidence_share": 0.125, "external_corroboration_share": 0.75, "major_conflict_share": 0.125, "doi_expected_missing_share": 0.125},
    ])
    fig = reliability_tab._record_type_figure(summary_df, metric="integration_ready_share")
    assert fig.data
```

```python
# tests/dashboard/test_data_loader.py
def test_load_source_reliability_summary_reads_latest(tmp_path):
    path = tmp_path / "source_reliability_summary_2026-03-25.csv"
    pd.DataFrame([{"source": "openalex", "record_type": "__all__", "canonical_works": 1}]).to_csv(path, index=False)
    df = load_source_reliability_summary(csv_dir=tmp_path)
    assert len(df) == 1


def test_load_source_reliability_flags_reads_latest(tmp_path):
    path = tmp_path / "source_reliability_flags_2026-03-25.csv"
    pd.DataFrame([{"source": "openalex", "record_type": "journal_article", "flag": "major_conflict", "n_works": 1, "denominator": 2, "share": 0.5}]).to_csv(path, index=False)
    df = load_source_reliability_flags(csv_dir=tmp_path)
    assert len(df) == 1
    assert "flag" in df.columns
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/dashboard/test_data_loader.py tests/dashboard/test_reliability_tab.py -v`

Expected: FAIL because the loaders and new tab do not exist

- [ ] **Step 3: Implement data loaders for summary and flags**

```python
# dashboard/data_loader.py
SOURCE_RELIABILITY_COLUMNS = [
    "source", "record_type", "canonical_works",
    "integration_ready_share", "reviewable_disputed_share", "not_integration_ready_share",
    "high_confidence_share", "medium_confidence_share", "low_confidence_share",
    "external_corroboration_share", "major_conflict_share", "doi_expected_missing_share",
]
FLAG_COLUMNS = ["source", "record_type", "flag", "n_works", "denominator", "share"]


def load_source_reliability_summary(csv_dir: Path | None = None) -> pd.DataFrame:
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(csv_dir.glob("source_reliability_summary_*.csv"))
    if not files:
        return pd.DataFrame(columns=SOURCE_RELIABILITY_COLUMNS)
    return _ensure_columns(pd.read_csv(files[-1]), SOURCE_RELIABILITY_COLUMNS)


def load_source_reliability_flags(csv_dir: Path | None = None) -> pd.DataFrame:
    csv_dir = Path(csv_dir) if csv_dir else _DEFAULT_PROCESSED
    files = sorted(csv_dir.glob("source_reliability_flags_*.csv"))
    if not files:
        return pd.DataFrame(columns=FLAG_COLUMNS)
    return _ensure_columns(pd.read_csv(files[-1]), FLAG_COLUMNS)
```

- [ ] **Step 4: Implement the reliability tab with overall plus record-type drill-down**

```python
# dashboard/tabs/reliability.py
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html


def layout(summary_df: pd.DataFrame, flags_df: pd.DataFrame) -> html.Div:
    if summary_df.empty:
        return html.Div(
            [
                html.H4("Reliability"),
                html.P("No reliability outputs found. Run `python run_reliability.py` first.", className="text-muted"),
            ]
        )
    overall = summary_df[summary_df["record_type"] == "__all__"]
    return html.Div(
        [
            html.H4("Reliability — Usable Coverage and Verification Risk"),
            html.P("Overall shares use all canonical works surfaced by the source. Record-type charts use within-type denominators.", className="text-muted"),
            dcc.Graph(figure=_outcome_share_figure(overall)),
            dcc.Graph(figure=_record_type_figure(summary_df, metric="integration_ready_share")),
            dcc.Graph(figure=_record_type_figure(summary_df, metric="doi_expected_missing_share")),
            dcc.Graph(figure=_flag_share_figure(flags_df)),
        ]
    )


def _outcome_share_figure(summary_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if summary_df.empty:
        fig.update_layout(title="No reliability summary data")
        return fig
    fig.add_bar(name="integration_ready", x=summary_df["source"], y=summary_df["integration_ready_share"])
    fig.add_bar(name="reviewable_disputed", x=summary_df["source"], y=summary_df["reviewable_disputed_share"])
    fig.add_bar(name="not_integration_ready", x=summary_df["source"], y=summary_df["not_integration_ready_share"])
    fig.update_layout(barmode="stack", title="Outcome state share by source", yaxis_tickformat=".0%")
    return fig


def _record_type_figure(summary_df: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    data = summary_df[summary_df["record_type"] != "__all__"]
    if data.empty:
        fig.update_layout(title="No record-type reliability data")
        return fig
    for source, grp in data.groupby("source"):
        fig.add_bar(name=source, x=grp["record_type"], y=grp[metric])
    fig.update_layout(barmode="group", title=f"{metric} by source and record type", yaxis_tickformat=".0%")
    return fig
```

Wire the new loaders into `dashboard/app.py`, register a `Reliability` tab, and expose the module from `dashboard/tabs/__init__.py`.

- [ ] **Step 5: Run focused dashboard tests and an import smoke test**

Run: `pytest tests/dashboard/test_data_loader.py tests/dashboard/test_reliability_tab.py -v`

Expected: PASS

Run: `python -c "import dashboard.app as app; print(len(app._reliability_summary_df), len(app._reliability_flags_df))"`

Expected: prints two integers and exits 0

- [ ] **Step 6: Commit**

```bash
git add dashboard/data_loader.py dashboard/app.py dashboard/tabs/__init__.py dashboard/tabs/reliability.py tests/dashboard/test_data_loader.py tests/dashboard/test_reliability_tab.py
git commit -m "feat(dashboard): add reliability tab"
```

---

## Verification Checklist

- [ ] `pytest tests/convergence/test_reliability_rules.py -v`
- [ ] `pytest tests/convergence/test_external_validation.py -v`
- [ ] `pytest tests/convergence/test_reliability.py -v`
- [ ] `pytest tests/outputs/test_reliability_exporter.py -v`
- [ ] `pytest tests/dashboard/test_data_loader.py tests/dashboard/test_reliability_tab.py -v`
- [ ] `pytest tests/ -q`
- [ ] `python run_reliability.py --run-id <smoke-run-id>`
- [ ] `python -c "import dashboard.app as app; print(len(app._reliability_summary_df), len(app._reliability_flags_df))"`

---

## Handoff

Implement tasks in order. Do not start Task 2 until Task 1 is green, and do not wire the dashboard until reliability CSV outputs are stable. Keep commits scoped to the task boundaries above so review and rollback stay easy.
