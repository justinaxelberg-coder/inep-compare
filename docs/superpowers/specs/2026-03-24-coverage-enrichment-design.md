# Coverage Enrichment — Design Spec
**Date:** 2026-03-24
**Project:** INEP Bibliometric Tool (SINAES source fitness evaluation)
**Status:** Approved for implementation planning

---

## 1. Objective

Extend the source fitness scorer with five new computed sub-dimensions that are currently stubbed or absent, and switch the phase 2 run to uncapped 2023-only data for better convergence analysis. All changes follow Option A (post-run enrichment pass): the existing phase 2 pipeline is untouched; a new `run_enrichment.py` runner and four modules under `enrichment/` enrich its outputs.

---

## 2. Run Configuration Changes

File: `config/sample_config.yaml`

- `years: [2023]` — single consolidated year; avoids timeliness noise, maximises citation consolidation
- `max_records` key removed entirely — uncapped; removes 500-record ceiling artifact from phase 2 pilot
- Sources: `openalex`, `dimensions`, `scopus` — all three retained; Scopus included for convergence even though SDG output is expected to be absent (see §4.1)

**Connector guard required (blocking):** `connectors/api/openalex.py` pagination loop `while len(records) < self.max_records` will raise `TypeError` when `max_records` is `None`. Fix: at connector initialisation, convert `None` to `float('inf')`:
```python
self.max_records = config.get("max_records") or float("inf")
```
Same guard must be applied to `connectors/api/dimensions.py` pagination.

Spotlight sample unchanged: UFABC, UNIFESP, UFPA, IFSP, PUC-Campinas (5 institutions, stratified by type and region).

---

## 3. New Sub-dimensions

| Dimension | Sub-dim | Weight | YAML update needed? | Source(s) | Method |
|---|---|---|---|---|---|
| `coverage` | `geographic_bias` | 0.10 | No (already in YAML, stubbed) | INEP Microdados | Coverage gap + output gap (§4.2) |
| `social_impact` | `sdg_coverage` | 0.25 | No (already in YAML, stubbed) | OpenAlex, Dimensions, Scopus (flag) | Per-goal rates SDG 1–17 (§4.1) |
| `innovation_link` | `nonacademic_coauth` | 0.10 (new) | **Yes — add to YAML** | OpenAlex | Detectability + volume + quality (§4.3) |
| `data_quality` | `deduplication` | 0.20 | No (already in YAML, never wired) | Convergence engine (existing) | Wire overlap CSV → scorer (§4.4) |
| `social_impact` | `diamond_oa` | 0.10 (new) | **Yes — add to YAML** | OpenAlex (existing fetch) | Scielo/Redalyc URL classifier + native OA status (§4.5) |

**YAML changes required in `config/scoring_weights.yaml`:**

*Current values → new values (delta shown):*

```yaml
# innovation_link — current sum = 1.00; add nonacademic_coauth and rebalance
innovation_link:
  npl_link_rate: 0.35        # was 0.40  (-0.05)
  patent_count_score: 0.25   # was 0.30  (-0.05)
  intl_family_score: 0.30    # unchanged
  nonacademic_coauth: 0.10   # NEW — sum still 1.00

# social_impact — current sum = 1.00; add diamond_oa and rebalance
social_impact:
  sdg_coverage: 0.25         # unchanged (was excluded from scorer; now active)
  oa_percentage: 0.20        # unchanged
  policy_citations: 0.15     # was 0.20  (-0.05)
  public_engagement: 0.15    # was 0.20  (-0.05)
  geographic_social_context: 0.15  # unchanged
  diamond_oa: 0.10           # NEW — sum still 1.00
```

**Score regression note:** activating `sdg_coverage` (weight 0.25, previously excluded from `_score_social_impact()`) will shift all social_impact scores. Sources with SDG data (OpenAlex, Dimensions) score higher; Scopus receives 0.0 for that sub-dim. Reducing `policy_citations` and `public_engagement` by 0.05 each partially offsets this. Additionally, adding `nonacademic_coauth` to `innovation_link` reduces existing patent sub-dim weights slightly. These shifts are intentional and correctly reflect source capability. See §10 (Migration Note) for handling of existing output files.

---

## 4. Module Designs

### 4.1 `enrichment/sdg.py` — SDG Coverage

**Data sources:**
- OpenAlex: `sustainable_development_goals` field (native, already in `select` list in `openalex.py`). Do **not** use `concepts` — deprecated in favour of `topics` and not in the current select list.
- Dimensions: `category_sdg` field (native SDG tagging) → normalise to `sdgs` in `normalize()`
- Scopus: attempt `sdg` field; if absent log `WARNING: Scopus SDG field not available — sdg_coverage will be 0.0 for Scopus` and write `{"scopus": {"sdg_available": false}}` to `data/processed/source_metadata.json` (atomic write: write to `.tmp` file then `os.replace()`)

**Computation:**
- Aggregate per source × per institution-type stratum: for each SDG goal 1–17, compute `rate = papers_tagged_sdgN / total_papers`
- **SDG convergence (matched set only):** the `matches_phase2_*.parquet` does not contain SDG tags — only `record_id_a` and `record_id_b`. To compare SDG assignments, `sdg.py` must:
  1. Load the full normalised records for both sources (from `data/processed/` or by re-querying with `select=["id","sustainable_development_goals"]` if not cached)
  2. Join matches on `record_id_a → openalex_id` and `record_id_b → dimensions_id`
  3. Compute `agreement_rate` per SDG goal on the joined set
  - Implementation note: prefer a lightweight re-query with `filter=openalex_id:({ids})` in batch rather than caching full records

**Output:** `data/processed/sdg_by_source_type_YYYY-MM-DD.csv`
- Columns: `source, inst_type, sdg_goal, sdg_label, rate, n_papers, n_tagged`

**Fitness scorer input:** `sdg_rate` = mean rate across SDG goals 1–17 (unweighted mean); passed as kwarg to `_score_social_impact()`

---

### 4.2 `enrichment/geographic.py` — Geographic Bias

**Data sources:**
- INEP Microdados: `registry/institutions.csv` (region distribution baseline, `faculty_with_phd`)
- Phase 2 output: institution-level publication counts per source (loaded from `overlap_phase2_*.csv` or summary CSV)

**Graceful degradation:** if `registry/institutions.csv` absent, log warning and return `None`; scorer treats `geographic_bias` as excluded sub-dim contributing 0.0 with `total_w` adjusted accordingly (consistent with existing excluded-sub-dim pattern in `_score_coverage()`).

**Two gap measures:**

*Coverage gap:*
```
expected_rate[region] = n_heis_in_region / total_heis          # from INEP
observed_rate[region] = n_heis_indexed_in_region / total_indexed  # from source
coverage_gap[region]  = observed_rate - expected_rate
```
Negative = source under-indexes that region.

*Output gap:*
```
output_gap[region] = mean(publications / faculty_with_phd) for institutions in region
```
Normalised by PhD faculty to remove size effect. Computed per source. If `faculty_with_phd` is 0 or NaN for an institution, exclude it from the mean.

**Composite `geographic_bias` score:**
```
score = 1 - mean(abs(coverage_gap[r]) for r in regions)
```
1.0 = perfectly proportional; lower = more biased. Clipped to [0.0, 1.0].

**Output:** `data/processed/geographic_coverage_YYYY-MM-DD.csv`
- Columns: `source, region, expected_rate, observed_rate, coverage_gap, mean_output_gap, n_heis_inep, n_heis_indexed`

---

### 4.3 `enrichment/coauthorship.py` — Non-academic Co-authorship

**Data source:** OpenAlex author affiliation objects.

**Connector change required (`connectors/api/openalex.py`):**
In `normalize()`, for each author in `authorships`, extract the `type` field from each affiliated institution:
```python
"affiliation_types": list({
    inst.get("type", "unknown")
    for inst in authorship.get("institutions", [])
    if inst.get("type")
})  # deduplicated list; empty list if no institutions
```
Non-academic types: `"company"`, `"government"`, `"nonprofit"`, `"facility"`, `"healthcare"`, `"other"`. Academic type: `"education"`. If author has both academic and non-academic affiliations, paper counts as having non-academic co-authorship.

**Three measures:**

| Measure | Definition | Feeds |
|---|---|---|
| Detectability | `papers_with_any_affil_type / total_papers` — can source identify affiliation types at all? | fitness scorer |
| Volume | `papers_with_nonacademic_coauthor / total_papers` per inst_type stratum | convergence report |
| Quality | `papers_with_ror_resolved_nonacademic / papers_with_nonacademic_coauthor` — are non-academic affiliations ROR-resolved vs free-text? Proxy: institution has a non-null `id` field in OpenAlex affiliations | fitness scorer |

**Zero-value guard:** if source returns 0 papers with any affiliation type (i.e. affiliation data unavailable), `detectability = 0.0`, `quality = 0.0`, `volume_rate = 0.0`. No crash.

**Fitness scorer composite:**
```python
nonacademic_coauth_score = 0.4 * detectability + 0.3 * volume_rate + 0.3 * quality_score
```
Hardcoded weights (not sub-sub-weights in YAML; the YAML entry `nonacademic_coauth: 0.10` is the weight of this composite score within `innovation_link`).

**Output:** `data/processed/nonacademic_coauth_YYYY-MM-DD.csv`
- Columns: `source, inst_type, detectability, volume_rate, quality_score, nonacademic_coauth_score, n_papers, n_with_nonacademic`

**The Lens:** deferred until API key available.

---

### 4.4 `scoring/fitness.py` — Deduplication Wiring

**Data source:** `data/processed/overlap_phase2_*.csv` — loaded into a DataFrame by `run_fitness.py` (already done in `_load_coverage()`). The overlap CSV has columns `source_a, source_b, overlap_pct_min`.

**Computation in `run_fitness.py`:**
```python
def _load_dedup_scores(overlap_df: pd.DataFrame) -> dict[str, float]:
    """Mean overlap_pct_min across all pairs involving each source."""
    scores = {}
    for source in pd.unique(overlap_df[["source_a","source_b"]].values.ravel()):
        mask = (overlap_df["source_a"] == source) | (overlap_df["source_b"] == source)
        scores[source] = overlap_df.loc[mask, "overlap_pct_min"].mean()
    return scores  # {source_id: float}
```

Pass `dedup_scores` dict to `FitnessScorer.build_profile()` as new kwarg `dedup_score: float = 0.0`.

**Scorer change:** `_score_data_quality()` signature gains `dedup_score: float = 0.0`; wires it into the `deduplication` sub-weight (already in YAML, was never used).

---

### 4.5 `enrichment/diamond_oa.py` — Diamond OA Classification

**Classification logic:**
```python
_DIAMOND_PATTERNS = ["scielo.br", "redalyc.org", "doaj.org/article", "ojs"]

def classify_oa(oa_status: str | None, pdf_url: str | None) -> str:
    if not oa_status or oa_status == "closed":
        return "closed"
    if oa_status == "diamond":           # OpenAlex native diamond tag — check first
        return "diamond"
    if pdf_url and any(p in pdf_url for p in _DIAMOND_PATTERNS):
        return "diamond"
    if oa_status == "gold":
        return "gold"
    if oa_status in ("green", "hybrid"):
        return oa_status
    return "unknown"
```

**Idempotency:** before writing `oa_type` column, check if it already exists; if so, skip (do not overwrite). Log info message either way.

**In-place enrichment:** reads `data/processed/oa_phase2_*.csv`, adds `oa_type` column, writes back to same path. Does **not** create a backup (file is reproducible from phase 2 outputs).

**Fitness scorer input:** `diamond_oa_rate = n_diamond / n_total`; new kwarg to `_score_social_impact()`.

---

## 5. New Entry Point — `run_enrichment.py`

```
python run_enrichment.py [--phase2-date YYYY-MM-DD] [--skip-sdg] [--skip-geo] [--skip-coauth] [--skip-diamond]
```

**File resolution:** `--phase2-date` uses the date suffix only (e.g. `2026-03-24`). Files are resolved as:
```python
pattern = f"data/processed/*phase2_{date}*.csv"  # or .parquet
```
If multiple files match the same date, latest by `mtime` wins. If no files match, exit with error: `"No phase 2 outputs found for date {date}. Run run_phase2.py first."`
If `--phase2-date` omitted, use the most recent file found by `mtime` across all `phase2_*.csv` files.

**Execution order:**
1. Validate phase 2 output files exist; exit early if not
2. `diamond_oa.py` — enriches OA file in-place (fast, no API calls)
3. `geographic.py` — reads INEP registry + phase 2 counts (no API calls)
4. `coauthorship.py` — OpenAlex affiliation query per institution
5. `sdg.py` — OpenAlex + Dimensions SDG query; Scopus attempt + flag
6. Write output CSVs with date suffix matching `--phase2-date`
7. Atomic update of `data/processed/source_metadata.json` (read → merge → write to `.tmp` → `os.replace()`)

---

## 6. Scorer Method Changes

Functions in `scoring/fitness.py` that require signature updates:

| Method | New parameter(s) |
|---|---|
| `build_profile()` | `dedup_score: float = 0.0`, `sdg_rate: float = 0.0`, `diamond_oa_rate: float = 0.0`, `geographic_bias: float = 0.0`, `nonacademic_coauth: float = 0.0` |
| `_score_coverage()` | `geographic_bias: float = 0.0` |
| `_score_data_quality()` | `dedup_score: float = 0.0` |
| `_score_social_impact()` | `sdg_rate: float = 0.0`, `diamond_oa_rate: float = 0.0` |
| `_score_innovation_link()` | `nonacademic_coauth: float = 0.0` |

`FitnessProfile` dataclass gains no new fields — sub-dimension scores are internal to the scorer; only the 7 top-level dimension scores are stored in the profile and written to CSV. Dashboard reads sub-dimension detail from enrichment CSVs directly (see §8).

---

## 7. Connector Changes

| Connector | Change |
|---|---|
| `connectors/api/openalex.py` | (a) `self.max_records = config.get("max_records") or float("inf")`; (b) `sustainable_development_goals` already in select — add parsing to `normalize()` as `sdgs: list[int]`; (c) add `affiliation_types: list[str]` per author as described in §4.3 |
| `connectors/api/dimensions.py` | (a) same `max_records` guard; (b) parse `category_sdg` → `sdgs: list[int]` in `normalize()` |
| `connectors/api/scopus.py` | Attempt SDG field; on absence write `sdg_available: false` flag to `source_metadata.json` via atomic write |

No changes to `convergence/matcher.py` or `run_phase2.py`.

---

## 8. Convergence Report Extension

`outputs/dataset/exporter.py` — new section added to phase 2 Markdown report:

- **SDG agreement table** (reads `sdg_by_source_type_*.csv`): per SDG goal, OpenAlex vs Dimensions agreement rate on matched set
- **Geographic coverage table** (reads `geographic_coverage_*.csv`): coverage gap per region per source
- Scopus SDG caveat: reads `source_metadata.json`; if `scopus.sdg_available == false`, adds note: _"⚠ Scopus SDG data not available via standard API (requires SciVal). sdg_coverage scored as 0.0."_

Both sections are conditional on enrichment files being present — no-op if absent.

---

## 9. Dashboard

No new tab. Existing tabs pick up enrichment data once files are present:

- `dashboard/data_loader.py` gains `load_sdg()`, `load_geographic()`, `load_coauth()` — each returns empty DataFrame if files absent
- `tabs/fitness.py` radar chart reads sub-dim detail from enrichment CSVs directly (not from `FitnessProfile`); gains `geographic_bias` and `sdg_coverage` as additional spokes when data available. The existing 7 top-level dimension spokes are unchanged.

---

## 10. Migration Note

Existing `data/processed/fitness_*.db` and `fitness_matrix_*.csv` from the phase 2 pilot run will have different `social_impact` scores once enrichment is applied (sdg_coverage and diamond_oa now contribute). These files should be regenerated by re-running `run_fitness.py` after `run_enrichment.py` completes. The SQLite table uses `if_exists="append"` — old rows are not deleted; re-run with the same `run_id` will produce duplicates. Recommendation: use a new `run_id` (date-stamped) for the 2023 uncapped run.

---

## 11. Testing

| Test file | N tests | Coverage |
|---|---|---|
| `tests/enrichment/test_sdg.py` | ≥8 | Per-goal rate computation; disagreement detection via join; Scopus flag written atomically; OpenAlex uses `sustainable_development_goals` not `concepts` |
| `tests/enrichment/test_geographic.py` | ≥6 | Coverage gap; output gap; faculty_with_phd=0 excluded; missing registry returns None gracefully |
| `tests/enrichment/test_coauthorship.py` | ≥6 | Detectability/volume/quality; zero when no affiliation types; multi-type author counted correctly |
| `tests/enrichment/test_diamond_oa.py` | ≥6 | URL pattern table; native `oa_status=="diamond"` path; idempotency (column exists → skip); existing `oa_rate` column unchanged |
| `tests/scoring/test_fitness.py` (extend) | ≥4 new | `dedup_score` wired; `sdg_rate` wired; `diamond_oa_rate` wired; `geographic_bias` wired |

**Target:** ≥30 new tests. All existing 176 tests continue to pass.

---

## 12. Out of Scope

- `timeliness` — remains hardcoded to 1.0; revisit after year-2 run
- `temporal_stability` — remains stubbed; requires multi-year comparison
- `equity_representation` — remains stubbed; definition deferred to working group
- The Lens non-academic coauthorship — deferred until API key available
- Full-population run (all 2,580 institutions) — deferred; enrichment designed for stratified sample
- Policy citations from Overton — deferred; coverage uncertain
