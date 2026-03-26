# Record Reliability and Usable Coverage — Design Spec
**Date:** 2026-03-25
**Project:** INEP Bibliometric Tool (source comparison for integration confidence)
**Status:** Reviewed; pending user approval

---

## 1. Objective

Define a record-level reliability framework that helps INEP judge how confidently each source can be integrated for mapping and evaluation. The design must make the tradeoff between coverage and accuracy visible instead of hiding it inside a single blended score.

The key question is not just "how many works does this source surface?" but "how much of that coverage is verifiable enough to integrate safely?".

This design therefore shifts reliability assessment toward canonical works, explicit verification rules, and source-level summaries derived from work-level evidence.

---

## 2. Decision Summary

This design follows a **record-first, source-rollup** approach.

- Reliability is judged first at the **canonical work** level.
- Source comparison remains **standalone**, not combination-based.
- The main outputs are **shares of works by reliability state and flag**, not a single source reliability score.
- `High-confidence` requires **external corroboration**.
- Missing metadata is usually treated as **uncertainty**, not a direct penalty, unless it blocks verification.
- Duplicate evidence is not ignored: duplicates can corroborate a work, reveal disputes, and attribute conflicts back to the source that introduced them.

This approach was chosen over:

1. `Source-only profile`
   - simpler, but too coarse to define usable coverage precisely
2. `Rule-based exclusion only`
   - operationally clear, but loses the confidence nuance needed to show the tradeoff frontier

---

## 3. Scope

This spec covers:

- work-level reliability assessment
- canonical work identity and matching hierarchy
- verification rules and evidence ladders
- outcome states and confidence bands
- multi-flag reliability diagnostics
- standalone source comparison outputs
- source attribution for conflicts and missing evidence

This spec does not yet cover:

- implementation plan or task decomposition
- exact storage schema for every new output table
- UI layouts beyond the reporting requirements in this document
- source-combination optimization
- automated resolution workflows for disputed works

---

## 4. Core Model

### 4.1 Unit of Analysis

The unit of analysis is the **canonical work**.

Coverage, usable coverage, and reliability summaries are all computed at the canonical-work level so that the same work is not counted repeatedly across sources. Duplicate records remain important evidence, but they do not inflate coverage totals.

### 4.2 Matching Hierarchy

Canonical work identity uses the following hierarchy:

1. DOI when present
2. Otherwise another external identifier
3. Otherwise strong fuzzy match on `title + year + author + institution`
4. Otherwise unresolved; do not assume equivalence

Duplicate records across or within sources should be retained as supporting evidence for the canonical work. They can:

- corroborate the work when they agree
- create a dispute when they disagree
- reveal overcounting or noisy metadata behavior

### 4.3 Standalone Source Comparison

Source comparison must be **standalone**.

The question being answered is: if INEP evaluates a specific source on its own, what canonical works does it surface, and how much of that surfaced coverage is integration-ready, disputed, or unusable?

This design explicitly does not optimize for combinations of sources in V1.

---

## 5. Verification Backbone

### 5.1 Universal Critical Fields

A canonical work can only be considered meaningfully verifiable when the following are available in verifiable form:

- institution linkage
- verifiable author
- publication year
- title or equivalent work label
- record type
- source provenance
- stable locator or persistent source-native ID

These are the minimum fields needed for audit, reinspection, and safe integration.

### 5.2 Evidence Ladders

The framework uses explicit evidence hierarchies instead of treating all identifiers as equivalent.

**Institution evidence**

- gold standard: persistent organization identifier such as ROR
- fallback: normalized institution name match

**Author evidence**

- gold standard: ORCID-linked author identity
- fallback: stable internal author identifier
- weak evidence: plain author string
- very weak evidence: incomplete or ambiguous string

**Record locator evidence**

- strongest: universal external identifier such as DOI or patent number with authority
- next: external but domain-bounded identifier
- much weaker: source-native internal identifier
- weakest: no stable locator

Richer metadata should be rewarded when it improves auditability and validation opportunity. It should not automatically be treated as correctness.

A persistent source-native ID is sufficient for **minimum auditability**, but not for strong portability or broad external verification. It therefore helps a work remain inspectable and may support `medium` confidence when paired with strong additional evidence, but it does not by itself support `high-confidence` or erase the need for external corroboration.

### 5.3 Expected DOI Rule

Missing DOI is not universally penalized across all record types.

The default guide is:

- `DOI expected`
  - journal articles
  - conference papers
  - many repository-hosted postprints or published versions
  - possibly some reports
- `DOI not universally expected`
  - theses
  - books
  - chapters
  - many policy documents
  - some local outputs

When DOI is expected but missing, the work should carry a specific reliability warning because it represents a verification and openness gap.

---

## 6. Reliability States and Confidence Bands

### 6.1 Outcome States

Each canonical work receives one primary `outcome_state`:

- `integration_ready`
  - verifiable enough for safe integration
- `reviewable_disputed`
  - verifiable, but materially conflicted and should remain visible for review
- `not_integration_ready`
  - too weakly evidenced or unverifiable for safe integration

These states are meant to support operational use by INEP rather than abstract source reputation scoring.

### 6.2 Confidence Bands

Each canonical work also receives a `confidence_band`:

- `high`
  - externally corroborated, no major unresolved conflicts, strong verification chain
- `medium`
  - verifiable and coherent, but weaker externally
- `low`
  - weakly evidenced, siloed, missing critical verification strength, or conflicted

`High-confidence` requires **external corroboration**. Strong internal evidence alone is not enough.

### 6.3 Relationship Between States and Bands

The outcome state and confidence band are related but distinct.

- a work may be `integration_ready` and still be `medium` confidence if it is usable but not strongly corroborated
- a work may be `reviewable_disputed` even if much of its evidence is otherwise strong
- a work that fails critical verification requirements becomes `not_integration_ready` regardless of partial strengths elsewhere

This separation keeps the model operational while preserving nuance.

---

## 7. Decision Rules

### 7.1 Integration Ready

A canonical work is `integration_ready` when:

- all universal critical fields are present in verifiable form
- the work has external corroboration
- there are no major unresolved conflicts on core facts
- only minor metadata variance remains

A source-native internal identifier may satisfy the minimum locator requirement for auditability, but it is not enough on its own to make a work `integration_ready`. A work with only internal locator strength must still be externally corroborated through other evidence to enter this state.

Minor metadata variance is acceptable and should not break external corroboration. Examples include:

- punctuation or casing changes
- subtitle truncation
- small title formatting differences
- explainable year drift such as online-first versus print year

### 7.2 Reviewable Disputed

A canonical work is `reviewable_disputed` when:

- the work is still verifiable
- but there is material disagreement from either:
  - an external validator
  - another source record tied to the same canonical work

This includes cases such as:

- duplicate records disagreeing on year or type
- strong identifiers attached to conflicting institution evidence
- strong identifiers attached to conflicting author identity
- externally corroborated work identity with contested key metadata

These works should not count as clean usable coverage, but they should remain visible for review and source attribution.

### 7.3 Not Integration Ready

A canonical work is `not_integration_ready` when one or more critical verification requirements fail. Examples include:

- no verifiable institution linkage
- no verifiable author identity
- no stable locator of any kind, meaning neither an external identifier nor a persistent source-native identifier
- unresolved work identity
- source provenance too weak to audit

Works with no external validator and no corroborating duplicate evidence should not be treated as `high-confidence`, and may fall here when they are effectively siloed and weakly evidenced.

---

## 8. Conflict Policy

Contradiction is worse than absence.

Strong identifiers increase trust only when they resolve to coherent facts. If a work has strong identifiers but materially conflicts with external or duplicate evidence, it should be sharply downgraded.

The following count as **major conflict fields**:

- institution
- author identity
- publication year
- title or work identity
- record type

Conflicts may be triggered by either:

- external validators
- other matched records for the same canonical work

Minor variance should remain allowed. The purpose is to distinguish normal metadata drift from true unreliability.

---

## 9. Reliability Flags

Each canonical work may carry multiple reliability flags. Flags are not mutually exclusive.

Initial flag set:

- `major_conflict`
- `missing_critical_verifiability_fields`
- `unverifiable_institution_linkage`
- `unverifiable_author_identity`
- `no_stable_locator`
- `unresolved_work_identity`
- `no_external_corroboration`
- `doi_expected_missing`
- `weak_author_identity`
- `weak_institution_linkage`
- `external_visibility_gap`

The purpose of the flags is diagnostic clarity. INEP should be able to see not only that a source adds lower-confidence works, but why.

---

## 10. Usable Coverage

The design distinguishes among:

- `coverage`
  - all canonical works surfaced by a source
- `usable coverage`
  - canonical works that are verifiable enough for safe integration
- `reviewable disputed coverage`
  - verifiable canonical works that should remain visible but should not count as clean usable coverage

At minimum, the following conditions should exclude a work from `usable coverage`:

- missing critical verifiability fields
- unverifiable institution linkage
- unverifiable author identity
- no stable locator of any kind, meaning neither an external identifier nor a persistent source-native identifier
- unresolved work identity

`Major conflict` should not be silently dropped. It should feed `reviewable_disputed` when the work remains verifiable.

---

## 11. Source Attribution

Although reliability is judged at the canonical-work level, accountability must remain source-specific.

For each canonical work, each source should be attributable for actions such as:

- providing externally corroborated metadata
- supplying critical verifiability fields
- introducing a conflict
- failing to provide expected DOI evidence
- contributing only weak author or institution identity

This makes it possible to say:

- which source surfaced the work
- which source made the work more trustworthy
- which source introduced the dispute

---

## 12. Reporting and Comparison Outputs

The main comparison should be **share-based**, not a single overall reliability score.

Unless otherwise noted, source-level shares use this denominator:

- all canonical works surfaced by that source

For `record_type` breakdowns, the denominator is:

- all canonical works of that `record_type` surfaced by that source

Metric-specific exceptions should be explicit:

- `DOI-expected missing DOI share`
  - denominator: canonical works surfaced by that source whose `record_type` falls in the `DOI expected` set
- any flag share not otherwise qualified
  - denominator: all canonical works surfaced by that source
- confidence-band share
  - denominator: all canonical works surfaced by that source
- outcome-state share
  - denominator: all canonical works surfaced by that source

For each source, report:

- total canonical works surfaced
- `integration_ready` share
- `reviewable_disputed` share
- `not_integration_ready` share
- `high / medium / low` confidence share
- externally corroborated share
- share with major conflicts
- share missing critical verifiability fields
- `DOI-expected` missing DOI share
- top downgrade reasons

The same outputs should also be broken down by `record_type`.

This allows statements such as:

- a source adds broad coverage, but much of the marginal gain is only medium-confidence
- a source adds less volume, but a larger share is integration-ready
- a source performs well for articles but poorly for theses or policy documents

---

## 13. Error Handling and Methodological Guardrails

The design should follow these guardrails:

- do not treat metadata richness alone as correctness
- do not collapse all reliability evidence into one opaque score
- do not silently deduplicate away disputes
- do not assume lack of metadata implies falsity unless verification is blocked
- do not award `high-confidence` without external corroboration
- do not count unresolved or unverifiable works as usable coverage

Where exact validation is impossible, the system should prefer explicit flags and conservative confidence rather than overstating trust.

---

## 14. Testing Requirements

Implementation planning should include tests that verify:

- canonical work matching follows the identifier hierarchy correctly
- duplicates corroborate or dispute without inflating coverage counts
- `high-confidence` is impossible without external corroboration
- missing non-critical metadata produces uncertainty rather than automatic exclusion
- missing critical verification fields exclude a work from usable coverage
- major conflicts on core fields trigger sharp downgrade behavior
- minor metadata variance does not break corroboration
- DOI-expected records without DOI are flagged correctly
- source attribution identifies which source introduced conflicts or weak evidence
- source summaries roll up correctly overall and by `record_type`

---

## 15. Open Implementation Questions

These questions should be resolved during implementation planning, not in this design stage:

- exact weighting or rule precedence for moving between `medium` and `low`
- exact tolerance rules for title and year variance
- exact list of external validators per record type
- exact storage and file formats for work-level and source-level outputs
- dashboard interaction details for inspecting disputed and flagged works

These are planning questions, not design blockers, because the core reliability framework is already defined.

---

## 16. Recommended Next Step

The next step is to write an implementation plan for:

1. canonical work reliability data model
2. work matching and evidence aggregation
3. outcome state and flag derivation
4. source-level rollups and reporting outputs
5. dashboard/report surfacing of usable coverage and reliability flags

That planning step should preserve the principles in this spec and avoid reducing the model to a single source score.
