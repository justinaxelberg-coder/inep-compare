from __future__ import annotations

DOI_EXPECTED_TYPES = {
    "journal_article",
    "conference_paper",
    "repository_postprint",
    "report",
}

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

INTEGRATION_READY = "integration_ready"
REVIEWABLE_DISPUTED = "reviewable_disputed"
NOT_INTEGRATION_READY = "not_integration_ready"

HIGH_CONFIDENCE = "high"
MEDIUM_CONFIDENCE = "medium"
LOW_CONFIDENCE = "low"


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
    for institution in work.get("institutions") or []:
        if institution.get("ror"):
            return INSTITUTION_GOLD
    for institution in work.get("institutions") or []:
        if institution.get("matched_name") or institution.get("name"):
            return INSTITUTION_WEAK
    return INSTITUTION_NONE


def flags_for_work(
    work: dict,
    has_external_corroboration: bool,
    has_major_conflict: bool,
    *,
    has_verifiable_author: bool | None = None,
    has_verifiable_institution: bool | None = None,
    has_stable_locator: bool | None = None,
    work_identity_resolved: bool | None = None,
) -> set[str]:
    flags: set[str] = set()

    locator_strength = classify_locator_strength(work)
    author_strength = classify_author_strength(work)
    institution_strength = classify_institution_strength(work)

    if has_stable_locator is None:
        has_stable_locator = locator_strength != LOCATOR_NONE
    if has_verifiable_author is None:
        has_verifiable_author = author_strength != AUTHOR_NONE
    if has_verifiable_institution is None:
        has_verifiable_institution = institution_strength != INSTITUTION_NONE
    if work_identity_resolved is None:
        work_identity_resolved = bool(work.get("title")) and bool(work.get("year"))

    if has_major_conflict:
        flags.add("major_conflict")

    if not has_external_corroboration:
        flags.add("no_external_corroboration")

    if not has_stable_locator:
        flags.add("no_stable_locator")

    if not has_verifiable_author:
        flags.add("unverifiable_author_identity")
    elif author_strength == AUTHOR_WEAK:
        flags.add("weak_author_identity")

    if not has_verifiable_institution:
        flags.add("unverifiable_institution_linkage")
    elif institution_strength == INSTITUTION_WEAK:
        flags.add("weak_institution_linkage")

    if not work_identity_resolved:
        flags.add("unresolved_work_identity")

    if not (work.get("title") and work.get("year") and work.get("record_type")):
        flags.add("missing_critical_verifiability_fields")

    if is_doi_expected(work.get("record_type")) and not work.get("doi"):
        flags.add("doi_expected_missing")

    if (
        not has_external_corroboration
        and has_stable_locator
        and has_verifiable_author
        and has_verifiable_institution
        and work_identity_resolved
    ):
        flags.add("external_visibility_gap")

    return flags


def outcome_state_for_work(
    *,
    flags: set[str] | frozenset[str] | list[str] | tuple[str, ...],
    has_external_corroboration: bool,
    has_verifiable_author: bool,
    has_verifiable_institution: bool,
    has_stable_locator: bool,
    work_identity_resolved: bool,
) -> str:
    flag_set = set(flags or ())

    if (
        "missing_critical_verifiability_fields" in flag_set
        or not work_identity_resolved
        or not has_stable_locator
        or not has_verifiable_author
        or not has_verifiable_institution
    ):
        return NOT_INTEGRATION_READY

    if "major_conflict" in flag_set:
        return REVIEWABLE_DISPUTED

    if not has_external_corroboration:
        return NOT_INTEGRATION_READY

    return INTEGRATION_READY


def confidence_band_for_work(
    *,
    flags: set[str] | frozenset[str] | list[str] | tuple[str, ...],
    has_external_corroboration: bool,
    has_major_conflict: bool,
    locator_strength: str,
    author_strength: str,
    institution_strength: str,
) -> str:
    flag_set = set(flags or ())
    major_conflict = has_major_conflict or ("major_conflict" in flag_set)

    if major_conflict:
        return LOW_CONFIDENCE

    if not has_external_corroboration:
        return LOW_CONFIDENCE

    if (
        locator_strength == LOCATOR_UNIVERSAL
        and author_strength == AUTHOR_GOLD
        and institution_strength == INSTITUTION_GOLD
    ):
        return HIGH_CONFIDENCE

    strong_signals = 0
    if locator_strength in {LOCATOR_UNIVERSAL, LOCATOR_EXTERNAL}:
        strong_signals += 1
    if author_strength in {AUTHOR_GOLD, AUTHOR_INTERNAL, AUTHOR_WEAK}:
        strong_signals += 1
    if institution_strength in {INSTITUTION_GOLD, INSTITUTION_WEAK}:
        strong_signals += 1

    if strong_signals >= 2:
        return MEDIUM_CONFIDENCE

    return LOW_CONFIDENCE
