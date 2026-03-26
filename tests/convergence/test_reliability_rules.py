from convergence.reliability_rules import (
    AUTHOR_GOLD,
    INSTITUTION_GOLD,
    LOCATOR_INTERNAL,
    LOCATOR_UNIVERSAL,
    LOW_CONFIDENCE,
    NOT_INTEGRATION_READY,
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
    assert band == LOW_CONFIDENCE


def test_missing_critical_verifiability_fields_block_readiness():
    outcome = outcome_state_for_work(
        flags={"missing_critical_verifiability_fields"},
        has_external_corroboration=True,
        has_verifiable_author=True,
        has_verifiable_institution=True,
        has_stable_locator=True,
        work_identity_resolved=True,
    )
    assert outcome == NOT_INTEGRATION_READY
