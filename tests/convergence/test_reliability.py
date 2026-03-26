import pandas as pd

from convergence.reliability import (
    build_canonical_work_summary,
    build_source_record_reliability_table,
    build_source_reliability_summary,
    canonical_ids_from_records,
)


def test_canonical_ids_use_matches_df_before_fallback_key():
    records_by_source = {
        "openalex": [
            {
                "source_record_id": "oa_1",
                "doi": None,
                "external_ids": ["pmid:123"],
                "title": "Paper",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            }
        ],
        "scopus": [
            {
                "source_record_id": "sc_1",
                "doi": None,
                "external_ids": ["pmid:123"],
                "title": "Paper",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            }
        ],
    }
    matches_df = pd.DataFrame(
        [
            {
                "source_a": "openalex",
                "source_b": "scopus",
                "record_id_a": "oa_1",
                "record_id_b": "sc_1",
                "match_key": "title_year_author",
                "confidence": 0.85,
            }
        ]
    )

    mapping = canonical_ids_from_records(records_by_source, matches_df)

    assert mapping[("openalex", "oa_1")]["match_basis"] == "convergence_match"
    assert mapping[("openalex", "oa_1")]["canonical_work_id"] == mapping[("scopus", "sc_1")]["canonical_work_id"]


def test_source_record_table_keeps_attribution_fields():
    source_record_df = build_source_record_reliability_table(
        pd.DataFrame(
            [
                {
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
                }
            ]
        )
    )

    row = source_record_df.iloc[0]

    assert row["introduced_major_conflict"] is True
    assert row["introduced_doi_expected_missing"] is True


def test_canonical_ids_collapse_on_any_shared_external_id():
    records_by_source = {
        "openalex": [
            {
                "source_record_id": "oa_1",
                "doi": None,
                "external_ids": ["zdb:999", "pmid:123"],
                "title": "Paper",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            }
        ],
        "scopus": [
            {
                "source_record_id": "sc_1",
                "doi": None,
                "external_ids": ["arxiv:abc", "pmid:123"],
                "title": "Paper",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            }
        ],
    }

    mapping = canonical_ids_from_records(records_by_source, matches_df=None)

    assert mapping[("openalex", "oa_1")]["match_basis"] == "external_id"
    assert mapping[("openalex", "oa_1")]["canonical_work_id"] == mapping[("scopus", "sc_1")]["canonical_work_id"]


def test_fallback_grouping_does_not_merge_distinct_matched_components():
    records_by_source = {
        "openalex": [
            {
                "source_record_id": "oa_1",
                "doi": None,
                "external_ids": [],
                "title": "Shared title",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            },
            {
                "source_record_id": "oa_2",
                "doi": None,
                "external_ids": [],
                "title": "Shared title",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            },
        ],
        "scopus": [
            {
                "source_record_id": "sc_1",
                "doi": None,
                "external_ids": [],
                "title": "Shared title",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            },
            {
                "source_record_id": "sc_2",
                "doi": None,
                "external_ids": [],
                "title": "Shared title",
                "year": 2023,
                "record_type": "journal_article",
                "authors": [{"orcid": "0000-0001"}],
                "institutions": [{"ror": "https://ror.org/03yrm5c26"}],
            },
        ],
    }
    matches_df = pd.DataFrame(
        [
            {
                "source_a": "openalex",
                "source_b": "scopus",
                "record_id_a": "oa_1",
                "record_id_b": "sc_1",
                "match_key": "title_year_author",
                "confidence": 0.85,
            },
            {
                "source_a": "openalex",
                "source_b": "scopus",
                "record_id_a": "oa_2",
                "record_id_b": "sc_2",
                "match_key": "title_year_author",
                "confidence": 0.85,
            },
        ]
    )

    mapping = canonical_ids_from_records(records_by_source, matches_df)

    assert mapping[("openalex", "oa_1")]["canonical_work_id"] == mapping[("scopus", "sc_1")]["canonical_work_id"]
    assert mapping[("openalex", "oa_2")]["canonical_work_id"] == mapping[("scopus", "sc_2")]["canonical_work_id"]
    assert mapping[("openalex", "oa_1")]["canonical_work_id"] != mapping[("openalex", "oa_2")]["canonical_work_id"]


def test_source_summary_counts_unique_canonical_works_not_rows():
    work_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "flags": [],
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "has_external_corroboration": True,
                "has_major_conflict": False,
            },
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1_dup",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "flags": [],
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "has_external_corroboration": True,
                "has_major_conflict": False,
            },
            {
                "canonical_work_id": "cw_2",
                "source": "openalex",
                "source_record_id": "oa_2",
                "record_type": "thesis",
                "match_basis": "fallback",
                "outcome_state": "not_integration_ready",
                "confidence_band": "low",
                "flags": ["unverifiable_author_identity"],
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": True,
                "introduced_weak_institution_linkage": True,
                "introduced_doi_expected_missing": False,
                "has_external_corroboration": False,
                "has_major_conflict": False,
            },
        ]
    )

    summary = build_source_reliability_summary(work_df)
    overall = summary[(summary["source"] == "openalex") & (summary["record_type"] == "__all__")].iloc[0]

    assert overall["canonical_works"] == 2


def test_canonical_work_summary_dedupes_sources():
    work_df = pd.DataFrame(
        [
            {
                "canonical_work_id": "cw_1",
                "source": "openalex",
                "source_record_id": "oa_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "integration_ready",
                "confidence_band": "high",
                "flags": [],
                "introduced_major_conflict": False,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "has_external_corroboration": True,
                "has_major_conflict": False,
            },
            {
                "canonical_work_id": "cw_1",
                "source": "scopus",
                "source_record_id": "sc_1",
                "record_type": "journal_article",
                "match_basis": "doi",
                "outcome_state": "reviewable_disputed",
                "confidence_band": "low",
                "flags": ["major_conflict"],
                "introduced_major_conflict": True,
                "introduced_weak_author_identity": False,
                "introduced_weak_institution_linkage": False,
                "introduced_doi_expected_missing": False,
                "has_external_corroboration": True,
                "has_major_conflict": True,
            },
        ]
    )

    canonical = build_canonical_work_summary(work_df)

    assert len(canonical) == 1
    assert canonical.iloc[0]["n_sources"] == 2
