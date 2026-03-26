from __future__ import annotations

import hashlib
from collections import Counter, defaultdict

import pandas as pd

from convergence.external_validation import _normalise_doi, _normalise_record_type, _normalise_title
from convergence.reliability_rules import (
    HIGH_CONFIDENCE,
    INTEGRATION_READY,
    LOW_CONFIDENCE,
    MEDIUM_CONFIDENCE,
    NOT_INTEGRATION_READY,
    REVIEWABLE_DISPUTED,
    is_doi_expected,
)

ALL_RECORD_TYPES = "__all__"
_OUTCOME_PRIORITY = {
    NOT_INTEGRATION_READY: 0,
    REVIEWABLE_DISPUTED: 1,
    INTEGRATION_READY: 2,
}
_CONFIDENCE_PRIORITY = {
    LOW_CONFIDENCE: 0,
    MEDIUM_CONFIDENCE: 1,
    HIGH_CONFIDENCE: 2,
}
_BOOL_COLUMNS = [
    "has_external_corroboration",
    "has_major_conflict",
    "introduced_major_conflict",
    "introduced_weak_author_identity",
    "introduced_weak_institution_linkage",
    "introduced_doi_expected_missing",
]


class _DisjointSet:
    def __init__(self) -> None:
        self.parent: dict[tuple[str, str], tuple[str, str]] = {}
        self.root_has_match: dict[tuple[str, str], bool] = {}

    def add(self, item: tuple[str, str]) -> None:
        self.parent.setdefault(item, item)
        self.root_has_match.setdefault(item, False)

    def find(self, item: tuple[str, str]) -> tuple[str, str]:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def mark_matched(self, item: tuple[str, str]) -> None:
        root = self.find(item)
        self.root_has_match[root] = True

    def can_union_as_fallback(self, left: tuple[str, str], right: tuple[str, str]) -> bool:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return False
        return not (self.root_has_match.get(left_root, False) and self.root_has_match.get(right_root, False))

    def union(self, left: tuple[str, str], right: tuple[str, str]) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if left_root <= right_root:
            self.parent[right_root] = left_root
            self.root_has_match[left_root] = self.root_has_match.get(left_root, False) or self.root_has_match.get(right_root, False)
            self.root_has_match.pop(right_root, None)
        else:
            self.parent[left_root] = right_root
            self.root_has_match[right_root] = self.root_has_match.get(left_root, False) or self.root_has_match.get(right_root, False)
            self.root_has_match.pop(left_root, None)


def _normalise_external_id(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip().lower() or None


def _first_author_identity(record: dict) -> str:
    authors = record.get("authors") or []
    if not authors:
        return ""
    first = authors[0]
    for field in ("orcid", "author_id", "name"):
        value = _normalise_external_id(first.get(field))
        if value:
            return value
    return ""


def _first_institution_identity(record: dict) -> str:
    institutions = record.get("institutions") or []
    if not institutions:
        return ""
    first = institutions[0]
    for field in ("ror", "matched_name", "name"):
        value = _normalise_external_id(first.get(field))
        if value:
            return value
    return ""


def _external_id_keys(record: dict) -> list[str]:
    return sorted(
        external_id
        for external_id in (_normalise_external_id(value) for value in (record.get("external_ids") or []))
        if external_id
    )


def _fallback_canonical_key(record: dict) -> tuple[str, str]:
    doi = _normalise_doi(record.get("doi"))
    if doi:
        return "doi", f"doi::{doi}"

    external_ids = _external_id_keys(record)
    if external_ids:
        return "external_id", f"external::{external_ids[0]}"

    raw = "|".join(
        [
            _normalise_title(record.get("title")) or "",
            str(record.get("year") or "").strip(),
            _first_author_identity(record),
            _first_institution_identity(record),
        ]
    )
    fallback_hash = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return "fallback", f"fallback::{fallback_hash}"


def canonical_ids_from_records(
    records_by_source: dict[str, list[dict]],
    matches_df: pd.DataFrame | None,
) -> dict[tuple[str, str], dict]:
    records_index: dict[tuple[str, str], dict] = {}
    components = _DisjointSet()
    matched_records: set[tuple[str, str]] = set()
    fallback_keys: dict[tuple[str, str], tuple[str, str]] = {}
    doi_groups: dict[str, list[tuple[str, str]]] = defaultdict(list)
    external_id_groups: dict[str, list[tuple[str, str]]] = defaultdict(list)
    fallback_hash_groups: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for source, records in sorted(records_by_source.items()):
        for record in records:
            source_record_id = str(record.get("source_record_id") or "").strip()
            if not source_record_id:
                continue
            key = (source, source_record_id)
            records_index[key] = record
            components.add(key)
            fallback_keys[key] = _fallback_canonical_key(record)
            basis, fallback_key = fallback_keys[key]
            if basis == "doi":
                doi_groups[fallback_key].append(key)
            elif basis == "external_id":
                for external_id in _external_id_keys(record):
                    external_id_groups[f"external::{external_id}"].append(key)
            else:
                fallback_hash_groups[fallback_key].append(key)

    if matches_df is not None and not matches_df.empty:
        for row in matches_df.to_dict("records"):
            left = (row.get("source_a"), row.get("record_id_a"))
            right = (row.get("source_b"), row.get("record_id_b"))
            if left not in records_index or right not in records_index:
                continue
            matched_records.add(left)
            matched_records.add(right)
            components.union(left, right)
            components.mark_matched(left)
            components.mark_matched(right)

    for fallback_groups in (doi_groups, external_id_groups, fallback_hash_groups):
        for members in fallback_groups.values():
            if len(members) < 2:
                continue
            first = members[0]
            for member in members[1:]:
                if components.can_union_as_fallback(first, member):
                    components.union(first, member)

    grouped_members: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for key in records_index:
        grouped_members[components.find(key)].append(key)

    sorted_groups = sorted(
        grouped_members.values(),
        key=lambda members: min(f"{source}|{record_id}" for source, record_id in members),
    )

    mapping: dict[tuple[str, str], dict] = {}
    for index, members in enumerate(sorted_groups, start=1):
        canonical_work_id = f"cw_{index}"
        for member in sorted(members):
            basis, fallback_key = fallback_keys[member]
            mapping[member] = {
                "canonical_work_id": canonical_work_id,
                "match_basis": "convergence_match" if member in matched_records else basis,
                "fallback_key": fallback_key,
            }

    return mapping


def build_source_record_reliability_table(work_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "canonical_work_id",
        "source",
        "source_record_id",
        "record_type",
        "match_basis",
        "outcome_state",
        "confidence_band",
        "has_external_corroboration",
        "has_major_conflict",
        "introduced_major_conflict",
        "introduced_weak_author_identity",
        "introduced_weak_institution_linkage",
        "introduced_doi_expected_missing",
        "flags",
    ]
    result = work_df[columns].copy()
    if "flags" in result.columns:
        result["flags"] = result["flags"].apply(lambda value: list(value) if isinstance(value, (list, tuple, set, frozenset)) else [])
    for column in _BOOL_COLUMNS:
        if column in result.columns:
            result[column] = result[column].map(bool).astype(object)
    return result


def _prefer_record_type(series: pd.Series) -> str | None:
    values = []
    for value in series:
        normalised = _normalise_record_type(value)
        if normalised:
            values.append(normalised)
    if not values:
        return None
    counts = Counter(values)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _select_outcome(values: pd.Series) -> str | None:
    valid = [value for value in values if value in _OUTCOME_PRIORITY]
    if not valid:
        return None
    return min(valid, key=lambda value: (_OUTCOME_PRIORITY[value], value))


def _select_confidence(values: pd.Series) -> str | None:
    valid = [value for value in values if value in _CONFIDENCE_PRIORITY]
    if not valid:
        return None
    return min(valid, key=lambda value: (_CONFIDENCE_PRIORITY[value], value))


def _merge_flags(values: pd.Series) -> list[str]:
    merged: set[str] = set()
    for value in values:
        if isinstance(value, (list, tuple, set, frozenset)):
            merged.update(value)
    return sorted(merged)


def _canonical_presence_table(source_record_df: pd.DataFrame, canonical_df: pd.DataFrame) -> pd.DataFrame:
    presence = source_record_df[["source", "canonical_work_id"]].drop_duplicates()
    return presence.merge(canonical_df, on="canonical_work_id", how="left", validate="many_to_one")


def build_canonical_work_summary(work_df: pd.DataFrame) -> pd.DataFrame:
    if work_df.empty:
        return pd.DataFrame(
            columns=[
                "canonical_work_id",
                "record_type",
                "n_sources",
                "sources",
                "source_record_ids",
                "match_bases",
                "outcome_state",
                "confidence_band",
                "flags",
                "has_external_corroboration",
                "has_major_conflict",
                "introduced_major_conflict",
                "introduced_weak_author_identity",
                "introduced_weak_institution_linkage",
                "introduced_doi_expected_missing",
                "doi_expected_missing",
            ]
        )

    rows: list[dict] = []
    for canonical_work_id, group in work_df.groupby("canonical_work_id", sort=True):
        flags = _merge_flags(group["flags"])
        record_type = _prefer_record_type(group["record_type"])
        introduced_doi_expected_missing = bool(group["introduced_doi_expected_missing"].fillna(False).any())
        doi_expected_missing = bool(
            is_doi_expected(record_type)
            and ("doi_expected_missing" in flags or introduced_doi_expected_missing)
        )
        rows.append(
            {
                "canonical_work_id": canonical_work_id,
                "record_type": record_type,
                "n_sources": int(group["source"].dropna().nunique()),
                "sources": sorted(group["source"].dropna().unique().tolist()),
                "source_record_ids": sorted(group["source_record_id"].dropna().astype(str).unique().tolist()),
                "match_bases": sorted(group["match_basis"].dropna().astype(str).unique().tolist()),
                "outcome_state": _select_outcome(group["outcome_state"]),
                "confidence_band": _select_confidence(group["confidence_band"]),
                "flags": flags,
                "has_external_corroboration": bool(group["has_external_corroboration"].fillna(False).any()),
                "has_major_conflict": bool(group["has_major_conflict"].fillna(False).any()),
                "introduced_major_conflict": bool(group["introduced_major_conflict"].fillna(False).any()),
                "introduced_weak_author_identity": bool(group["introduced_weak_author_identity"].fillna(False).any()),
                "introduced_weak_institution_linkage": bool(group["introduced_weak_institution_linkage"].fillna(False).any()),
                "introduced_doi_expected_missing": introduced_doi_expected_missing,
                "doi_expected_missing": doi_expected_missing,
            }
        )
    return pd.DataFrame(rows)


def _share(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def _build_summary_rows(presence_df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for source, source_group in presence_df.groupby("source", sort=True):
        for record_type, subset in [(ALL_RECORD_TYPES, source_group), *list(source_group.groupby("record_type", sort=True))]:
            canonical_works = int(subset["canonical_work_id"].nunique())
            integration_ready = int((subset["outcome_state"] == INTEGRATION_READY).sum())
            reviewable_disputed = int((subset["outcome_state"] == REVIEWABLE_DISPUTED).sum())
            not_integration_ready = int((subset["outcome_state"] == NOT_INTEGRATION_READY).sum())
            high_confidence = int((subset["confidence_band"] == HIGH_CONFIDENCE).sum())
            medium_confidence = int((subset["confidence_band"] == MEDIUM_CONFIDENCE).sum())
            low_confidence = int((subset["confidence_band"] == LOW_CONFIDENCE).sum())
            externally_corroborated = int(subset["has_external_corroboration"].fillna(False).sum())
            major_conflicts = int(subset["has_major_conflict"].fillna(False).sum())
            doi_gap_works = int(subset["doi_expected_missing"].fillna(False).sum())
            rows.append(
                {
                    "source": source,
                    "record_type": record_type,
                    "canonical_works": canonical_works,
                    "integration_ready_works": integration_ready,
                    "reviewable_disputed_works": reviewable_disputed,
                    "not_integration_ready_works": not_integration_ready,
                    "high_confidence_works": high_confidence,
                    "medium_confidence_works": medium_confidence,
                    "low_confidence_works": low_confidence,
                    "externally_corroborated_works": externally_corroborated,
                    "major_conflict_works": major_conflicts,
                    "doi_gap_works": doi_gap_works,
                    "integration_ready_share": _share(integration_ready, canonical_works),
                    "reviewable_disputed_share": _share(reviewable_disputed, canonical_works),
                    "not_integration_ready_share": _share(not_integration_ready, canonical_works),
                    "high_confidence_share": _share(high_confidence, canonical_works),
                    "medium_confidence_share": _share(medium_confidence, canonical_works),
                    "low_confidence_share": _share(low_confidence, canonical_works),
                    "external_corroboration_share": _share(externally_corroborated, canonical_works),
                    "major_conflict_share": _share(major_conflicts, canonical_works),
                    "doi_gap_share": _share(doi_gap_works, canonical_works),
                }
            )
    return rows


def build_source_reliability_summary(
    source_record_df: pd.DataFrame,
    canonical_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if canonical_df is None:
        canonical_df = build_canonical_work_summary(source_record_df)
    if source_record_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "record_type",
                "canonical_works",
                "integration_ready_works",
                "reviewable_disputed_works",
                "not_integration_ready_works",
                "high_confidence_works",
                "medium_confidence_works",
                "low_confidence_works",
                "externally_corroborated_works",
                "major_conflict_works",
                "doi_gap_works",
                "integration_ready_share",
                "reviewable_disputed_share",
                "not_integration_ready_share",
                "high_confidence_share",
                "medium_confidence_share",
                "low_confidence_share",
                "external_corroboration_share",
                "major_conflict_share",
                "doi_gap_share",
            ]
        )

    presence_df = _canonical_presence_table(source_record_df, canonical_df)
    summary = pd.DataFrame(_build_summary_rows(presence_df))
    return summary.sort_values(["source", "record_type"], kind="stable").reset_index(drop=True)
