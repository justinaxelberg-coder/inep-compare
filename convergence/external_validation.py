from __future__ import annotations


def _normalise_record_type(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("-", "_").strip().lower()


def _normalise_year(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalise_crossref_validation(payload: dict | None) -> dict:
    if not payload:
        return {}
    return {
        "doi": payload.get("doi"),
        "record_type": _normalise_record_type(payload.get("document_type") or payload.get("record_type")),
        "title": payload.get("title"),
        "year": _normalise_year(payload.get("published_year") or payload.get("year")),
        "ror_affiliation_present": bool(payload.get("ror_affiliation_present")),
    }


def external_corroboration_for_work(work: dict, crossref_payload: dict | None) -> dict:
    ref = normalise_crossref_validation(crossref_payload)
    if not ref:
        return {"has_external_corroboration": False, "has_major_conflict": False, "conflict_fields": []}

    conflict_fields: list[str] = []

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
