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


def _normalise_title(value: object) -> str | None:
    if value in (None, ""):
        return None
    cleaned = []
    for char in str(value).strip().lower():
        if char.isalnum() or char.isspace():
            cleaned.append(char)
        else:
            cleaned.append(" ")
    return " ".join("".join(cleaned).split())


def _title_tokens(value: object) -> list[str]:
    title = _normalise_title(value)
    return title.split() if title else []


def _titles_have_major_conflict(left: object, right: object) -> bool:
    left_tokens = _title_tokens(left)
    right_tokens = _title_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens == right_tokens:
        return False
    shorter, longer = sorted((left_tokens, right_tokens), key=len)
    return longer[: len(shorter)] != shorter


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

    if _titles_have_major_conflict(work.get("title"), ref.get("title")):
        conflict_fields.append("title")

    has_external_corroboration = bool(work.get("doi") and ref.get("doi") and work["doi"] == ref["doi"])

    return {
        "has_external_corroboration": has_external_corroboration,
        "has_major_conflict": bool(conflict_fields),
        "conflict_fields": conflict_fields,
    }
