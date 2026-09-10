"""Select source definitions and fields without sending the whole schema."""

from __future__ import annotations

import re
from typing import Any

from rdfsolve.registry import FieldDescription, Registry, TypeDescription


def excerpt(text: str | None, terms: list[str] | None = None, size: int = 400) -> str | None:
    """Return a bounded passage; keep the original text in the schema or query log."""
    if text is None or len(text) <= size:
        return text
    positions = [text.casefold().find(term.casefold()) for term in terms or []]
    start = max(0, min((p for p in positions if p >= 0), default=0) - size // 4)
    return ("…" if start else "") + text[start : start + size] + "…"


def words(text: str) -> list[str]:
    """Split catalogue words, not record search phrases."""
    return [
        word.rstrip("s") if len(word) > 4 else word for word in re.findall(r"\w+", text.casefold())
    ]


def score(text: str, query: str) -> int:
    """Count matching query words; this is not a confidence estimate."""
    return sum(word in text.casefold() for word in words(query))


def field_card(field: FieldDescription) -> dict[str, Any]:
    """Keep field meaning, value kinds and exact source bindings."""
    return {
        "name": field.name,
        "label": field.label,
        "description": excerpt(field.description),
        "node_kinds": field.node_kinds,
        "targets": field.targets,
        "datatypes": field.datatypes,
        "path": field.binding["path"],
        "examples": [excerpt(str(value)) for value in field.examples[:1]],
    }


def type_text(item: TypeDescription) -> str:
    """Collect type and field text for local discovery."""
    return " ".join(
        [
            item.id,
            item.label,
            item.description or "",
            *[f"{field.name} {field.label} {field.description or ''}" for field in item.fields],
        ]
    )


def catalogue(
    registry: Registry, text: str, kinds: set[str], offset: int, limit: int
) -> dict[str, Any]:
    """Return relevant type cards, or fields for explicitly selected types."""
    items = [item for item in registry.types if not kinds or item.id in kinds]
    if text and not kinds:
        items = [item for item in items if score(type_text(item), text)]
    items.sort(
        key=lambda item: (
            -score(item.label, text),
            -score(type_text(item), text),
            item.label,
            item.id,
        )
    )
    cards = []
    for item in items if kinds else items[offset : offset + limit]:
        card: dict[str, Any] = {
            "id": item.id,
            "label": item.label,
            "description": excerpt(item.description, words(text)),
        }
        if kinds:
            fields = sorted(
                item.fields,
                key=lambda field: (
                    -score(f"{field.name} {field.label} {field.description or ''}", text),
                    field.name,
                ),
            )
            if text:
                fields = [
                    field
                    for field in fields
                    if score(f"{field.name} {field.label} {field.description or ''}", text)
                ]
            card.update(
                fields=[field_card(field) for field in fields[offset : offset + limit]],
                field_count=len(fields),
                next_offset=offset + limit if offset + limit < len(fields) else None,
            )
        else:
            fields = sorted(
                item.fields,
                key=lambda field: (
                    -score(f"{field.label} {field.description or ''}", text),
                    field.name,
                ),
            )
            card["relevant_fields"] = [field_card(field) for field in fields[:3]] if text else []
        cards.append(card)
    return {
        "types": cards,
        "matched_types": len(items),
        "data_queried": False,
        "next_offset": offset + limit if not kinds and offset + limit < len(items) else None,
        "basis": "Saved source definitions; field presence does not establish record values",
    }
