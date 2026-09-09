"""Retrieve text candidates and retain the statements that matched."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter
from rdflib import Literal

from rdfsolve.exploration import SEARCH_PREDICATES
from rdfsolve.hydration import _iri, _term
from rdfsolve.schema_models.enrichment import DEFINITION_PREDICATES
from rdfsolve.schema_models.pattern import SchemaPattern

if TYPE_CHECKING:
    from rdfsolve.client_api import Client, Results


def search_records(
    client: Client, terms: list[str], kind: str | None, fields: list[str]
) -> Results:
    """Search any supplied phrase across names and mined text fields, in sequence."""
    from rdfsolve.client_api import Results, _name_fields
    from rdfsolve.rdf_operations import Search

    args = Search(terms=terms, kind=kind, fields=fields)
    models = [client.model(args.kind)] if args.kind else list(client.models.values())
    pairs: dict[tuple[str, str], str] = {}
    for model in models:
        cls = str(getattr(model, "rdf_class_iri", ""))
        selected = [client.field_name(model, name) for name in fields] if fields else []
        for name, info in model.model_fields.items():
            extra = info.json_schema_extra
            if not isinstance(extra, dict) or not extra.get("rdf_property_iri"):
                continue
            predicate = str(extra["rdf_property_iri"])
            patterns = TypeAdapter(list[SchemaPattern]).validate_python(
                extra.get("rdf_patterns", [])
            )
            text_field = any(
                p.object_class == "Literal"
                and p.datatype
                in (
                    None,
                    "http://www.w3.org/2001/XMLSchema#string",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
                )
                for p in patterns
            )
            if name in selected or (
                not fields
                and (
                    text_field
                    or predicate in SEARCH_PREDICATES
                    or predicate in DEFINITION_PREDICATES
                )
            ):
                pairs[(cls, predicate)] = name
    if not pairs:
        raise ValueError("No searchable fields in the selected schema")
    phrases = list(dict.fromkeys(term.strip() for term in terms))
    condition = " || ".join(
        f"CONTAINS(LCASE(STR(?text)), LCASE({Literal(term).n3()}))" for term in phrases
    )
    values = " ".join(f"({_iri(cls)} {_iri(p)})" for cls, p in sorted(pairs))
    body = client._scope(
        f"VALUES (?type ?p) {{ {values} }} ?s a ?type ; ?p ?text . "
        f"FILTER(isIRI(?s) && !isBlank(?text) && ({condition}))"
    )
    with client.step("Search names and descriptive text"):
        rows = client._select(
            f"SELECT DISTINCT ?s ?type ?p ?text ?_graph WHERE {{ {body} }} "
            f"ORDER BY ?s ?type ?p ?text LIMIT {client.max_rows + 1}"
        )
        truncated = len(rows) > client.max_rows
        subjects: set[str] = set()
        groups: dict[str, set[str]] = defaultdict(set)
        evidence = []
        for row in rows[: client.max_rows]:
            subject, class_term, predicate_term = (_term(row[key]) for key in ("s", "type", "p"))
            if (
                any(term.kind != "uri" for term in (subject, class_term, predicate_term))
                or (class_term.value, predicate_term.value) not in pairs
            ):
                raise ValueError("Unexpected search subject, type or predicate")
            if subject.value not in subjects and len(subjects) >= client.max_subjects:
                truncated = True
                continue
            subjects.add(subject.value)
            groups[class_term.value].add(subject.value)
            evidence.append(
                {
                    "id": subject.value,
                    "type": class_term.value,
                    "field": pairs[(class_term.value, predicate_term.value)],
                    "predicate": predicate_term.value,
                    "text": _term(row["text"]).model_dump(mode="json"),
                    "graph": row.get("_graph", {}).get("value"),
                    "query_id": len(client._records()),
                }
            )
        records = []
        for cls, ids in sorted(groups.items()):
            model = client.model(cls)
            records.extend(client.get_many(model, sorted(ids), fields=_name_fields(model)))
    return Results(
        client,
        records,
        evidence=evidence,
        coverage={
            "status": "partial" if truncated else "complete",
            "terms": phrases,
            "searched_fields": [
                {"type": cls, "field": name, "predicate": p}
                for (cls, p), name in sorted(pairs.items())
            ],
            "basis": "Text candidates in saved-schema types; not a complete topic assessment",
            "limit_reached": truncated,
        },
    )
