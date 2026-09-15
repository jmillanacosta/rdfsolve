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
    if not 1 <= len(terms) <= 12 or any(not t.strip() or len(t) > 200 for t in terms):
        raise ValueError("Use 1..12 nonempty search phrases of at most 200 characters")
    if len(fields) > 12:
        raise ValueError("Use at most twelve search fields")
    models = [client.model(kind)] if kind else list(client.models.values())
    if fields and any(
        (model.model_fields[client.field_name(model, name)].json_schema_extra or {}).get("rdf_path", {}).get("operator") not in (None, "predicate")
        for model in models for name in fields
    ):
        return _search_field_paths(client, terms, models, fields)
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


def _search_field_paths(client, terms, models, fields):
    """Search explicitly selected full field paths; keep path evidence, not fake edges."""
    from rdfsolve.client_api import Results, _name_fields
    from rdfsolve.exploration import _path
    from rdfsolve.schema_models.exporters.paths import path_to_sparql
    branches, selected = [], {}
    condition = " || ".join(f"CONTAINS(LCASE(STR(?text)), LCASE({Literal(t).n3()}))" for t in terms)
    for model in models:
        cls = str(model.rdf_class_iri)
        for field in fields:
            name = client.field_name(model, field)
            path = _path(model, name)
            selected[(cls, name)] = path
            branches.append("{ " + f"?s a {_iri(cls)} ; {path_to_sparql(path)} ?text . "
                            f"BIND({_iri(cls)} AS ?type) BIND({Literal(name).n3()} AS ?field) "
                            f"FILTER(isIRI(?s) && !isBlank(?text) && ({condition}))" + " }")
    if not branches:
        raise ValueError("No matching generated field paths")
    with client.step("Search selected generated field paths"):
        rows = client._select("SELECT DISTINCT ?s ?type ?field ?text ?_graph WHERE { "
                              + client._scope(" UNION ".join(branches))
                              + f" }} ORDER BY ?s ?type ?field ?text LIMIT {client.max_rows+1}")
        partial = len(rows) > client.max_rows
        groups, evidence, seen = defaultdict(set), [], set()
        for row in rows[:client.max_rows]:
            subject, cls, name = (_term(row[k]) for k in ("s", "type", "field"))
            if subject.kind != "uri" or (cls.value, name.value) not in selected:
                raise ValueError("Unexpected field-path search binding")
            if subject.value not in seen and len(seen) >= client.max_subjects:
                partial = True
                continue
            seen.add(subject.value); groups[cls.value].add(subject.value)
            path = selected[(cls.value, name.value)]
            evidence.append({"id":subject.value,"type":cls.value,"field":name.value,
                             "predicate":path.iri if path.operator=="predicate" else None,
                             "path":path.model_dump(mode="json"),"text":_term(row["text"]).model_dump(mode="json"),
                             "graph":row.get("_graph",{}).get("value"),"query_id":len(client._records())})
        records=[]
        for cls, ids in groups.items():
            model=client.model(cls)
            records.extend(client.get_many(model,sorted(ids),fields=_name_fields(model)))
    return Results(client,records,evidence=evidence,coverage={"status":"partial" if partial else "complete",
                   "basis":"Text candidates through explicitly selected generated field paths", "limit_reached":partial})
