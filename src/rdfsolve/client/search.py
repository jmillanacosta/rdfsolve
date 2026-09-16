"""Retrieve typed text candidates and retain the exact fields that matched."""

from __future__ import annotations

from collections import defaultdict

from rdflib import Literal

from rdfsolve.client.exploration import SEARCH_PREDICATES, _path
from rdfsolve.client.hydration import HydrationLimitError, _iri, _term
from rdfsolve.schema_models.enrichment import DEFINITION_PREDICATES, SYNONYM_PREDICATES
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath


def search_records(client, terms, kind, fields, *, names_only=False, allow_partial=False):
    """Search generated fields in one bounded request, keeping full RDF evidence."""
    from rdfsolve.client.api import Results, _name_fields

    if not 1 <= len(terms) <= 12 or any(not t.strip() or len(t) > 200 for t in terms):
        raise ValueError("Use 1..12 nonempty search phrases of at most 200 characters")
    if len(fields) > 12:
        raise ValueError("Use at most twelve search fields")
    models = [client.model(kind)] if kind else list(client.models.values())
    selected, direct, branches = {}, [], []
    for model in models:
        cls = str(model.rdf_class_iri)
        names = {client.field_name(model, name) for name in fields}
        if names_only and not fields:
            for predicate in sorted(SEARCH_PREDICATES):
                selected[(cls, predicate)] = (
                    None,
                    PropertyPath(operator="predicate", iri=predicate),
                )
                direct.append(f"({_iri(cls)} {_iri(predicate)})")
            continue
        for name, info in model.model_fields.items():
            extra = info.json_schema_extra or {}
            if not extra.get("rdf_path"):
                continue
            path = _path(model, name)
            text_field = any(
                p["object_class"] == "Literal"
                and p.get("datatype")
                in (
                    None,
                    "http://www.w3.org/2001/XMLSchema#string",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
                )
                for p in extra.get("rdf_patterns", [])
            )
            if not (
                name in names
                or (
                    not fields
                    and (text_field or path.iri in SEARCH_PREDICATES | set(DEFINITION_PREDICATES))
                )
            ):
                continue
            key = path.iri or name
            selected[(cls, key)] = (name, path)
            if path.operator == "predicate":
                direct.append(f"({_iri(cls)} {_iri(path.iri)})")
            else:
                branches.append(
                    f"{{ ?s a {_iri(cls)} ; {path_to_sparql(path)} ?text . BIND({_iri(cls)} AS ?type) BIND({Literal(key).n3()} AS ?p) }}"
                )
    if direct:
        branches.insert(
            0, f"{{ VALUES (?type ?p) {{ {' '.join(direct)} }} ?s a ?type ; ?p ?text . }}"
        )
    if not branches:
        raise ValueError("No searchable generated fields in the selected classes")
    phrases = list(dict.fromkeys(t.strip() for t in terms))
    condition = " || ".join(
        f"CONTAINS(LCASE(STR(?text)), LCASE({Literal(t).n3()}))" for t in phrases
    )
    body = (
        "{ "
        + " UNION ".join(branches)
        + f" }} FILTER(isIRI(?s) && !isBlank(?text) && ({condition}))"
    )
    with client.step(("Find " if names_only else "Search ") + ", ".join(phrases)):
        rows = client._select(
            f"SELECT DISTINCT ?s ?type ?p ?text ?_graph WHERE {{ {client._scope(body)} }} ORDER BY ?s ?type ?p ?text LIMIT {client.max_rows + 1}"
        )
        partial = len(rows) > client.max_rows
        groups, evidence, seen = defaultdict(set), [], set()
        query_id = len(client._records())
        for row in rows[: client.max_rows]:
            subject, cls, key = (_term(row[k]) for k in ("s", "type", "p"))
            if subject.kind != "uri" or (cls.value, key.value) not in selected:
                raise ValueError("Unexpected subject or generated field in search response")
            if subject.value not in seen and len(seen) >= client.max_subjects:
                partial = True
                continue
            seen.add(subject.value)
            groups[cls.value].add(subject.value)
            name, path = selected[(cls.value, key.value)]
            evidence.append(
                {
                    "id": subject.value,
                    "type": cls.value,
                    "field": name,
                    "predicate": path.iri,
                    "name_scope": SYNONYM_PREDICATES.get(path.iri),
                    "path": path.model_dump(mode="json"),
                    "text": _term(row["text"]).model_dump(mode="json"),
                    "graph": row.get("_graph", {}).get("value"),
                    "query_id": query_id,
                }
            )
        if partial and names_only and not allow_partial:
            raise HydrationLimitError("Too many name matches; narrow the text or choose a class")
        records = []
        for cls, ids in sorted(groups.items()):
            model = client.model(cls)
            records.extend(
                client.get_many(model, sorted(ids), fields=[] if partial else _name_fields(model))
            )
    return Results(
        client,
        records,
        evidence=evidence,
        coverage={
            "status": "partial" if partial else "complete",
            "terms": phrases,
            "searched_fields": [
                {"type": cls, "field": name, "predicate": path.iri}
                for (cls, _), (name, path) in selected.items()
            ],
            "basis": "Names and identifiers in generated classes"
            if names_only and not fields
            else "Text matches through generated field paths",
            "limit_reached": partial,
            "query_ids": client._steps[-1]["query_ids"],
        },
    )
