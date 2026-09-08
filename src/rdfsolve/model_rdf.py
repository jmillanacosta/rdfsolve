"""Write generated model records as RDF without inventing path edges."""

from __future__ import annotations

import hashlib
from typing import Any

from pydantic import BaseModel
from rdflib import RDF, BNode, Graph, Literal, URIRef
from rdflib.term import Identifier

from rdfsolve.hydration import _iri, _value
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import PropertyPath


def _resource(value: str, scope: str) -> URIRef | BNode:
    if value.startswith("_:"):
        label = value[2:]
        if scope:
            label = hashlib.sha256(f"{scope}:{label}".encode()).hexdigest()
        return BNode(label)
    _iri(value)
    return URIRef(value)


def _new_term(value: Any, extra: dict[str, Any], scope: str) -> Identifier:
    if isinstance(value, BaseModel):
        return _resource(str(vars(value)["uri"]), scope)
    patterns = extra.get("rdf_patterns", [])
    kinds = {p["object_class"] for p in patterns}
    if kinds and "Literal" not in kinds:
        if not isinstance(value, str):
            raise ValueError("An RDF resource needs an IRI or a named model record")
        return _resource(value, scope)
    if kinds == {"Literal"}:
        datatypes = {p.get("datatype") for p in patterns}
        if len(datatypes) != 1:
            raise ValueError("Choose a literal datatype in rdf_terms")
        datatype = datatypes.pop()
        if datatype == str(RDF.langString):
            raise ValueError("Supply the literal language in rdf_terms")
        return Literal(value, datatype=URIRef(datatype) if datatype else None)
    raise ValueError("Ambiguous RDF value; supply its kind and datatype in rdf_terms")


def model_to_graph(record: BaseModel, *, fields: list[str] | None = None) -> Graph:
    """Write populated fields using their RDF definitions.

    Keep retrieved lexical forms, datatypes, and languages. Reject stale raw
    terms after edits. Compound paths need intermediate statements and cannot
    be reconstructed from their terminal values. Pass fields to select only
    direct fields. Inverse single predicates can be written without guessing.

    The result is one RDF graph, not a named-graph dataset or a validation
    report. Mappings do not add types, replace predicates, or merge identities.
    """
    graph = Graph()
    _add_record(record, graph, set(), fields)
    return graph


def _add_record(
    record: BaseModel, graph: Graph, seen: set[int], fields: list[str] | None = None,
) -> None:
    if id(record) in seen:
        return
    seen.add(id(record))
    data = vars(record)
    source = data.get("rdf_source", {})
    scope = str(source.get("blank_node_scope", ""))
    subject = _resource(str(data["uri"]), scope)
    types = data.get("rdf_type", [])
    if not types and not source:
        class_iri = getattr(type(record), "rdf_class_iri", None)
        types = [class_iri] if class_iri else []
    for iri in types:
        graph.add((subject, RDF.type, _resource(iri, scope)))
    available = {
        name: info.json_schema_extra
        for name, info in type(record).model_fields.items()
        if isinstance(info.json_schema_extra, dict) and info.json_schema_extra.get("rdf_path")
    }
    selected = list(available) if fields is None else fields
    unknown = set(selected) - available.keys()
    if unknown:
        raise ValueError(f"No RDF definition for fields: {sorted(unknown)}")
    if fields is None and record.model_extra:
        raise ValueError("Extra fields lack RDF definitions; select fields explicitly")
    for name in selected:
        value = data.get(name)
        if value is None or value == []:
            continue
        extra = available[name]
        if not isinstance(extra, dict):
            raise TypeError("Expected RDF field metadata")
        path = PropertyPath.model_validate(extra["rdf_path"])
        reverse = path.operator == "inverse" and path.items[0].operator == "predicate"
        predicate = path.items[0].iri if reverse else path.iri
        if path.operator != "predicate" and not reverse:
            raise ValueError(f"Cannot reconstruct intermediate triples for {name}; select direct fields")
        if predicate is None:
            raise ValueError(f"No predicate for {name}")
        values = value if isinstance(value, list) else [value]
        retained = data.get("rdf_terms", {}).get(name)
        if retained is not None:
            terms = [RdfTerm.model_validate(term) for term in retained]
            if [_value(term) for term in terms] != [
                str(vars(item)["uri"]) if isinstance(item, BaseModel) else item for item in values
            ]:
                raise ValueError(f"Values changed in {name}; update or remove its rdf_terms entry")
            nodes = [
                _resource("_:" + term.value, scope) if term.kind == "bnode" else term.to_rdf()
                for term in terms
            ]
        else:
            nodes = [_new_term(item, extra, scope) for item in values]
        for node in nodes:
            if reverse:
                if not isinstance(node, (URIRef, BNode)):
                    raise ValueError(f"An inverse path needs a resource, not a literal: {name}")
                graph.add((node, URIRef(predicate), subject))
            else:
                graph.add((subject, URIRef(predicate), node))
        for item in values:
            if isinstance(item, BaseModel):
                _add_record(item, graph, seen)
