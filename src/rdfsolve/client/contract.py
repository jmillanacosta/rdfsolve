"""Validate authored graphs against approved generated fields and SHACL profiles."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from pydantic import BaseModel
from rdflib import RDF, Graph, URIRef

from rdfsolve.client.authoring import _check_term
from rdfsolve.client.collections import RDFList
from rdfsolve.client.hydration import class_iri, field_metadata
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.shacl_model import ShaclNodeShape


def _records(record: BaseModel, seen: set[int]) -> Iterator[BaseModel]:
    if id(record) in seen:
        return
    seen.add(id(record))
    yield record
    for name in type(record).model_fields:
        if not field_metadata(type(record).model_fields[name]).get("rdf_path"):
            continue
        value = getattr(record, name)
        values = value if isinstance(value, list) else [value]
        for item in values:
            members = item.items if isinstance(item, RDFList) else [item]
            for member in members:
                if isinstance(member, BaseModel) and not isinstance(member, RdfTerm):
                    yield from _records(member, seen)


def validate_contract(record: BaseModel, graph: Graph) -> None:
    """Check field RDF kinds and active declared constraints without source requests."""
    from rdfsolve.client.model_rdf import _resource

    shapes = Graph()
    seen_shapes: set[str] = set()
    records = list(_records(record, set()))
    inherited: dict[Any, set[str]] = {}  # a record's node -> classes its model inherits
    for item in records:
        scope = str(vars(item).get("rdf_source", {}).get("blank_node_scope", ""))
        resource = _resource(str(vars(item)["uri"]), scope)
        for base in type(item).__mro__:
            iri = getattr(base, "rdf_class_iri", None)
            if isinstance(iri, str):
                inherited.setdefault(resource, set()).add(iri)
    for item in records:
        data = vars(item)
        scope = str(data.get("rdf_source", {}).get("blank_node_scope", ""))
        subject = _resource(str(data["uri"]), scope)
        if (subject, RDF.type, URIRef(class_iri(item))) not in graph:
            raise ValueError("The record must retain its model class in rdf_type")
        for name, info in type(item).model_fields.items():
            extra = field_metadata(info)
            if not extra.get("rdf_path"):
                continue
            path = PropertyPath.model_validate(extra["rdf_path"])
            reverse = path.operator == "inverse"
            predicate = path.items[0].iri if reverse else path.iri
            if predicate is None:
                continue
            nodes = (
                graph.subjects(URIRef(predicate), subject)
                if reverse
                else graph.objects(subject, URIRef(predicate))
            )
            for node in nodes:
                if extra.get("rdf_collections") and (
                    node == RDF.nil or (node, RDF.first, None) in graph
                ):
                    continue
                _check_term(RdfTerm.from_rdf(node), extra.get("rdf_patterns", []), name)
                expected = {p["object_class"] for p in extra.get("rdf_patterns", [])}
                actual = {str(cls) for cls in graph.objects(node, RDF.type)} | inherited.get(
                    node, set()
                )
                if (
                    expected
                    and actual
                    and not expected.intersection({"Resource", "BlankNode", *actual})
                ):
                    raise ValueError(f"{name}: linked record has an unexpected class")
        for raw in getattr(type(item), "rdf_shapes", []):
            shape = ShaclNodeShape.model_validate(raw)
            if not shape.deactivated and shape.uri not in seen_shapes:
                shape.to_rdf(shapes)
                seen_shapes.add(shape.uri)
    if not shapes:
        return
    try:
        from pyshacl import validate
    except ImportError as error:
        raise ImportError(
            "Install rdfsolve[validation] to check declared SHACL constraints"
        ) from error
    conforms, _report, message = validate(
        graph,
        shacl_graph=shapes,
        inference="none",
        do_owl_imports=False,
        advanced=False,
        js=False,
    )
    if not conforms:
        raise ValueError(f"RDF contract violation:\n{message}")
