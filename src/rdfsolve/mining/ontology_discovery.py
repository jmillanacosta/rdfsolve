"""Bounded discovery of provider ontology graphs.

Discovery is intentionally separate from ontology *usage*.  A named graph can
contain ontology material without that ontology being used by the empirical RDF
schema.  Usage is attributed only from overlap with observed class/property
terms (and, in later analysis stages, observed ontology-valued data terms).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field
from rdflib import URIRef

from rdfsolve.evidence.ontology import (
    VERSION_PREDICATES,
    OntologyGraphCandidate,
    OntologyVersionEvidence,
)
from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper
from rdfsolve.void_retrieval import discover_graph_names

_OWL = "http://www.w3.org/2002/07/owl#"
_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
_RDFS = "http://www.w3.org/2000/01/rdf-schema#"

_CLASS_KINDS = (_OWL + "Class", _RDFS + "Class")
_PROPERTY_KINDS = (
    _RDF + "Property",
    _OWL + "ObjectProperty",
    _OWL + "DatatypeProperty",
    _OWL + "AnnotationProperty",
)
_AXIOM_PREDICATES = (
    _RDFS + "subClassOf",
    _RDFS + "subPropertyOf",
    _RDFS + "domain",
    _RDFS + "range",
    _OWL + "equivalentClass",
    _OWL + "equivalentProperty",
    _OWL + "inverseOf",
    _OWL + "disjointWith",
)


def _query_id(query: str) -> str:
    return hashlib.sha256(query.encode()).hexdigest()[:16]


def _scope(pattern: str, graph_uri: str | None) -> str:
    if graph_uri is None:
        return pattern
    return f"GRAPH {URIRef(graph_uri).n3()} {{ {pattern} }}"


def _candidate_probe_query(graph_uri: str | None) -> str:
    class_kinds = " ".join(URIRef(item).n3() for item in _CLASS_KINDS)
    property_kinds = " ".join(URIRef(item).n3() for item in _PROPERTY_KINDS)
    axiom_predicates = " ".join(URIRef(item).n3() for item in _AXIOM_PREDICATES)
    body = f"""
      {{ ?s a <{_OWL}Ontology> }}
      UNION {{ ?s a ?kind . VALUES ?kind {{ {class_kinds} {property_kinds} }} }}
      UNION {{ ?s ?axiom ?o . VALUES ?axiom {{ {axiom_predicates} }} }}
    """
    return f"ASK {{ {_scope(body, graph_uri)} }}"


def _metadata_query(graph_uri: str | None) -> str:
    version_predicates = " ".join(URIRef(str(item)).n3() for item in VERSION_PREDICATES)
    body = f"""
      {{
        ?ontology a <{_OWL}Ontology> .
        BIND("ontology" AS ?rowKind)
        BIND(?ontology AS ?subject)
        OPTIONAL {{ ?ontology <{_OWL}imports> ?object }}
      }}
      UNION
      {{
        VALUES ?predicate {{ {version_predicates} }}
        ?subject ?predicate ?object .
        BIND("version" AS ?rowKind)
      }}
    """
    return (
        "SELECT DISTINCT ?rowKind ?subject ?predicate ?object WHERE { "
        + _scope(body, graph_uri)
        + " }"
    )


def _overlap_query(
    graph_uri: str | None,
    classes: list[str],
    properties: list[str],
) -> str | None:
    branches: list[str] = []
    if classes:
        values = " ".join(URIRef(item).n3() for item in classes)
        kinds = " ".join(URIRef(item).n3() for item in _CLASS_KINDS)
        branches.append(
            "{ VALUES ?term { "
            + values
            + " } ?term a ?declKind . VALUES ?declKind { "
            + kinds
            + ' } BIND("class" AS ?termKind) }'
        )
    if properties:
        values = " ".join(URIRef(item).n3() for item in properties)
        kinds = " ".join(URIRef(item).n3() for item in _PROPERTY_KINDS)
        branches.append(
            "{ VALUES ?term { "
            + values
            + " } ?term a ?declKind . VALUES ?declKind { "
            + kinds
            + ' } BIND("property" AS ?termKind) }'
        )
    if not branches:
        return None
    body = " UNION ".join(branches)
    return "SELECT DISTINCT ?term ?termKind WHERE { " + _scope(body, graph_uri) + " }"


class OntologyDiscoverySummary(BaseModel):
    """Bounded ontology-graph discovery for one endpoint observation."""

    endpoint: str
    observed_at: str
    discovered_named_graphs: int
    scanned_named_graphs: int
    graph_scan_truncated: bool = False
    default_graph_scanned: bool = False
    candidates: list[OntologyGraphCandidate] = Field(default_factory=list)


def _select_rows(helper: SparqlHelper, query: str, purpose: str) -> list[dict[str, Any]]:
    result = helper.select(query, purpose=purpose)
    return list(result.get("results", {}).get("bindings", []))


def _chunks(values: Iterable[str], size: int) -> list[list[str]]:
    ordered = sorted(set(values))
    return [ordered[start : start + size] for start in range(0, len(ordered), size)]


def inspect_remote_ontology_graph(
    helper: SparqlHelper,
    graph_uri: str | None,
    *,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
    term_batch_size: int = 100,
) -> OntologyGraphCandidate | None:
    """Probe one endpoint graph and return discovery/usage evidence.

    A graph-name hint (``ontology``/``.owl``) is sufficient to keep a candidate
    even when the structural probe is negative.  It is never sufficient to mark
    the ontology as used by the empirical schema.
    """
    if term_batch_size < 1:
        raise ValueError("term_batch_size must be positive")
    graph_name = graph_uri or "DEFAULT"
    hint = graph_uri is not None and (
        "ontology" in graph_uri.lower() or ".owl" in graph_uri.lower()
    )
    observed_at = datetime.now(timezone.utc).isoformat()
    query_ids: list[str] = []

    probe = _candidate_probe_query(graph_uri)
    query_ids.append(_query_id(probe))
    try:
        matched = helper.ask(probe)
    except EndpointTimeoutError as error:
        if not hint:
            return OntologyGraphCandidate(
                graph_uri=graph_name,
                candidate_reasons=["probe_timeout"],
                discovery_status="timeout",
                discovery_error=str(error),
                observed_at=observed_at,
                query_ids=query_ids,
            )
        matched = False
        probe_error: Exception | None = error
    except Exception as error:  # discovery evidence must not abort the source run
        if not hint:
            return OntologyGraphCandidate(
                graph_uri=graph_name,
                candidate_reasons=["probe_error"],
                discovery_status="error",
                discovery_error=str(error),
                observed_at=observed_at,
                query_ids=query_ids,
            )
        matched = False
        probe_error = error
    else:
        probe_error = None

    if not matched and not hint:
        return None

    reasons: set[str] = set()
    if matched:
        reasons.add("ontology_structure_probe")
    if hint:
        reasons.add("graph_iri_hint")

    ontology_iris: set[str] = set()
    imports: set[str] = set()
    version_rows: set[tuple[str, str, str, str]] = set()

    meta_query = _metadata_query(graph_uri)
    query_ids.append(_query_id(meta_query))
    try:
        for row in _select_rows(helper, meta_query, f"ontology-discovery/metadata/{graph_name}"):
            row_kind = row.get("rowKind", {}).get("value")
            subject = row.get("subject", {}).get("value")
            obj = row.get("object", {}).get("value")
            predicate = row.get("predicate", {}).get("value")
            if row_kind == "ontology" and subject:
                ontology_iris.add(subject)
                reasons.add("owl_ontology_declaration")
                if obj:
                    imports.add(obj)
            elif row_kind == "version" and subject and predicate and obj:
                # Scope is finalized after ontology declarations are known.
                version_rows.add((subject, predicate, obj, "pending"))
    except Exception as error:
        # Candidate discovery remains useful even if metadata extraction fails.
        probe_error = probe_error or error

    version_evidence = []
    for subject, predicate, value, _ in sorted(version_rows):
        scope = (
            "ontology"
            if subject in ontology_iris
            else "graph"
            if graph_uri is not None and subject == graph_uri
            else "other"
        )
        version_evidence.append(
            OntologyVersionEvidence(
                subject=subject,
                predicate=predicate,
                value=value,
                scope=scope,
            )
        )

    class_overlap: set[str] = set()
    property_overlap: set[str] = set()
    class_batches = _chunks(observed_classes, term_batch_size)
    prop_batches = _chunks(observed_properties, term_batch_size)
    max_batches = max(len(class_batches), len(prop_batches), 1)
    for index in range(max_batches):
        classes = class_batches[index] if index < len(class_batches) else []
        properties = prop_batches[index] if index < len(prop_batches) else []
        query = _overlap_query(graph_uri, classes, properties)
        if query is None:
            continue
        query_ids.append(_query_id(query))
        try:
            for row in _select_rows(helper, query, f"ontology-discovery/overlap/{graph_name}"):
                term = row.get("term", {}).get("value")
                kind = row.get("termKind", {}).get("value")
                if not term:
                    continue
                if kind == "class":
                    class_overlap.add(term)
                elif kind == "property":
                    property_overlap.add(term)
        except Exception as error:
            probe_error = probe_error or error
            break

    return OntologyGraphCandidate(
        graph_uri=graph_name,
        explicit_ontology_iris=sorted(ontology_iris),
        imports=sorted(imports),
        version_evidence=version_evidence,
        candidate_reasons=sorted(reasons),
        discovery_status="matched" if matched else "hint_only",
        discovery_error=str(probe_error) if probe_error is not None else None,
        observed_at=observed_at,
        query_ids=query_ids,
        observed_class_overlap=sorted(class_overlap),
        observed_property_overlap=sorted(property_overlap),
    )


def discover_remote_ontology_graphs(
    helper: SparqlHelper,
    *,
    graph_uris: list[str] | None = None,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
    include_default_graph: bool = True,
    batch_size: int = 100,
    max_pages: int = 100,
    max_graphs: int = 500,
    term_batch_size: int = 100,
) -> OntologyDiscoverySummary:
    """Discover ontology-bearing graphs without downloading complete ontologies.

    ``graph_uris`` limits discovery to known graphs.  Otherwise named graph
    identities are enumerated using the existing graph-discovery machinery and
    capped by ``max_graphs``.  A cap is reported as truncation rather than being
    silently interpreted as complete endpoint coverage.
    """
    if min(batch_size, max_pages, max_graphs, term_batch_size) < 1:
        raise ValueError("discovery limits must be positive")
    names = (
        sorted(set(graph_uris))
        if graph_uris is not None
        else discover_graph_names(helper, batch_size=batch_size, max_pages=max_pages)
    )
    discovered_count = len(names)
    truncated = discovered_count > max_graphs
    names = names[:max_graphs]
    candidates: list[OntologyGraphCandidate] = []
    for graph_uri in names:
        candidate = inspect_remote_ontology_graph(
            helper,
            graph_uri,
            observed_classes=observed_classes,
            observed_properties=observed_properties,
            term_batch_size=term_batch_size,
        )
        if candidate is not None:
            candidates.append(candidate)
    if include_default_graph:
        candidate = inspect_remote_ontology_graph(
            helper,
            None,
            observed_classes=observed_classes,
            observed_properties=observed_properties,
            term_batch_size=term_batch_size,
        )
        if candidate is not None:
            candidates.append(candidate)
    return OntologyDiscoverySummary(
        endpoint=helper.endpoint_url,
        observed_at=datetime.now(timezone.utc).isoformat(),
        discovered_named_graphs=discovered_count,
        scanned_named_graphs=len(names),
        graph_scan_truncated=truncated,
        default_graph_scanned=include_default_graph,
        candidates=sorted(candidates, key=lambda item: item.graph_uri),
    )


__all__ = [
    "OntologyDiscoverySummary",
    "discover_remote_ontology_graphs",
    "inspect_remote_ontology_graph",
]
