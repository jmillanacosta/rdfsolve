"""Ontology material in a dataset: which graphs hold ontology declarations, and their versions.

Discovery is separate from ontology *usage* (rdfsolve.ontology.usage). A named graph can
contain ontology material without the data using that ontology; usage is attributed only from
overlap with observed class and property terms. Local graphs are inspected in full; a remote
endpoint is probed with bounded queries.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, Dataset, Graph, URIRef

from rdfsolve.ontology.vocabulary import (
    CLASS_TYPES,
    PROPERTY_TYPES,
    STRUCTURE_PREDICATES,
    VERSION_PREDICATES,
)
from rdfsolve.ontology.vocabulary import OWL as _OWL
from rdfsolve.sparql_helper import EndpointTimeoutError, SparqlHelper
from rdfsolve.void_retrieval import discover_graph_names

_CLASS_KINDS = CLASS_TYPES
_PROPERTY_KINDS = PROPERTY_TYPES
_AXIOM_PREDICATES = STRUCTURE_PREDICATES
_VERSION = tuple(URIRef(p) for p in VERSION_PREDICATES)


class OntologyVersionEvidence(BaseModel):
    """A version statement associated with ontology or graph discovery.

    Only ``scope="ontology"`` or ``scope="graph"`` should be used to identify
    an ontology artifact version automatically. ``other`` is retained as context.
    """

    subject: str
    predicate: str
    value: str
    scope: str


class OntologyGraphCandidate(BaseModel):
    """Evidence that one RDF graph contains ontology material.

    ``used_by_schema`` is deliberately derived only from observed-term overlap.
    A graph can be an ontology candidate without being used by the dataset.
    """

    graph_uri: str
    explicit_ontology_iris: list[str] = Field(default_factory=list)
    imports: list[str] = Field(default_factory=list)
    version_evidence: list[OntologyVersionEvidence] = Field(default_factory=list)

    declared_classes: int | None = None
    declared_properties: int | None = None
    candidate_reasons: list[str] = Field(default_factory=list)
    discovery_status: Literal["matched", "hint_only", "timeout", "error"] = "matched"
    discovery_error: str | None = None
    observed_at: str | None = None
    query_ids: list[str] = Field(default_factory=list)

    observed_class_overlap: list[str] = Field(default_factory=list)
    observed_property_overlap: list[str] = Field(default_factory=list)

    @property
    def used_by_schema(self) -> bool:
        """Whether empirical class/property use overlaps this graph."""
        return bool(self.observed_class_overlap or self.observed_property_overlap)


def _declared_terms(graph: Graph, kinds: tuple[URIRef, ...]) -> set[str]:
    terms: set[str] = set()
    for kind in kinds:
        terms.update(
            str(subject)
            for subject in graph.subjects(RDF.type, kind)
            if isinstance(subject, URIRef)
        )
    return terms


def _version_evidence(
    graph: Graph, graph_uri: str, ontology_iris: set[str]
) -> list[OntologyVersionEvidence]:
    rows: set[tuple[str, str, str, str]] = set()
    for predicate in _VERSION:
        for subject, value in graph.subject_objects(predicate):
            if not isinstance(subject, URIRef):
                continue
            subject_s = str(subject)
            scope = (
                "ontology"
                if subject_s in ontology_iris
                else "graph"
                if subject_s == graph_uri
                else "other"
            )
            rows.add((subject_s, str(predicate), str(value), scope))
    return [
        OntologyVersionEvidence(subject=s, predicate=p, value=v, scope=scope)
        for s, p, v, scope in sorted(rows)
    ]


def inspect_ontology_graph(
    graph: Graph,
    graph_uri: str,
    *,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
) -> OntologyGraphCandidate | None:
    """Inspect one graph and return ontology-discovery evidence when present.

    Version metadata by itself is not sufficient to classify a graph as an
    ontology graph because dataset metadata graphs commonly publish versions.
    """
    ontology_iris = sorted(
        str(subject)
        for subject in graph.subjects(RDF.type, OWL.Ontology)
        if isinstance(subject, URIRef)
    )
    classes = _declared_terms(graph, tuple(URIRef(t) for t in _CLASS_KINDS))
    properties = _declared_terms(graph, tuple(URIRef(t) for t in _PROPERTY_KINDS))

    reasons: list[str] = []
    if ontology_iris:
        reasons.append("owl_ontology_declaration")
    if classes:
        reasons.append("class_declarations")
    if properties:
        reasons.append("property_declarations")
    lowered = graph_uri.lower()
    if "ontology" in lowered or ".owl" in lowered:
        reasons.append("graph_iri_hint")

    # A graph-name hint alone is useful discovery evidence, but a plain metadata
    # graph carrying only dcterms:issued/version is not an ontology candidate.
    if not reasons:
        return None

    imports = sorted(
        str(obj)
        for ontology in graph.subjects(RDF.type, OWL.Ontology)
        for obj in graph.objects(ontology, OWL.imports)
        if isinstance(obj, URIRef)
    )
    observed_class_set = set(observed_classes)
    observed_property_set = set(observed_properties)

    return OntologyGraphCandidate(
        graph_uri=graph_uri,
        explicit_ontology_iris=ontology_iris,
        imports=imports,
        version_evidence=_version_evidence(graph, graph_uri, set(ontology_iris)),
        declared_classes=len(classes),
        declared_properties=len(properties),
        candidate_reasons=sorted(set(reasons)),
        observed_class_overlap=sorted(classes & observed_class_set),
        observed_property_overlap=sorted(properties & observed_property_set),
    )


def discover_ontology_graphs(
    dataset: Dataset,
    *,
    observed_classes: Iterable[str] = (),
    observed_properties: Iterable[str] = (),
) -> list[OntologyGraphCandidate]:
    """Inspect named graphs in an RDFLib Dataset.

    This is the local/artifact implementation. Remote endpoint discovery should
    use the same evidence contract but apply bounded SPARQL probes instead of
    downloading every graph.
    """
    candidates: list[OntologyGraphCandidate] = []
    for graph in dataset.graphs():
        identifier = str(graph.identifier)
        candidate = inspect_ontology_graph(
            graph,
            identifier,
            observed_classes=observed_classes,
            observed_properties=observed_properties,
        )
        if candidate is not None:
            candidates.append(candidate)
    return sorted(candidates, key=lambda item: item.graph_uri)


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
        values = " ".join(f"<{item}>" for item in classes)
        kinds = " ".join(URIRef(item).n3() for item in _CLASS_KINDS)
        branches.append(
            "{ VALUES ?term { "
            + values
            + " } ?term a ?declKind . VALUES ?declKind { "
            + kinds
            + ' } BIND("class" AS ?termKind) }'
        )
    if properties:
        values = " ".join(f"<{item}>" for item in properties)
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
    "OntologyGraphCandidate",
    "OntologyVersionEvidence",
    "discover_ontology_graphs",
    "discover_remote_ontology_graphs",
    "inspect_ontology_graph",
    "inspect_remote_ontology_graph",
]
