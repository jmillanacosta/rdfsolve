"""Shared constants for schema and mapping models."""

from __future__ import annotations

SERVICE_NAMESPACE_PREFIXES: tuple[str, ...] = (
    "http://www.openlinksw.com/",
    "http://www.w3.org/ns/sparql-service-description",
    "urn:virtuoso:",
    "http://localhost:8890/",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/ns/sparql-service-description#",
)
"""Namespace prefixes for service / system IRIs.

A URI is considered a "service" URI when it starts with any of
these strings.  Used by
:meth:`MinedSchema.filter_service_namespaces`.
"""

_RESOURCE_URIS = frozenset(
    {
        "http://www.w3.org/2000/01/rdf-schema#Resource",
        "rdfs:Resource",
        "Resource",
    }
)
_BLANK_NODE_URIS = frozenset(
    {
        "BlankNode",
        "_:BlankNode",
    }
)
_SENTINEL_OBJECTS = frozenset({"Literal", "Resource", "BlankNode"})
"""Special object_class values that are not actual URIs.

- Literal: Object is an RDF literal (datatype should be set)
- Resource: Object is an untyped URI (no rdf:type discovered)
- BlankNode: Object is a blank node (anonymous resource)
"""
_URI_SCHEMES = ("http://", "https://", "urn:")
_GRAPH_SKIP_KEYS = frozenset(
    {
        "void:inDataset",
        "dcterms:created",
        "dcterms:title",
    }
)
_BASE_URI = "https://jmillanacosta.com/rdfsolve"
