"""Shared constants for schema and mapping models."""

from __future__ import annotations

SERVICE_NAMESPACE_PREFIXES: tuple[str, ...] = (
    # Filtering disabled - was removing legitimate classes
    # "http://www.openlinksw.com/",
    # "http://www.w3.org/ns/sparql-service-description",
    # "urn:virtuoso:",
    # "http://localhost:8890/",
)
"""Namespace prefixes for service / system IRIs.

A URI is considered a "service" URI when it starts with any of
these strings.  Used by
:meth:`MinedSchema.filter_service_namespaces`.
"""

SUGGESTED_SERVICE_NAMESPACES: tuple[str, ...] = (
    "http://www.openlinksw.com/",
    "http://www.w3.org/ns/sparql-service-description#",
    "http://www.w3.org/ns/shacl#SPARQLExecutable",
    "http://www.w3.org/ns/shacl#SPARQLSelectExecutable",
    "http://www.w3.org/ns/shacl#SPARQLAskExecutable",
    "http://www.w3.org/ns/ldp#",
    "http://localhost:8890/",
    "urn:core:services:sparql",
    "urn:activitystreams-owl:",
)
"""Engine and example-query namespaces seen on public endpoints.

Pass these to :meth:`MinedSchema.clean_schema`; nothing applies them
implicitly. Extend the list per endpoint instead of editing it.
"""

SUGGESTED_SERVICE_GRAPHS: tuple[str, ...] = (
    "http://www.openlinksw.com/",
    "http://www.w3.org/ns/ldp#",
    "http://localhost:8890/",
    "urn:core:services:sparql",
    "urn:activitystreams-owl:",
)
"""Graph IRI prefixes that hold engine metadata rather than source data.

Endpoint-specific description graphs, such as a host's
``.well-known/sparql-examples``, are named per endpoint by the caller.
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
_URI_SCHEMES = ("http://", "https://", "urn:", "_:")
_BASE_URI = "https://jmillanacosta.com/rdfsolve"
