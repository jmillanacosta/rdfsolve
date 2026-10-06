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
    # Virtuoso's service description graph, named with a relative IRI (AOP-Wiki, WikiPathways).
    "servicedescription",
    # The OWL vocabulary that Virtuoso loads into a graph of its own (160 triples; ATTED-II's
    # endpoint holds nothing else, 2026-10-06): not a source's data.
    "http://www.w3.org/2002/07/owl#",
)
"""Graph IRI prefixes that hold engine metadata rather than source data.

Endpoint-specific description graphs, such as a host's
``.well-known/sparql-examples``, are named per endpoint by the caller.
"""

KNOWN_ENGINE_GRAPHS: tuple[str, ...] = (
    "http://www.openlinksw.com/schemas/virtrdf#",
    "http://localhost:8890/DAV/",
    "http://localhost:8890/sparql",
    "http://www.w3.org/ns/ldp#",
    "urn:core:services:sparql",
    "urn:activitystreams-owl:map",
    "http://www.w3.org/2002/07/owl#",
)
"""Graphs that a Virtuoso endpoint holds of its own (rehearsal 2026-10-06: AOP-Wiki, AOPDB,
WikiPathways, NanoSafety). Asked for by name when the endpoint does not list its graphs in time.
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
UNTYPED_SUBJECT = "http://www.w3.org/2000/01/rdf-schema#Resource"
"""The subject class of a pattern of IRI subjects that have no type (subject_binding "untyped").

rdfs:Resource, the class of everything, as an untyped IRI object is "Resource" (rdfs:Resource):
the pattern says nothing of the subject beyond its property. ABSTAT gives untyped records
owl:Thing for the same purpose. The binding, not the IRI, marks the pattern: a source may type
records with rdfs:Resource, and those are typed patterns of that class.
"""
UNTYPED_SUBJECTS_LABEL = "untyped subjects"
"""The name under which class-centric views list the patterns of subjects without a type.

Not a class: views show these rows apart from every class, never as rdfs:Resource.
"""
