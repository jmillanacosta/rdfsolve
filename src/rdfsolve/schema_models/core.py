"""Core schema models: SchemaPattern, AboutMetadata, MinedSchema.

These are the primary data structures for mined RDF schemas.
"""

from __future__ import annotations

import json as _json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from hashlib import md5
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# PatternType enum


class PatternType(str, Enum):
    """Semantic type of an RDF pattern.

    Distinguishes what kind of RDF construct a pattern represents,
    which is essential for proper schema interpretation and
    downstream code generation.
    """

    OBJECT_PROPERTY = "object_property"
    """Links subject to another resource (typed or untyped URI)."""

    DATATYPE_PROPERTY = "datatype_property"
    """Links subject to a literal value (string, integer, date, etc)."""

    ANNOTATION_PROPERTY = "annotation"
    """Metadata property (rdfs:label, rdfs:comment, dcterms prefix, etc)."""

    BLANK_NODE_PROPERTY = "blank_node_property"
    """Links subject to a blank node (structural/anonymous)."""

    UNKNOWN = "unknown"
    """Could not determine the pattern type."""


from rdfsolve._uri import (
    make_expander,
    uri_to_curie,
)
from rdfsolve.schema_models._constants import (
    _BASE_URI,
    _GRAPH_SKIP_KEYS,
    _RESOURCE_URIS,
    _SENTINEL_OBJECTS,
    _URI_SCHEMES,
    SERVICE_NAMESPACE_PREFIXES,
)

_log = logging.getLogger(__name__)


# SchemaPattern


class SchemaPattern(BaseModel):
    """A single schema pattern: subject_class -> property -> object.

    Captures four kinds of relationships:

    - **typed-object**:
      ``?s a ?sc . ?s ?p ?o . ?o a ?oc``
    - **literal**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isLiteral(?o))``
    - **untyped-uri** (unconstrained URI):
      ``?s a ?sc . ?s ?p ?o . FILTER(isURI(?o) && NOT EXISTS { ?o a ?any })``
    - **blank-node**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isBlank(?o))``

    This model is shared between SchemaMiner (direct SPARQL)
    and VoidParser (RDF triples VoID catalog-based extraction).
    """

    subject_class: str = Field(
        ...,
        description="URI of the subject class",
    )
    property_uri: str = Field(
        ...,
        description="URI of the property",
    )
    object_class: str = Field(
        ...,
        description=(
            "URI of the object class. Special values: "
            "'Literal' for literal objects (use datatype for XSD type), "
            "'Resource' for untyped URI objects (rdfs:Resource), "
            "'BlankNode' for blank node objects."
        ),
    )
    count: int | None = Field(
        None,
        ge=0,
        description="Number of triples matching this pattern",
    )
    datatype: str | None = Field(
        None,
        description="XSD datatype URI for literal objects (only when object_class == 'Literal')",
    )
    blank_node_predicates: list[str] | None = Field(
        None,
        description=(
            "Predicates that blank node objects have (structural signature). "
            "Only set when object_class == 'BlankNode'. Common patterns: "
            "rdf:first/rdf:rest (lists), owl:onProperty (restrictions)."
        ),
    )

    # Semantic type
    pattern_type: PatternType = Field(
        default=PatternType.UNKNOWN,
        description="Semantic type of this pattern (object/datatype/annotation/blank_node)",
    )

    # Evidence metrics
    distinct_subjects: int | None = Field(
        None,
        ge=0,
        description="Number of distinct subjects using this pattern",
    )
    distinct_objects: int | None = Field(
        None,
        ge=0,
        description="Number of distinct objects in this pattern",
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score (0.0-1.0) for this pattern",
    )
    evidence_source: str = Field(
        default="mined",
        description="How this pattern was discovered: 'mined', 'inferred', 'imported'",
    )

    # Labels
    subject_label: str | None = Field(
        None,
        description="Human-readable label for the subject class",
    )
    property_label: str | None = Field(
        None,
        description="Human-readable label for the property",
    )
    object_label: str | None = Field(
        None,
        description="Human-readable label for the object class",
    )

    @field_validator("subject_class", "property_uri")
    @classmethod
    def _validate_uri(cls, v: str) -> str:
        if not v.startswith(_URI_SCHEMES):
            msg = f"Invalid URI: {v}"
            raise ValueError(msg)
        return v

    @field_validator("object_class")
    @classmethod
    def _validate_object(cls, v: str) -> str:
        if v not in _SENTINEL_OBJECTS and not v.startswith(
            _URI_SCHEMES,
        ):
            msg = f"Invalid object class: {v}"
            raise ValueError(msg)
        return v


# AboutMetadata


class AboutMetadata(BaseModel):
    """Provenance metadata attached to every schema export.

    Contains identity, source, provenance, quality, and validation
    information critical for versioned schema management and
    downstream typed-client generation.
    """

    # Identity
    schema_id: str | None = Field(
        None,
        description="Unique identifier (UUID or content hash)",
    )
    schema_version: str = Field(
        default="0.0.0",
        description="Semantic version of this schema (major.minor.patch)",
    )

    # Source
    dataset_name: str | None = Field(
        None,
        description="Human-readable dataset name",
    )
    endpoint: str | None = Field(
        None,
        description="SPARQL endpoint URL",
    )
    graph_uris: list[str] | None = Field(
        None,
        description="Named graph URIs queried",
    )

    # Provenance
    generated_by: str = Field(
        default="unknown",
        description="Tool and version string",
    )
    generated_at: str = Field(
        default="",
        description="ISO-8601 timestamp (UTC)",
    )
    strategy: str = Field(
        "unknown",
        description="Mining strategy used (e.g. 'qlever_oneshot', 'sparql_paginated', 'void')",
    )

    # Data Versioning (critical for typed-client)
    source_version: str | None = Field(
        None,
        description="Version of the source data if known (e.g. '2024_01', 'v3.2')",
    )
    source_version_iri: str | None = Field(
        None,
        description="Dataset version IRI (owl:versionIRI)",
    )
    source_issued: str | None = Field(
        None,
        description="Publication date of source dataset (ISO-8601, dcat:issued)",
    )
    source_modified: str | None = Field(
        None,
        description="Last-Modified timestamp of source data (ISO-8601)",
    )
    source_license: str | None = Field(
        None,
        description="License URI for the source data",
    )
    source_publisher: str | None = Field(
        None,
        description="Publisher URI or name (dcterms:publisher)",
    )
    source_creator: list[str] | None = Field(
        None,
        description="Creator URIs (dcterms:creator)",
    )
    homepage: str | None = Field(
        None,
        description="Dataset homepage URI (foaf:homepage)",
    )
    title: str | None = Field(
        None,
        description="Dataset title (dcterms:title, overrides dataset_name if present)",
    )
    description: str | None = Field(
        None,
        description="Dataset description (dcterms:description)",
    )

    # Tool Versions
    rdfsolve_version: str | None = Field(
        None,
        description="rdfsolve version string",
    )
    qlever_version: dict[str, str] | None = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's "
            '?cmd=stats: {"git_hash_server": str, "git_hash_index": str}'
        ),
    )

    # Timing
    started_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when mining started",
    )
    finished_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when mining finished",
    )
    total_duration_s: float | None = Field(
        None,
        ge=0,
        description="Total wall-clock seconds",
    )

    # Statistics
    pattern_count: int = Field(
        0,
        ge=0,
        description="Number of schema patterns",
    )
    class_count: int = Field(
        0,
        ge=0,
        description="Number of distinct classes (declared + used types)",
    )
    declared_class_count: int = Field(
        0,
        ge=0,
        description=(
            "Number of formally declared classes (owl:Class or rdfs:Class). "
            "These have explicit class definitions in the dataset."
        ),
    )
    used_type_count: int = Field(
        0,
        ge=0,
        description=(
            "Number of URIs used as rdf:type values but NOT declared as classes. "
            "Common in LOD: external ontology terms used as types without importing definitions."
        ),
    )
    property_count: int = Field(
        0,
        ge=0,
        description="Number of distinct properties",
    )
    triple_count_estimate: int | None = Field(
        None,
        ge=0,
        description="Estimated total triples in source data",
    )
    distinct_subject_count: int | None = Field(
        None,
        ge=0,
        description="COUNT(DISTINCT ?s) across all patterns",
    )
    distinct_predicate_count: int | None = Field(
        None,
        ge=0,
        description="COUNT(DISTINCT ?p) across all patterns",
    )

    # Quality Metrics
    coverage_score: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Fraction of data covered by schema patterns (0.0-1.0)",
    )
    confidence_score: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Overall schema confidence score (0.0-1.0)",
    )

    # Validation
    validation_status: Literal["unvalidated", "auto_validated", "manual_validated"] = Field(
        default="unvalidated",
        description="Validation state of this schema",
    )
    validation_errors: list[str] = Field(
        default_factory=list,
        description="List of validation error messages",
    )

    # Authors
    authors: list[dict[str, str]] | None = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # Canonical URIs (auto-populated from dataset_name)
    schema_uri: str | None = Field(
        None,
        description="Canonical URI where this schema is served",
    )
    void_uri: str | None = Field(
        None,
        description="Canonical URI where the VoID catalog is served",
    )
    report_uri: str | None = Field(
        None,
        description="Canonical URI where the run report is served",
    )
    linkml_uri: str | None = Field(
        None,
        description="Canonical URI where the LinkML schema is served",
    )

    model_config = ConfigDict(extra="allow")

    @staticmethod
    def build(
        endpoint: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
        pattern_count: int = 0,
        class_count: int = 0,
        declared_class_count: int = 0,
        used_type_count: int = 0,
        property_count: int = 0,
        strategy: str = "unknown",
        started_at: str | None = None,
        finished_at: str | None = None,
        total_duration_s: float | None = None,
        authors: list[dict[str, str]] | None = None,
        qlever_version: dict[str, str] | None = None,
        # New version fields
        schema_version: str = "1.0.0",
        source_version: str | None = None,
        source_version_iri: str | None = None,
        source_issued: str | None = None,
        source_modified: str | None = None,
        source_license: str | None = None,
        source_publisher: str | None = None,
        source_creator: list[str] | None = None,
        homepage: str | None = None,
        title: str | None = None,
        description: str | None = None,
        triple_count_estimate: int | None = None,
        distinct_subject_count: int | None = None,
        distinct_predicate_count: int | None = None,
        coverage_score: float | None = None,
        confidence_score: float | None = None,
    ) -> AboutMetadata:
        """Create metadata with auto-populated version + timestamp."""
        from urllib.parse import quote
        from uuid import uuid4

        from rdfsolve.version import VERSION

        def _uri(suffix: str) -> str | None:
            if not dataset_name:
                return None
            encoded_name = quote(dataset_name, safe="")
            return f"{_BASE_URI}/api/{suffix}/{encoded_name}"

        return AboutMetadata(
            # Identity
            schema_id=str(uuid4()),
            schema_version=schema_version,
            # Source
            dataset_name=dataset_name,
            endpoint=endpoint,
            graph_uris=graph_uris,
            # Provenance
            generated_by=f"rdfsolve {VERSION}",
            generated_at=datetime.now(timezone.utc).isoformat(),
            strategy=strategy,
            # Data versioning
            source_version=source_version,
            source_version_iri=source_version_iri,
            source_issued=source_issued,
            source_modified=source_modified,
            source_license=source_license,
            source_publisher=source_publisher,
            source_creator=source_creator,
            homepage=homepage,
            title=title,
            description=description,
            # Tool versions
            rdfsolve_version=VERSION,
            qlever_version=qlever_version,
            # Timing
            started_at=started_at,
            finished_at=finished_at,
            total_duration_s=total_duration_s,
            # Statistics
            pattern_count=pattern_count,
            class_count=class_count,
            declared_class_count=declared_class_count,
            used_type_count=used_type_count,
            property_count=property_count,
            triple_count_estimate=triple_count_estimate,
            distinct_subject_count=distinct_subject_count,
            distinct_predicate_count=distinct_predicate_count,
            # Quality
            coverage_score=coverage_score,
            confidence_score=confidence_score,
            # Authors
            authors=authors,
            # Canonical URIs
            schema_uri=_uri("schemas"),
            void_uri=_uri("void"),
            report_uri=_uri("reports"),
            linkml_uri=_uri("linkml"),
        )


# JSON-LD helpers


def _merge_into_list(
    grouped: dict[str, dict[str, Any]],
    key: str,
    prop: str,
    value: Any,
) -> None:
    """Merge *value* into ``grouped[key][prop]``.

    Creates a list when two distinct values share the same slot.
    """
    node = grouped.setdefault(key, {"@id": key})
    existing = node.get(prop)
    if existing is None:
        node[prop] = value
    elif isinstance(existing, list):
        if value not in existing:
            existing.append(value)
    elif existing != value:
        node[prop] = [existing, value]


def _object_value_and_key(
    pat: SchemaPattern,
    context: dict[str, str],
    labels: dict[str, str],
) -> tuple[dict[str, Any], str]:
    """Return the JSON-LD object value dict and count-map key.

    Handles four cases:
    - Literal: returns {"@type": datatype_curie}
    - Resource (untyped URI): returns {"@id": "rdfs:Resource"}
    - BlankNode: returns {"@id": "_:BlankNode"} (structural marker)
    - Typed object: returns {"@id": class_curie}
    """
    if pat.object_class == "Literal":
        if pat.datatype:
            dt_c, dt_pfx, dt_ns = uri_to_curie(pat.datatype)
            if dt_pfx and dt_ns:
                context[dt_pfx] = dt_ns
            return {"@type": dt_c}, f"Literal:{dt_c}"
        context.setdefault(
            "xsd",
            "http://www.w3.org/2001/XMLSchema#",
        )
        return {"@type": "xsd:string"}, "Literal:xsd:string"

    if pat.object_class == "Resource":
        context.setdefault(
            "rdfs",
            "http://www.w3.org/2000/01/rdf-schema#",
        )
        return {"@id": "rdfs:Resource"}, "Resource"

    if pat.object_class == "BlankNode":
        # Blank nodes in JSON-LD are represented with _: prefix
        # We use a structural marker to indicate "some blank node"
        return {"@id": "_:BlankNode"}, "BlankNode"

    oc, oc_pfx, oc_ns = uri_to_curie(pat.object_class)
    if oc_pfx and oc_ns:
        context[oc_pfx] = oc_ns
    if pat.object_label:
        labels[oc] = pat.object_label
    return {"@id": oc}, oc


# MinedSchema


class MinedSchema(BaseModel):
    """Complete mined schema: patterns + provenance.

    Supports direct export to multiple formats:
    - JSON-LD: to_jsonld()
    - VoID RDF: to_void_graph()
    - LinkML: to_linkml(), to_linkml_yaml()
    - SHACL: to_shacl()
    """

    patterns: list[SchemaPattern] = Field(
        default_factory=list,
        description="Schema patterns",
    )
    about: AboutMetadata = Field(
        ...,
        description="Provenance metadata",
    )

    # Service-namespace filtering

    def filter_service_namespaces(
        self,
        extra_prefixes: list[str] | None = None,
    ) -> MinedSchema:
        """Return a copy without service/system patterns.

        A pattern is removed when **any** of its
        ``subject_class``, ``property_uri``, or ``object_class``
        starts with a prefix listed in
        :data:`SERVICE_NAMESPACE_PREFIXES` (or *extra_prefixes*).
        """
        prefixes = SERVICE_NAMESPACE_PREFIXES
        if extra_prefixes:
            prefixes = (*prefixes, *extra_prefixes)

        def _svc(uri: str) -> bool:
            return uri.startswith(prefixes)

        kept = [
            p
            for p in self.patterns
            if not (
                _svc(p.subject_class)
                or _svc(p.property_uri)
                or (p.object_class not in _SENTINEL_OBJECTS and _svc(p.object_class))
            )
        ]
        return self.model_copy(update={"patterns": kept})

    # Queries -

    def get_classes(self) -> list[str]:
        """Return sorted unique subject/object class URIs."""
        classes: set[str] = set()
        for p in self.patterns:
            classes.add(p.subject_class)
            if p.object_class not in _SENTINEL_OBJECTS:
                classes.add(p.object_class)
        return sorted(classes)

    def get_properties(self) -> list[str]:
        """Return sorted unique property URIs."""
        return sorted({p.property_uri for p in self.patterns})

    # JSON-LD import

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> MinedSchema:
        """Reconstruct from a JSON-LD dict (e.g. returned by :meth:`to_jsonld`).

        Inverse of :meth:`to_jsonld`.  Expands CURIEs using the
        dict's own ``@context`` block.
        """
        context: dict[str, str] = raw.get("@context", {})
        about_data = raw.get("@about", {})
        labels: dict[str, str] = raw.get("_labels", {})
        expand = make_expander(context)

        patterns = _parse_schema_graph(
            raw.get("@graph", []),
            expand,
            labels,
        )
        about = AboutMetadata.model_validate(about_data)
        return cls(patterns=patterns, about=about)

    @classmethod
    def from_jsonld(cls, path: str | Path) -> MinedSchema:
        """Reconstruct from a ``*_schema.jsonld`` file.

        Convenience wrapper around :meth:`from_dict` that reads and
        parses the file first.
        """
        raw = _json.loads(
            Path(path).read_text(encoding="utf-8"),
        )
        return cls.from_dict(raw)

    # NetworkX export

    def to_networkx(self) -> Any:
        """Export as a typed-object ``nx.MultiDiGraph``.

        Nodes are class URIs.  Each typed-object pattern becomes a
        directed edge.  Literal/Resource sentinels are excluded.
        """
        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for to_networkx(); install it with: pip install networkx",
            ) from exc

        graph: Any = _nx.MultiDiGraph()
        dataset = self.about.dataset_name or ""

        for pat in self.patterns:
            if pat.object_class in _SENTINEL_OBJECTS:
                continue
            for uri, label in (
                (pat.subject_class, pat.subject_label),
                (pat.object_class, pat.object_label),
            ):
                if uri not in graph:
                    graph.add_node(
                        uri,
                        dataset=dataset,
                        label=label or "",
                    )
            graph.add_edge(
                pat.subject_class,
                pat.object_class,
                predicate=pat.property_uri,
                dataset=dataset,
                count=pat.count,
            )
        return graph

    # JSON-LD export

    def to_jsonld(self) -> dict[str, Any]:
        """Export schema as JSON-LD by serializing the VoID graph.

        This produces proper semantic RDF using VoID vocabulary,
        serialized as JSON-LD. The result is fully interconvertible
        with other RDF formats.

        Returns a JSON-LD document with:
        - void:Dataset for the schema metadata
        - void:propertyPartition for each pattern
        - void:triples for counts
        - rdfs:label for labels
        """
        import json

        # Get the VoID graph (proper semantic RDF)
        void_graph = self.to_void_graph()

        # Serialize as JSON-LD
        jsonld_str = void_graph.serialize(format="json-ld", auto_compact=True)
        result: dict[str, Any] = json.loads(jsonld_str)
        return result

    # VoID graph export

    def to_void_graph(self) -> Any:
        """Build an rdflib VoID Graph from the mined patterns.

        Allows feeding the result into VoidParser for downstream
        conversion to LinkML, SHACL, RDF-config, etc.
        """
        import json

        from rdflib import Graph, Namespace, URIRef
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, RDFS, XSD

        from rdfsolve.config import get_base_uri

        # Configure namespaces
        base_uri = get_base_uri()
        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")
        vocab_ns = Namespace(f"{base_uri}/vocab#")
        partition_ns = Namespace(f"{base_uri}/schema#")

        g = Graph()

        # Bind standard namespaces
        for pfx, ns in (
            ("void", void),
            ("void-ext", void_ext),
            ("rdf", RDF),
            ("rdfs", RDFS),
            ("xsd", XSD),
            ("dcterms", DCTERMS),
            ("foaf", FOAF),
            ("owl", OWL),
        ):
            g.bind(pfx, ns)

        # Bind rdfsolve-specific namespaces
        g.bind("vocab", vocab_ns)
        g.bind("partition", partition_ns)

        # Dataset URI: represents THE SOURCE RDF dataset
        endpoint = self.about.endpoint

        if endpoint and not endpoint.startswith(("http://localhost", "http://127.0.0.1")):
            # Remote endpoint: use endpoint URL as dataset identifier
            dataset_uri = URIRef(endpoint)
            # Use schema namespace for partitions
            base = str(partition_ns)
        else:
            # Local or no endpoint: use configured base URI
            dataset_name = self.about.dataset_name or "unknown"
            dataset_uri = URIRef(f"{base_uri}/dataset/{dataset_name}")
            # Use schema namespace for partitions
            base = str(partition_ns)

        # void:Dataset represents the source RDF dataset
        g.add((dataset_uri, RDF.type, void.Dataset))

        # SPARQL endpoint (only for remote endpoints)
        if endpoint and not endpoint.startswith(("http://localhost", "http://127.0.0.1")):
            g.add((dataset_uri, void.sparqlEndpoint, URIRef(endpoint)))

        # Title: prefer explicit title from metadata, fallback to dataset_name
        title_value = self.about.title or self.about.dataset_name
        if title_value:
            g.add((dataset_uri, DCTERMS.title, RdfLiteral(title_value)))

        # Description
        if self.about.description:
            g.add((dataset_uri, DCTERMS.description, RdfLiteral(self.about.description)))

        # License
        if self.about.source_license:
            g.add((dataset_uri, DCTERMS.license, URIRef(self.about.source_license)))

        # Publisher
        if self.about.source_publisher:
            # Try as URI first, fallback to literal if not a valid URI
            try:
                if self.about.source_publisher.startswith(("http://", "https://", "urn:")):
                    g.add((dataset_uri, DCTERMS.publisher, URIRef(self.about.source_publisher)))
                else:
                    g.add((dataset_uri, DCTERMS.publisher, RdfLiteral(self.about.source_publisher)))
            except Exception:
                g.add((dataset_uri, DCTERMS.publisher, RdfLiteral(self.about.source_publisher)))

        # Creators (multiple allowed)
        if self.about.source_creator:
            for creator in self.about.source_creator:
                # Try as URI first, fallback to literal
                try:
                    if creator.startswith(("http://", "https://", "urn:")):
                        g.add((dataset_uri, DCTERMS.creator, URIRef(creator)))
                    else:
                        g.add((dataset_uri, DCTERMS.creator, RdfLiteral(creator)))
                except Exception:
                    g.add((dataset_uri, DCTERMS.creator, RdfLiteral(creator)))

        # Homepage
        if self.about.homepage:
            g.add((dataset_uri, FOAF.homepage, URIRef(self.about.homepage)))

        # Version info
        if self.about.source_version_iri:
            g.add((dataset_uri, OWL.versionIRI, URIRef(self.about.source_version_iri)))

        # Dates
        if self.about.source_issued:
            g.add((dataset_uri, DCTERMS.issued, RdfLiteral(self.about.source_issued)))
        if self.about.source_modified:
            g.add((dataset_uri, DCTERMS.modified, RdfLiteral(self.about.source_modified)))

        # Dataset statistics
        if self.about.class_count:
            g.add(
                (
                    dataset_uri,
                    void.classes,
                    RdfLiteral(self.about.class_count, datatype=XSD.integer),
                )
            )
        if self.about.property_count:
            g.add(
                (
                    dataset_uri,
                    void.properties,
                    RdfLiteral(self.about.property_count, datatype=XSD.integer),
                )
            )

        # Extract vocabularies and classes from patterns
        vocabs = set()
        classes = set()
        for pat in self.patterns:
            if pat.subject_class:
                classes.add(pat.subject_class)
            if pat.object_class and pat.object_class not in _SENTINEL_OBJECTS:
                classes.add(pat.object_class)

            for uri in [pat.subject_class, pat.property_uri, pat.object_class]:
                if uri and uri not in _SENTINEL_OBJECTS:
                    # Extract namespace
                    if "#" in uri:
                        vocab = uri.rsplit("#", 1)[0] + "#"
                    elif "/" in uri:
                        vocab = uri.rsplit("/", 1)[0] + "/"
                    else:
                        continue
                    # Skip W3C vocabularies
                    if not vocab.startswith("http://www.w3.org/"):
                        vocabs.add(vocab)

        # Add vocabulary declarations
        for vocab_uri in sorted(vocabs):
            g.add((dataset_uri, void.vocabulary, URIRef(vocab_uri)))

        # Add class partitions
        for cls_uri in sorted(classes):
            cls_hash = md5(cls_uri.encode(), usedforsecurity=False).hexdigest()[:8]
            cp_uri = URIRef(f"{base}class-{cls_hash}")
            g.add((dataset_uri, void.classPartition, cp_uri))
            g.add((cp_uri, RDF.type, void.Dataset))
            g.add((cp_uri, void["class"], URIRef(cls_uri)))

        # Get dataset identifier for hash uniqueness
        # Use dataset_name if available, otherwise "default"
        if self.about.dataset_name:
            dataset_id = self.about.dataset_name
        else:
            # No dataset name: use "default" for readable URIs
            # Still include full dataset_uri in hash for uniqueness
            dataset_id = "default"

        def _extract_local_name(uri: str) -> str:
            """Extract local name from URI for readability."""
            if "#" in uri:
                return uri.split("#")[-1]
            elif "/" in uri:
                return uri.split("/")[-1]
            return uri

        def _pid(s: str, p: str, o: str) -> URIRef:
            """Generate partition URI with dataset-scoped hash and explanatory name.

            Inspired by UniProt VoID, using hyphens as separators for Turtle compatibility.
            Format: {dataset}-{hash}-{description}
            E.g., aopwiki-a1b2c3d4-KeyEvent-has_name-String

            This makes URIs human-readable, debuggable, and serializes cleanly
            as Turtle QNames (partition:aopwiki-a1b2c3d4-...).
            """
            # Extract local names for readability
            s_name = _extract_local_name(s)
            p_name = _extract_local_name(p)
            o_name = _extract_local_name(o)

            # Build descriptive pattern name
            # Limit each part to avoid excessively long URIs
            s_short = s_name[:30] if len(s_name) > 30 else s_name
            p_short = p_name[:30] if len(p_name) > 30 else p_name
            o_short = o_name[:30] if len(o_name) > 30 else o_name

            # Create pattern description
            desc = f"{s_short}-{p_short}-{o_short}"
            # Clean up for URI safety (remove spaces, special chars)
            desc = desc.replace(" ", "_").replace(":", "_")

            # Generate hash for uniqueness
            # Include dataset_uri (not just dataset_id) to ensure uniqueness
            # even when multiple datasets use dataset_id="default"
            hash_input = f"{dataset_uri!s}|{s}|{p}|{o}"
            h = md5(hash_input.encode(), usedforsecurity=False).hexdigest()[:8]

            # Format: {dataset}-{hash}-{description}
            # Use safe dataset name for readability (replace special chars with hyphen)
            dataset_safe = (
                dataset_id.replace("/", "-")
                .replace(":", "-")
                .replace(".", "-")
                .replace("_", "-")[:30]
            )
            return URIRef(f"{base}{dataset_safe}-{h}-{desc}")

        for pat in self.patterns:
            pp = _pid(
                pat.subject_class,
                pat.property_uri,
                pat.object_class,
            )
            g.add((pp, void.property, URIRef(pat.property_uri)))
            g.add(
                (
                    pp,
                    void_ext.subjectClass,
                    URIRef(pat.subject_class),
                )
            )

            _add_void_object(
                g,
                pp,
                pat,
                void_ext,
                RDFS,
                XSD,
                base,
            )

            if pat.count is not None:
                g.add(
                    (
                        pp,
                        void.triples,
                        RdfLiteral(
                            pat.count,
                            datatype=XSD.integer,
                        ),
                    )
                )

            _add_void_labels(g, pat, URIRef, RdfLiteral, RDFS)

        _bind_discovered_prefixes(g, self.patterns)
        return g

    # LinkML export

    def to_linkml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
    ) -> Any:
        """Convert to LinkML SchemaDefinition with full metadata.

        Returns LinkML SchemaDefinition object.
        """
        from rdfsolve.schema_models.linkml import to_linkml

        jsonld = self.to_jsonld()
        return to_linkml(
            jsonld,
            schema_name=schema_name or self.about.dataset_name,
            schema_description=schema_description,
        )

    def to_linkml_yaml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
    ) -> str:
        """Convert to LinkML YAML with full metadata.

        Returns YAML string.
        """
        from typing import cast

        from linkml.generators.yamlgen import YAMLGenerator

        linkml_schema = self.to_linkml(schema_name, schema_description)
        return cast(str, YAMLGenerator(linkml_schema).serialize())

    def to_shacl(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
    ) -> str:
        """Convert to SHACL shapes via LinkML.

        Returns SHACL Turtle string.
        """
        from rdfsolve.schema_models.shacl import to_shacl

        jsonld = self.to_jsonld()
        return to_shacl(
            jsonld,
            schema_name=schema_name or self.about.dataset_name,
            schema_description=schema_description,
        )


# VoID graph helpers


def _add_void_object(
    g: Any,
    pp: Any,
    pat: SchemaPattern,
    void_ext: Any,
    rdfs: Any,
    xsd: Any,
    base: str,
) -> None:
    """Add object-class triple(s) for one pattern."""
    from rdflib import URIRef

    if pat.object_class == "Literal":
        g.add((pp, void_ext.objectClass, rdfs.Literal))
        if pat.datatype:
            h = md5(
                pat.datatype.encode(),
                usedforsecurity=False,
            ).hexdigest()[:12]
            dt_node = URIRef(f"{base}dt_{h}")
            g.add((pp, void_ext.datatypePartition, dt_node))
            g.add(
                (
                    dt_node,
                    void_ext.datatype,
                    URIRef(pat.datatype),
                )
            )
    elif pat.object_class == "Resource":
        g.add((pp, void_ext.objectClass, rdfs.Resource))
    else:
        g.add(
            (
                pp,
                void_ext.objectClass,
                URIRef(pat.object_class),
            )
        )


def _add_void_labels(
    g: Any,
    pat: SchemaPattern,
    uri_ref: Any,
    rdf_literal: Any,
    rdfs: Any,
) -> None:
    """Add rdfs:label triples for subject, property, object."""
    for uri, label in (
        (pat.subject_class, pat.subject_label),
        (pat.property_uri, pat.property_label),
    ):
        if label:
            g.add(
                (
                    uri_ref(uri),
                    rdfs.label,
                    rdf_literal(label),
                )
            )
    if pat.object_label and pat.object_class not in _SENTINEL_OBJECTS:
        g.add(
            (
                uri_ref(pat.object_class),
                rdfs.label,
                rdf_literal(pat.object_label),
            )
        )


def _bind_discovered_prefixes(
    g: Any,
    patterns: list[SchemaPattern],
) -> None:
    """Bind bioregistry-derived prefixes to the graph."""
    for pat in patterns:
        for uri in (
            pat.subject_class,
            pat.property_uri,
            pat.object_class,
        ):
            if uri in _SENTINEL_OBJECTS:
                continue
            _, pfx, ns = uri_to_curie(uri)
            if pfx and ns:
                try:
                    g.bind(pfx, ns, override=False)
                except Exception:
                    _log.debug(
                        "Could not bind %s=%s",
                        pfx,
                        ns,
                        exc_info=True,
                    )


# JSON-LD @graph parsers


def _parse_schema_graph(
    graph_nodes: list[Any],
    expand: Callable[[str], str],
    labels: dict[str, str],
) -> list[SchemaPattern]:
    """Parse @graph nodes into a list of SchemaPattern objects."""
    patterns: list[SchemaPattern] = []
    for node in graph_nodes:
        sc_curie = node.get("@id", "")
        if not sc_curie:
            continue
        sc_uri = expand(sc_curie)
        if not sc_uri.startswith(_URI_SCHEMES):
            continue
        counts_map: dict[str, dict[str, int]] = node.get(
            "_counts",
            {},
        )
        for key, val in node.items():
            if key.startswith(("@", "_")) or key in (_GRAPH_SKIP_KEYS):
                continue
            p_uri = expand(key)
            if not p_uri.startswith(_URI_SCHEMES):
                continue
            entries = val if isinstance(val, list) else [val]
            for entry in entries:
                pat = _parse_schema_entry(
                    entry,
                    sc_uri,
                    p_uri,
                    key,
                    sc_curie,
                    expand,
                    labels,
                    counts_map,
                )
                if pat:
                    patterns.append(pat)
    return patterns


def _parse_schema_entry(
    entry: Any,
    sc_uri: str,
    p_uri: str,
    key: str,
    sc_curie: str,
    expand: Callable[[str], str],
    labels: dict[str, str],
    counts_map: dict[str, dict[str, int]],
) -> SchemaPattern | None:
    """Parse a single @graph entry into a SchemaPattern or None."""
    if not isinstance(entry, dict):
        return None

    obj_id = entry.get("@id")
    obj_type = entry.get("@type")
    base = {
        "subject_class": sc_uri,
        "property_uri": p_uri,
        "subject_label": labels.get(sc_curie),
        "property_label": labels.get(key),
    }

    try:
        if obj_id is not None:
            oc_uri = expand(obj_id)
            count = counts_map.get(key, {}).get(
                obj_id,
                None,
            )
            if oc_uri in _RESOURCE_URIS:
                return SchemaPattern(
                    **base,
                    object_class="Resource",
                    count=count,
                )
            if oc_uri.startswith(_URI_SCHEMES):
                return SchemaPattern(
                    **base,
                    object_class=oc_uri,
                    count=count,
                    object_label=labels.get(obj_id),
                )
        elif obj_type is not None:
            dt_uri = expand(obj_type)
            return SchemaPattern(
                **base,
                object_class="Literal",
                datatype=dt_uri,
                count=counts_map.get(key, {}).get(
                    obj_type,
                    None,
                ),
            )
    except Exception:
        _log.debug(
            "Skipping invalid pattern entry",
            exc_info=True,
        )

    return None
