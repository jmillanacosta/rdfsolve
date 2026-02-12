"""
Pydantic models for RDF schema representation.

Provides type-safe data structures with validation for schema elements.
Shared by both VoidParser (VoID-based extraction) and SchemaMiner
(direct SPARQL mining).
"""

import re
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

# ---------------------------------------------------------------------------
# Shared URI helpers
# ---------------------------------------------------------------------------

def uri_to_curie(uri: str) -> tuple[str, str, str]:
    """Convert a URI to a CURIE using bioregistry, with fallback.

    Returns:
        (curie, prefix, namespace_uri)
    """
    curie = None
    prefix = None
    namespace_uri = None

    if uri.startswith(("http://", "https://")):
        try:
            from bioregistry import curie_from_iri, parse_iri

            parsed = parse_iri(uri)
            if parsed:
                prefix, local_id = parsed
                if local_id in uri:
                    idx = uri.rfind(local_id)
                    namespace_uri = uri[:idx]
                elif "#" in uri:
                    namespace_uri = uri.rsplit("#", 1)[0] + "#"
                else:
                    namespace_uri = uri.rsplit("/", 1)[0] + "/"
                curie = curie_from_iri(uri)
                if not curie and prefix and local_id:
                    curie = f"{prefix}:{local_id}"
        except Exception:
            pass

    if not curie:
        if "#" in uri:
            ns_part, local_part = uri.rsplit("#", 1)
            namespace_uri = ns_part + "#"
        elif "/" in uri:
            ns_part, local_part = uri.rsplit("/", 1)
            namespace_uri = ns_part + "/"
        else:
            local_part = uri

        if not prefix and namespace_uri:
            clean = (
                namespace_uri.replace("http://", "")
                .replace("https://", "")
                .replace("www.", "")
                .strip("/")
                .strip("#")
            )
            if "/" in clean:
                parts = clean.split("/")
                prefix = parts[-1] if parts[-1] else (
                    parts[-2] if len(parts) > 1 else "ns"
                )
            else:
                prefix = clean.split(".")[0] if "." in clean else clean
            prefix = re.sub(r"[^a-zA-Z0-9_]", "", prefix)[:10]

        curie = f"{prefix}:{local_part}" if prefix and local_part else uri

    return curie or uri, prefix or "", namespace_uri or ""


class SchemaTriple(BaseModel):
    """A single schema relationship triple."""

    subject_class: str = Field(..., description="Subject class name")
    subject_uri: str = Field(..., description="Subject class URI")
    property: str = Field(..., description="Property name")
    property_uri: str = Field(..., description="Property URI")
    object_class: str = Field(..., description="Object class name")
    object_uri: str = Field(..., description="Object URI")

    @field_validator("subject_uri", "property_uri", "object_uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validate that URIs are properly formatted."""
        if not v.startswith(("http://", "https://", "urn:")):
            if v not in ["Literal", "Resource"]:
                raise ValueError(f"Invalid URI format: {v}")
        return v


class SchemaMetadata(BaseModel):
    """Metadata about the extracted schema."""

    total_triples: int = Field(..., ge=0, description="Total number of triples")
    total_classes: int = Field(..., ge=0, description="Total number of classes")
    total_properties: int = Field(..., ge=0, description="Total number of properties")
    dataset_name: Optional[str] = Field(None, description="Name of the dataset")
    extraction_date: Optional[str] = Field(None, description="Date of extraction")
    source_endpoint: Optional[HttpUrl] = Field(None, description="Source SPARQL endpoint")

    model_config = ConfigDict(extra="forbid")


class VoidSchema(BaseModel):
    """Complete VoID-extracted schema with triples and metadata."""

    triples: List[SchemaTriple] = Field(..., description="Schema triples")
    metadata: SchemaMetadata = Field(..., description="Schema metadata")

    @field_validator("triples")
    @classmethod
    def validate_triples(cls, v: List[SchemaTriple]) -> List[SchemaTriple]:
        """Ensure we have at least some triples for a valid schema."""
        if not v:
            raise ValueError("Schema must contain at least one triple")
        return v

    def get_classes(self) -> List[str]:
        """Get all unique class names."""
        classes = set()
        for triple in self.triples:
            classes.add(triple.subject_class)
            if triple.object_class not in ["Literal", "Resource"]:
                classes.add(triple.object_class)
        return sorted(classes)

    def get_properties(self) -> List[str]:
        """Get all unique property names."""
        return sorted({t.property for t in self.triples})

    def get_class_properties(self, class_name: str) -> List[str]:
        """Get all properties used by a specific class."""
        return sorted({t.property for t in self.triples if t.subject_class == class_name})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "triples": [[t.subject_uri, t.property_uri, t.object_uri] for t in self.triples],
            "metadata": self.metadata.dict(),
            "classes": self.get_classes(),
            "properties": self.get_properties(),
        }


class LinkMLClass(BaseModel):
    """Represents a LinkML class definition."""

    name: str = Field(..., description="Class name")
    description: Optional[str] = Field(None, description="Class description")
    slots: List[str] = Field(default_factory=list, description="Slot names")
    class_uri: Optional[str] = Field(None, description="Class URI")


class LinkMLSlot(BaseModel):
    """Represents a LinkML slot definition."""

    name: str = Field(..., description="Slot name")
    description: Optional[str] = Field(None, description="Slot description")
    range: str = Field(..., description="Slot range type")
    domain_of: List[str] = Field(default_factory=list, description="Classes using this slot")
    required: bool = Field(False, description="Whether slot is required")
    multivalued: bool = Field(False, description="Whether slot accepts multiple values")
    slot_uri: Optional[str] = Field(None, description="Slot URI")


class LinkMLSchema(BaseModel):
    """Represents a complete LinkML schema.

    LinkML schemas can be exported to multiple formats including:
    - YAML: Human-readable schema definition
    - JSON Schema: For JSON validation
    - SHACL: For RDF data validation (Shapes Constraint Language)
    - Python: Pydantic models for data validation
    - And more...
    """

    id: str = Field(..., description="Schema ID")
    name: str = Field(..., description="Schema name")
    description: Optional[str] = Field(None, description="Schema description")
    classes: Dict[str, LinkMLClass] = Field(default_factory=dict, description="Class definitions")
    slots: Dict[str, LinkMLSlot] = Field(default_factory=dict, description="Slot definitions")

    def get_class_count(self) -> int:
        """Get number of classes."""
        return len(self.classes)

    def get_slot_count(self) -> int:
        """Get number of slots."""
        return len(self.slots)

    def get_object_properties(self) -> List[str]:
        """Get slots that reference other classes (object properties)."""
        object_props = []
        for slot_name, slot in self.slots.items():
            if slot.range in self.classes:
                object_props.append(slot_name)
        return sorted(object_props)


# -------------------------------------------------------------------
# Shared models for schema mining and VoID parsing
# -------------------------------------------------------------------

class SchemaPattern(BaseModel):
    """A single schema pattern: subject_class → property → object.

    Captures three kinds of relationships:

    - **typed-object**:
      ``?s a ?sc . ?s ?p ?o . ?o a ?oc``
    - **literal**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isLiteral(?o))``
    - **untyped-uri**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isURI(?o)) FILTER NOT EXISTS { ?o a ?any }``

    This model is the shared contract between SchemaMiner (direct SPARQL)
    and VoidParser (VoID-based extraction).
    """

    subject_class: str = Field(
        ..., description="URI of the subject class",
    )
    property_uri: str = Field(
        ..., description="URI of the property",
    )
    object_class: str = Field(
        ...,
        description=(
            "URI of the object class, or the special sentinel "
            "'Literal' / 'Resource'"
        ),
    )
    count: Optional[int] = Field(
        None, ge=0,
        description="Number of triples matching this pattern",
    )
    datatype: Optional[str] = Field(
        None,
        description=(
            "XSD datatype URI for literal objects "
            "(only when object_class == 'Literal')"
        ),
    )
    subject_label: Optional[str] = Field(
        None,
        description=(
            "Human-readable label for the subject class "
            "(rdfs:label > dc:title > local name)"
        ),
    )
    property_label: Optional[str] = Field(
        None,
        description=(
            "Human-readable label for the property "
            "(rdfs:label > dc:title > local name)"
        ),
    )
    object_label: Optional[str] = Field(
        None,
        description=(
            "Human-readable label for the object class "
            "(rdfs:label > dc:title > local name)"
        ),
    )

    @field_validator("subject_class", "property_uri")
    @classmethod
    def _validate_uri(cls, v: str) -> str:
        if not v.startswith(("http://", "https://", "urn:")):
            raise ValueError(f"Invalid URI: {v}")
        return v

    @field_validator("object_class")
    @classmethod
    def _validate_object(cls, v: str) -> str:
        if v in ("Literal", "Resource"):
            return v
        if not v.startswith(("http://", "https://", "urn:")):
            raise ValueError(f"Invalid object class: {v}")
        return v


class AboutMetadata(BaseModel):
    """Provenance metadata attached to every schema export."""

    generated_by: str = Field(
        ..., description="Tool and version string",
    )
    generated_at: str = Field(
        ..., description="ISO-8601 timestamp (UTC)",
    )
    endpoint: Optional[str] = Field(
        None, description="SPARQL endpoint URL",
    )
    dataset_name: Optional[str] = Field(
        None, description="Human-readable dataset name",
    )
    graph_uris: Optional[List[str]] = Field(
        None, description="Named graph URIs queried",
    )
    pattern_count: int = Field(
        0, ge=0, description="Number of schema patterns",
    )
    strategy: str = Field(
        "unknown",
        description="Mining strategy used (e.g. 'miner', 'void')",
    )

    model_config = ConfigDict(extra="allow")

    @staticmethod
    def build(
        endpoint: Optional[str] = None,
        dataset_name: Optional[str] = None,
        graph_uris: Optional[List[str]] = None,
        pattern_count: int = 0,
        strategy: str = "unknown",
    ) -> "AboutMetadata":
        """Convenience factory with auto-populated version/time."""
        from rdfsolve.version import VERSION

        return AboutMetadata(
            generated_by=f"rdfsolve {VERSION}",
            generated_at=(
                datetime.now(timezone.utc).isoformat()
            ),
            endpoint=endpoint,
            dataset_name=dataset_name,
            graph_uris=graph_uris,
            pattern_count=pattern_count,
            strategy=strategy,
        )


# -------------------------------------------------------------------
# Mining analytics report
# -------------------------------------------------------------------


class QueryStats(BaseModel):
    """Cumulative statistics for one query category."""

    sent: int = Field(0, ge=0, description="Queries sent")
    failed: int = Field(0, ge=0, description="Queries that failed")
    total_time_s: float = Field(
        0.0, ge=0, description="Wall-clock seconds for this category",
    )

    model_config = ConfigDict(extra="forbid")


class PhaseReport(BaseModel):
    """Timing and outcome for one mining phase."""

    name: str = Field(..., description="Phase identifier")
    started_at: Optional[str] = Field(
        None, description="ISO-8601 start time",
    )
    finished_at: Optional[str] = Field(
        None, description="ISO-8601 finish time",
    )
    duration_s: Optional[float] = Field(
        None, ge=0, description="Wall-clock seconds",
    )
    items_discovered: int = Field(
        0, ge=0,
        description="Number of items produced by this phase",
    )
    error: Optional[str] = Field(
        None, description="Error message if the phase failed",
    )

    model_config = ConfigDict(extra="forbid")


class MiningReport(BaseModel):
    """Analytical metadata collected during a mining run.

    Designed to be written to disk incrementally (after each phase
    completes) so that partial data is preserved even if mining
    crashes midway.
    """

    # ── Identification ─────────────────────────────────────────────
    dataset_name: Optional[str] = Field(
        None, description="Human-readable name of the mined dataset",
    )
    endpoint_url: str = Field(
        ..., description="SPARQL endpoint URL",
    )
    graph_uris: Optional[List[str]] = Field(
        None, description="Named-graph URIs (if any)",
    )
    strategy: str = Field(
        "unknown",
        description="Mining strategy: 'miner' or 'miner/two-phase'",
    )

    # ── Versions & environment ─────────────────────────────────────
    rdfsolve_version: str = Field(
        ..., description="Package version string",
    )
    python_version: str = Field(
        ..., description="Python interpreter version",
    )

    # ── Timing ─────────────────────────────────────────────────────
    started_at: str = Field(
        ..., description="ISO-8601 timestamp when mining started",
    )
    finished_at: Optional[str] = Field(
        None, description="ISO-8601 timestamp when mining finished",
    )
    total_duration_s: Optional[float] = Field(
        None, ge=0, description="Total wall-clock seconds",
    )

    # ── Query statistics (by purpose tag) ──────────────────────────
    query_stats: Dict[str, QueryStats] = Field(
        default_factory=dict,
        description=(
            "Per-purpose query statistics.  Keys are purpose tags "
            "such as 'mining/typed-object', 'labels', etc."
        ),
    )
    total_queries_sent: int = Field(
        0, ge=0, description="Grand total of queries sent",
    )
    total_queries_failed: int = Field(
        0, ge=0, description="Grand total of failed queries",
    )

    # ── Phase breakdown ────────────────────────────────────────────
    phases: List[PhaseReport] = Field(
        default_factory=list,
        description="Ordered list of mining phases with timing",
    )

    # ── Results summary ────────────────────────────────────────────
    pattern_count: int = Field(
        0, ge=0, description="Number of schema patterns extracted",
    )
    class_count: int = Field(
        0, ge=0, description="Number of unique classes",
    )
    property_count: int = Field(
        0, ge=0, description="Number of unique properties",
    )
    unique_uris_labelled: int = Field(
        0, ge=0,
        description="Number of URIs for which labels were fetched",
    )

    # ── Configuration snapshot ─────────────────────────────────────
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "SchemaMiner configuration: chunk_size, delay, "
            "timeout, counts, two_phase"
        ),
    )

    model_config = ConfigDict(extra="allow")


class MinedSchema(BaseModel):
    """Complete mined schema: patterns + provenance.

    Primary export format is JSON-LD. Can also be converted to a
    VoID RDF graph for downstream conversion to LinkML / SHACL /
    RDF-config via VoidParser.
    """

    patterns: List[SchemaPattern] = Field(
        default_factory=list,
        description="Schema patterns",
    )
    about: AboutMetadata = Field(
        ..., description="Provenance metadata",
    )

    # ---- Queries --------------------------------------------------

    def get_classes(self) -> List[str]:
        """Return sorted unique subject/object class URIs."""
        classes: set[str] = set()
        for p in self.patterns:
            classes.add(p.subject_class)
            if p.object_class not in ("Literal", "Resource"):
                classes.add(p.object_class)
        return sorted(classes)

    def get_properties(self) -> List[str]:
        """Return sorted unique property URIs."""
        return sorted({p.property_uri for p in self.patterns})

    # ---- JSON-LD export -------------------------------------------

    def to_jsonld(self) -> Dict[str, Any]:
        """Export schema as JSON-LD with @context, @graph, @about.

        The @graph groups triples by subject class. Each subject node
        lists its properties with ``{"@id": object_curie}`` or literal
        sentinels.

        Labels are exported in a top-level ``_labels`` map keyed by
        CURIE, so the frontend can display human-readable names.

        When triple counts are available they are stored per class
        node in a ``_counts`` dict keyed by
        ``"property_curie|object_key"`` → count, where *object_key*
        is the object CURIE, ``"Literal"`` / ``"Literal:datatype"``,
        or ``"Resource"``.  This preserves full (s, p, o) granularity.
        """
        context: Dict[str, str] = {}
        grouped: Dict[str, Dict[str, Any]] = {}
        # Per-class count maps: sc → {pp → {o_key → count}}
        node_counts: Dict[str, Dict[str, Dict[str, int]]] = {}
        labels: Dict[str, str] = {}

        for pat in self.patterns:
            sc, sc_pfx, sc_ns = uri_to_curie(pat.subject_class)
            pp, pp_pfx, pp_ns = uri_to_curie(pat.property_uri)

            if sc_pfx and sc_ns:
                context[sc_pfx] = sc_ns
            if pp_pfx and pp_ns:
                context[pp_pfx] = pp_ns

            # Collect labels (CURIE → human label)
            if pat.subject_label:
                labels[sc] = pat.subject_label
            if pat.property_label:
                labels[pp] = pat.property_label

            # Build object value
            if pat.object_class == "Literal":
                if pat.datatype:
                    dt_c, dt_pfx, dt_ns = uri_to_curie(pat.datatype)
                    if dt_pfx and dt_ns:
                        context[dt_pfx] = dt_ns
                    o_val: Any = {"@type": dt_c}
                    o_key = f"Literal:{dt_c}"
                else:
                    # Untyped literal → xsd:string per RDF 1.1
                    context.setdefault(
                        "xsd",
                        "http://www.w3.org/2001/XMLSchema#",
                    )
                    o_val = {"@type": "xsd:string"}
                    o_key = "Literal:xsd:string"
            elif pat.object_class == "Resource":
                o_val = {"@id": "rdfs:Resource"}
                context.setdefault(
                    "rdfs",
                    "http://www.w3.org/2000/01/rdf-schema#",
                )
                o_key = "Resource"
            else:
                oc, oc_pfx, oc_ns = uri_to_curie(
                    pat.object_class,
                )
                if oc_pfx and oc_ns:
                    context[oc_pfx] = oc_ns
                o_val = {"@id": oc}
                o_key = oc
                if pat.object_label:
                    labels[oc] = pat.object_label

            # Record count nested: sc → pp → o_key → count
            if pat.count is not None:
                node_counts.setdefault(
                    sc, {},
                ).setdefault(
                    pp, {},
                )[o_key] = pat.count

            # Merge into grouped node
            if sc not in grouped:
                grouped[sc] = {"@id": sc}

            existing = grouped[sc].get(pp)
            if existing is None:
                grouped[sc][pp] = o_val
            else:
                if not isinstance(existing, list):
                    existing = [existing]
                if o_val not in existing:
                    existing.append(o_val)
                grouped[sc][pp] = existing

        # Attach _counts to each class node that has them
        for sc_curie, cmap in node_counts.items():
            if sc_curie in grouped:
                grouped[sc_curie]["_counts"] = cmap

        result: Dict[str, Any] = {
            "@context": context,
            "@graph": list(grouped.values()),
            "@about": self.about.model_dump(exclude_none=True),
        }
        if labels:
            result["_labels"] = labels
        return result

    # ---- VoID graph export ----------------------------------------

    def to_void_graph(self) -> "Graph":  # type: ignore[name-defined]  # noqa: F821
        """Build an rdflib VoID Graph from the mined patterns.

        This allows feeding the result into VoidParser for downstream
        conversion to LinkML, SHACL, RDF-config, etc.
        """
        from rdflib import Graph, Namespace, URIRef
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, RDFS, XSD

        VOID = Namespace("http://rdfs.org/ns/void#")
        VOID_EXT = Namespace("http://ldf.fi/void-ext#")

        g = Graph()
        g.bind("void", VOID)
        g.bind("void-ext", VOID_EXT)
        g.bind("rdf", RDF)
        g.bind("rdfs", RDFS)
        g.bind("xsd", XSD)

        # Determine a base URI for partition IRIs
        endpoint = self.about.endpoint or "urn:rdfsolve"
        base = endpoint.rstrip("/") + "/void/"

        def _partition_id(s: str, p: str, o: str) -> URIRef:
            h = md5(f"{s}|{p}|{o}".encode()).hexdigest()[:12]
            return URIRef(f"{base}pp_{h}")

        for pat in self.patterns:
            pp_uri = _partition_id(
                pat.subject_class, pat.property_uri, pat.object_class,
            )
            g.add((pp_uri, VOID.property, URIRef(pat.property_uri)))
            g.add((
                pp_uri, VOID_EXT.subjectClass,
                URIRef(pat.subject_class),
            ))

            if pat.object_class == "Literal":
                g.add((
                    pp_uri, VOID_EXT.objectClass,
                    RDFS.Literal,
                ))
                if pat.datatype:
                    dt_node = URIRef(
                        f"{base}dt_{md5(pat.datatype.encode()).hexdigest()[:12]}"
                    )
                    g.add((
                        pp_uri, VOID_EXT.datatypePartition, dt_node,
                    ))
                    g.add((
                        dt_node, VOID_EXT.datatype,
                        URIRef(pat.datatype),
                    ))
            elif pat.object_class == "Resource":
                g.add((
                    pp_uri, VOID_EXT.objectClass, RDFS.Resource,
                ))
            else:
                g.add((
                    pp_uri, VOID_EXT.objectClass,
                    URIRef(pat.object_class),
                ))

            if pat.count is not None:
                g.add((
                    pp_uri, VOID.triples,
                    RdfLiteral(pat.count, datatype=XSD.integer),
                ))

            # Add labels as rdfs:label triples
            if pat.subject_label:
                g.add((
                    URIRef(pat.subject_class),
                    RDFS.label,
                    RdfLiteral(pat.subject_label),
                ))
            if pat.property_label:
                g.add((
                    URIRef(pat.property_uri),
                    RDFS.label,
                    RdfLiteral(pat.property_label),
                ))
            if pat.object_label and pat.object_class not in (
                "Literal", "Resource",
            ):
                g.add((
                    URIRef(pat.object_class),
                    RDFS.label,
                    RdfLiteral(pat.object_label),
                ))

        # Bind prefixes discovered via bioregistry
        for pat in self.patterns:
            for uri in (
                pat.subject_class, pat.property_uri, pat.object_class,
            ):
                if uri in ("Literal", "Resource"):
                    continue
                _, pfx, ns = uri_to_curie(uri)
                if pfx and ns:
                    try:
                        g.bind(pfx, ns, override=False)
                    except Exception:
                        pass

        return g
