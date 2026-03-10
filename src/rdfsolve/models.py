"""
Pydantic models for RDF schema representation.

Provides type-safe data structures with validation for schema elements.
Shared by both VoidParser (VoID-based extraction) and SchemaMiner
(direct SPARQL mining), and the instance-based matcher.
"""

import re
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

# ---------------------------------------------------------------------------
# Service / system namespace prefixes
# ---------------------------------------------------------------------------
# URIs starting with any of these prefixes are considered internal to the
# triple-store infrastructure (Virtuoso, OpenLink, SPARQL service graphs)
# and are excluded from mined schemas when ``filter_service_namespaces``
# is active.  The list is intentionally conservative – only namespaces
# that are unambiguously infrastructure are included.

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

    This model is shared contract between SchemaMiner (direct SPARQL)
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


_BASE_URI = "https://jmillanacosta.com/rdfsolve"


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

    # Versions
    rdfsolve_version: Optional[str] = Field(
        None, description="rdfsolve version string",
    )
    qlever_version: Optional[Dict[str, str]] = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's ?cmd=stats: "
            '{"git_hash_server": str, "git_hash_index": str}'
        ),
    )

    # Timing
    started_at: Optional[str] = Field(
        None, description="ISO-8601 timestamp when mining started",
    )
    finished_at: Optional[str] = Field(
        None, description="ISO-8601 timestamp when mining finished",
    )
    total_duration_s: Optional[float] = Field(
        None, ge=0, description="Total wall-clock seconds",
    )

    # Provenance
    authors: Optional[List[Dict[str, str]]] = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # Canonical URIs (auto-populated from dataset_name)
    schema_uri: Optional[str] = Field(
        None,
        description="Canonical URI where this schema is served",
    )
    void_uri: Optional[str] = Field(
        None,
        description="Canonical URI where the VoID catalog is served",
    )
    report_uri: Optional[str] = Field(
        None,
        description="Canonical URI where the run report is served",
    )
    linkml_uri: Optional[str] = Field(
        None,
        description="Canonical URI where the LinkML schema is served",
    )

    model_config = ConfigDict(extra="allow")

    @staticmethod
    def build(
        endpoint: Optional[str] = None,
        dataset_name: Optional[str] = None,
        graph_uris: Optional[List[str]] = None,
        pattern_count: int = 0,
        strategy: str = "unknown",
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
        total_duration_s: Optional[float] = None,
        authors: Optional[List[Dict[str, str]]] = None,
        qlever_version: Optional[Dict[str, str]] = None,
    ) -> "AboutMetadata":
        """Convenience factory with auto-populated version/time."""
        from rdfsolve.version import VERSION

        schema_uri = (
            f"{_BASE_URI}/api/schemas/{dataset_name}"
            if dataset_name else None
        )
        void_uri = (
            f"{_BASE_URI}/api/void/{dataset_name}"
            if dataset_name else None
        )
        report_uri = (
            f"{_BASE_URI}/api/reports/{dataset_name}"
            if dataset_name else None
        )
        linkml_uri = (
            f"{_BASE_URI}/api/linkml/{dataset_name}"
            if dataset_name else None
        )

        return AboutMetadata(
            generated_by=f"rdfsolve {VERSION}",
            generated_at=datetime.now(timezone.utc).isoformat(),
            endpoint=endpoint,
            dataset_name=dataset_name,
            graph_uris=graph_uris,
            pattern_count=pattern_count,
            strategy=strategy,
            rdfsolve_version=VERSION,
            started_at=started_at,
            finished_at=finished_at,
            total_duration_s=total_duration_s,
            authors=authors,
            qlever_version=qlever_version,
            schema_uri=schema_uri,
            void_uri=void_uri,
            report_uri=report_uri,
            linkml_uri=linkml_uri,
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


class OneShotQueryResult(BaseModel):
    """Outcome of a single unbounded SELECT against a SPARQL endpoint.

    Used to record the raw performance of an unguarded one-shot query
    so it can be compared against the fallback-chain result in the
    same report.
    """

    query_type: str = Field(
        ...,
        description=(
            "Pattern type queried: 'typed-object', 'literal', "
            "or 'untyped-uri'"
        ),
    )
    success: bool = Field(
        ...,
        description="True if the endpoint returned a result set",
    )
    duration_s: Optional[float] = Field(
        None, ge=0,
        description="Wall-clock seconds for the single HTTP call",
    )
    row_count: Optional[int] = Field(
        None, ge=0,
        description=(
            "Number of result rows returned (None on failure)"
        ),
    )
    error: Optional[str] = Field(
        None,
        description="Exception message if the query failed",
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
    qlever_version: Optional[Dict[str, str]] = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's ?cmd=stats: "
            '{"git_hash_server": str, "git_hash_index": str}'
        ),
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
    abort_reason: Optional[str] = Field(
        None,
        description=(
            "If mining was cut short (e.g. endpoint unhealthy), "
            "the reason is recorded here."
        ),
    )
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

    # ── Benchmark / resource usage (populated at finalise) ─────────
    machine: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Machine info: hostname, OS, CPU model, logical/physical "
            "cores, RAM, Python version"
        ),
    )
    benchmark: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Resource usage: wall_time_s, cpu_user_s, cpu_system_s, "
            "peak_rss_mb, read_bytes, write_bytes"
        ),
    )

    # ── One-shot baseline (for comparison against fallback chain) ──
    one_shot_results: Optional[List[OneShotQueryResult]] = Field(
        None,
        description=(
            "Results of running each pattern query as a single "
            "unbounded SELECT (no LIMIT/OFFSET/fallback). "
            "Populated when mining is run in one-shot mode or when "
            "a one-shot baseline pass is requested alongside the "
            "standard fallback-chain pass. Null when not requested."
        ),
    )

    # ── Author provenance ──────────────────────────────────────────
    authors: Optional[List[Dict[str, str]]] = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # ── Captured endpoint metadata (DC / VoID / DCAT) ─────────────
    dataset_metadata: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Self-describing metadata captured from the endpoint's "
            "VoID/DC/DCAT declarations (may be null if the endpoint "
            "does not publish any)."
        ),
    )

    # ── Canonical URI ──────────────────────────────────────────────
    report_uri: Optional[str] = Field(
        None,
        description=(
            "Canonical URI where this report is served, "
            "e.g. https://jmillanacosta.com/rdfsolve/api/reports/{dataset}"
        ),
    )

    model_config = ConfigDict(extra="allow")


# -------------------------------------------------------------------
# Shared base for all run reports
# -------------------------------------------------------------------


class BaseReport(BaseModel):
    """Fields common to both mined and discovered schema reports."""

    dataset_name: Optional[str] = Field(
        None, description="Human-readable name of the dataset",
    )
    endpoint_url: str = Field(
        "", description="SPARQL endpoint URL",
    )
    graph_uris: Optional[List[str]] = Field(
        None, description="Named-graph URIs (if any)",
    )
    strategy: str = Field(
        "unknown", description="Strategy string",
    )

    # Versions
    rdfsolve_version: str = Field(
        "", description="Package version string",
    )
    python_version: str = Field(
        "", description="Python interpreter version",
    )
    qlever_version: Optional[Dict[str, str]] = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's ?cmd=stats: "
            '{"git_hash_server": str, "git_hash_index": str}'
        ),
    )

    # Timing
    started_at: str = Field(
        "", description="ISO-8601 timestamp when run started",
    )
    finished_at: Optional[str] = Field(
        None, description="ISO-8601 timestamp when run finished",
    )
    total_duration_s: Optional[float] = Field(
        None, ge=0, description="Total wall-clock seconds",
    )

    # Provenance
    authors: Optional[List[Dict[str, str]]] = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # Machine & benchmark
    machine: Optional[Dict[str, Any]] = Field(None)
    benchmark: Optional[Dict[str, Any]] = Field(None)

    # Captured endpoint metadata
    dataset_metadata: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Self-describing metadata captured from the endpoint's "
            "VoID/DC/DCAT declarations."
        ),
    )

    # Canonical URI
    report_uri: Optional[str] = Field(
        None,
        description=(
            "Canonical URI where this report is served"
        ),
    )

    model_config = ConfigDict(extra="allow")


class DiscoveryReport(BaseReport):
    """Report for a VoID discovery run."""

    strategy: str = Field(
        "discovery/void",
        description="Strategy string (always 'discovery/void')",
    )

    graphs_found: int = Field(
        0, ge=0, description="Named graphs found",
    )
    partitions_found: int = Field(
        0, ge=0, description="VoID partitions found",
    )
    void_file: Optional[str] = Field(
        None, description="Path to the generated VoID Turtle file",
    )




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

    # ---- Service-namespace filtering ------------------------------

    def filter_service_namespaces(
        self,
        extra_prefixes: Optional[List[str]] = None,
    ) -> "MinedSchema":
        """Return a copy with service/system namespace patterns removed.

        A pattern is removed when **any** of its ``subject_class``,
        ``property_uri``, or ``object_class`` starts with a prefix
        listed in :data:`SERVICE_NAMESPACE_PREFIXES` (or in
        *extra_prefixes*).

        This is intended as a **post-mining** clean-up step so the
        actual SPARQL queries remain simple (no per-namespace
        ``FILTER``).  The filtering is cheap because it operates on
        the already-collected in-memory list of
        :class:`SchemaPattern` objects.

        Parameters
        ----------
        extra_prefixes:
            Additional URI prefixes to treat as service
            namespaces, on top of the built-in list.

        Returns
        -------
        MinedSchema
            A new schema with the offending patterns stripped.
        """
        prefixes = SERVICE_NAMESPACE_PREFIXES
        if extra_prefixes:
            prefixes = tuple(prefixes) + tuple(extra_prefixes)

        def _is_service(uri: str) -> bool:
            return uri.startswith(prefixes)

        kept = [
            p for p in self.patterns
            if not (
                _is_service(p.subject_class)
                or _is_service(p.property_uri)
                or (
                    p.object_class not in ("Literal", "Resource")
                    and _is_service(p.object_class)
                )
            )
        ]
        return self.model_copy(update={"patterns": kept})

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

    # ---- JSON-LD import -------------------------------------------

    @classmethod
    def from_jsonld(cls, path: "str | Path") -> "MinedSchema":
        """Reconstruct a :class:`MinedSchema` from a schema JSON-LD file.

        Inverse of :meth:`to_jsonld`.  Expands CURIEs using the file's
        own ``@context`` block, so no external resolver is required.

        Args:
            path: Path to a ``*_schema.jsonld`` file produced by
                ``rdfsolve mine``.

        Returns:
            :class:`MinedSchema` with fully-expanded URIs in all patterns.
        """
        import json as _json
        from pathlib import Path as _Path

        raw = _json.loads(_Path(path).read_text(encoding="utf-8"))
        context: Dict[str, str] = raw.get("@context", {})
        about_data = raw.get("@about", {})
        labels: Dict[str, str] = raw.get("_labels", {})

        def _expand(curie: str) -> str:
            if curie.startswith(("http://", "https://", "urn:")):
                return curie
            if ":" in curie:
                prefix, local = curie.split(":", 1)
                ns = context.get(prefix)
                if ns and isinstance(ns, str):
                    return ns + local
            return curie

        patterns: List[SchemaPattern] = []
        for node in raw.get("@graph", []):
            sc_curie = node.get("@id", "")
            if not sc_curie:
                continue
            sc_uri = _expand(sc_curie)
            if not sc_uri.startswith(("http://", "https://", "urn:")):
                continue
            counts_map: Dict[str, Dict[str, int]] = node.get("_counts", {})
            for key, val in node.items():
                if key.startswith("@") or key.startswith("_"):
                    continue
                if key in ("void:inDataset", "dcterms:created", "dcterms:title"):
                    continue
                p_uri = _expand(key)
                if not p_uri.startswith(("http://", "https://", "urn:")):
                    continue
                entries = val if isinstance(val, list) else [val]
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    obj_id = entry.get("@id")
                    obj_type = entry.get("@type")
                    if obj_id is not None:
                        oc_uri = _expand(obj_id)
                        if oc_uri in (
                            "http://www.w3.org/2000/01/rdf-schema#Resource",
                            "rdfs:Resource",
                            "Resource",
                        ):
                            try:
                                patterns.append(SchemaPattern(
                                    subject_class=sc_uri,
                                    property_uri=p_uri,
                                    object_class="Resource",
                                    count=counts_map.get(key, {}).get(
                                        obj_id or "", None
                                    ),
                                    subject_label=labels.get(sc_curie),
                                    property_label=labels.get(key),
                                ))
                            except Exception:
                                pass
                        elif oc_uri.startswith(("http://", "https://", "urn:")):
                            try:
                                patterns.append(SchemaPattern(
                                    subject_class=sc_uri,
                                    property_uri=p_uri,
                                    object_class=oc_uri,
                                    count=counts_map.get(key, {}).get(
                                        obj_id or "", None
                                    ),
                                    subject_label=labels.get(sc_curie),
                                    property_label=labels.get(key),
                                    object_label=labels.get(obj_id),
                                ))
                            except Exception:
                                pass
                    elif obj_type is not None:
                        dt_uri = _expand(obj_type)
                        try:
                            patterns.append(SchemaPattern(
                                subject_class=sc_uri,
                                property_uri=p_uri,
                                object_class="Literal",
                                datatype=dt_uri,
                                count=counts_map.get(key, {}).get(
                                    obj_type or "", None
                                ),
                                subject_label=labels.get(sc_curie),
                                property_label=labels.get(key),
                            ))
                        except Exception:
                            pass

        # Normalise camelCase @about keys produced by older pipeline versions
        _camel_map = {
            "generatedBy":   "generated_by",
            "generatedAt":   "generated_at",
            "datasetName":   "dataset_name",
            "graphUris":     "graph_uris",
            "patternCount":  "pattern_count",
            "tripleCount":   "pattern_count",
            "rdfsolveVersion": "rdfsolve_version",
            "qleverVersion": "qlever_version",
        }
        about_data = {
            _camel_map.get(k, k): v for k, v in about_data.items()
        }
        # Inject sentinel values for required fields missing in old files
        about_data.setdefault("generated_by", "rdfsolve (legacy)")
        about_data.setdefault("generated_at", "1970-01-01T00:00:00+00:00")

        about = AboutMetadata.model_validate(about_data)
        return cls(patterns=patterns, about=about)

    # ---- NetworkX export ------------------------------------------

    def to_networkx(self) -> "Any":
        """Export the schema as a typed-object ``nx.MultiDiGraph``.

        Nodes are class URIs.  Each typed-object pattern
        ``(subject_class, property_uri, object_class)`` becomes a
        directed edge with attributes:

        * ``predicate`` — property URI
        * ``dataset`` — ``about.dataset_name``
        * ``count`` — co-occurrence count (may be ``None``)

        Literal and Resource sentinel patterns are **excluded**; they
        do not contribute edges between named classes.

        Node attributes:

        * ``dataset`` — ``about.dataset_name``
        * ``label`` — ``subject_label`` / ``object_label`` if present

        Returns:
            ``networkx.MultiDiGraph``
        """
        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for to_networkx(); "
                "install it with: pip install networkx"
            ) from exc

        G: "Any" = _nx.MultiDiGraph()
        dataset = self.about.dataset_name or ""

        for pat in self.patterns:
            if pat.object_class in ("Literal", "Resource"):
                continue
            for uri, label in (
                (pat.subject_class, pat.subject_label),
                (pat.object_class,  pat.object_label),
            ):
                if uri not in G:
                    G.add_node(uri, dataset=dataset, label=label or "")
            G.add_edge(
                pat.subject_class,
                pat.object_class,
                predicate=pat.property_uri,
                dataset=dataset,
                count=pat.count,
            )
        return G

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


# ---------------------------------------------------------------------------
# Instance-based mapping models
# ---------------------------------------------------------------------------

SKOS_NARROW_MATCH = "http://www.w3.org/2004/02/skos/core#narrowMatch"


class MappingEdge(BaseModel):
    """A single mapping edge asserting a relationship between two classes.

    Used by :class:`Mapping` and its subclasses to represent cross-dataset
    links discovered by the instance matcher.
    """

    source_class: str = Field(
        ..., description="URI of the source class",
    )
    target_class: str = Field(
        ..., description="URI of the target class",
    )
    predicate: str = Field(
        SKOS_NARROW_MATCH,
        description="Mapping predicate URI (default: skos:narrowMatch)",
    )
    source_dataset: str = Field(
        ..., description="Dataset name where source_class lives",
    )
    target_dataset: str = Field(
        ..., description="Dataset name where target_class lives",
    )
    source_endpoint: Optional[str] = Field(
        None, description="SPARQL endpoint URL for the source dataset",
    )
    target_endpoint: Optional[str] = Field(
        None, description="SPARQL endpoint URL for the target dataset",
    )
    confidence: Optional[float] = Field(
        None, ge=0, le=1,
        description="Optional match confidence score 0–1",
    )


class InstanceMatchResult(BaseModel):
    """Raw result of probing one URI format against one dataset endpoint."""

    dataset_name: str = Field(..., description="Dataset name")
    endpoint_url: str = Field(..., description="SPARQL endpoint URL")
    uri_format: str = Field(
        ..., description="URI prefix that was probed",
    )
    matched_class: Optional[str] = Field(
        None,
        description=(
            "Class URI returned by the endpoint for this pattern; "
            "None if no match"
        ),
    )


class Mapping(BaseModel):
    """Container for a set of mapping edges with provenance.

    Base class for all mapping types.  Mirrors :class:`MinedSchema`:
    stores edges + ``about`` metadata and serialises to JSON-LD via
    :meth:`to_jsonld`.
    """

    edges: List[MappingEdge] = Field(default_factory=list)
    about: AboutMetadata = Field(...)
    mapping_type: str = Field(
        "unknown", description="Mapping strategy identifier",
    )

    # ---- JSON-LD import -------------------------------------------

    @classmethod
    def from_jsonld(cls, path: "str | Path") -> "Mapping":
        """Reconstruct a :class:`Mapping` from a mapping JSON-LD file.

        Inverse of :meth:`to_jsonld`.  Expands CURIEs using the file's
        own ``@context`` block.  All predicates found in the file are
        accepted — no allowlist is applied.

        Args:
            path: Path to a mapping ``*.jsonld`` file produced by any
                rdfsolve mapping strategy.

        Returns:
            :class:`Mapping` with fully-expanded URIs in all edges.
        """
        import json as _json
        from pathlib import Path as _Path

        # Build a global prefix→namespace lookup from bioregistry once.
        # Replaces the slow get_iri() path (which calls model_dump() per edge).
        _br_prefix_ns: Dict[str, str] = {}
        try:
            from bioregistry import manager as _br_manager
            for _pfx, _res in _br_manager.registry.items():
                _fmt = _res.get_uri_format()
                if _fmt and "$1" in _fmt:
                    _ns = _fmt.replace("$1", "")
                    _br_prefix_ns[_pfx] = _ns
                    # Synonyms cover case variants like CHEBI, NCBITaxon, UniProtKB
                    for _syn in (_res.get_synonyms() or []):
                        _br_prefix_ns.setdefault(_syn, _ns)
        except Exception:
            pass  # bioregistry not available

        raw = _json.loads(_Path(path).read_text(encoding="utf-8"))
        context: Dict[str, str] = raw.get("@context", {})
        about_data = raw.get("@about", {})

        # For SSSOM files the curie_map lives in @about (legacy files written
        # before the curie_map→@context fix).  Merge it so _expand can use it.
        curie_map_fallback: Dict[str, str] = about_data.get("curie_map") or {}
        if curie_map_fallback:
            merged_context: Dict[str, str] = {**curie_map_fallback, **context}
        else:
            merged_context = context

        # Per-file cache: CURIE string → expanded URI.  Most files contain
        # thousands of edges with the same ~dozens of distinct CURIEs, so a
        # dict cache converts O(N * bioregistry_lookup) → O(unique_prefixes).
        _expand_cache: Dict[str, str] = {}

        def _expand(curie: str) -> str:
            cached = _expand_cache.get(curie)
            if cached is not None:
                return cached
            result = curie
            if curie.startswith(("http://", "https://", "urn:")):
                result = curie
            elif ":" in curie:
                prefix, local = curie.split(":", 1)
                # 1. Try explicit context / curie_map first (fastest, exact)
                ns = merged_context.get(prefix)
                if ns and isinstance(ns, str):
                    result = ns + local
                else:
                    # 2. Fast dict lookup from bioregistry prefix map
                    ns2 = _br_prefix_ns.get(prefix)
                    if ns2:
                        result = ns2 + local
            _expand_cache[curie] = result
            return result

        edges: List[MappingEdge] = []
        for node in raw.get("@graph", []):
            source_id = node.get("@id", "")
            if not source_id:
                continue
            src_uri = _expand(source_id)
            src_ds_node = node.get("void:inDataset", {}) or {}
            src_ds = src_ds_node.get("dcterms:title", "") or ""
            src_ep_node = src_ds_node.get("void:sparqlEndpoint") or {}
            src_ep = src_ep_node.get("@id") if isinstance(src_ep_node, dict) else None

            for key, val in node.items():
                if key.startswith("@") or key in (
                    "void:inDataset", "dcterms:created",
                ):
                    continue
                pred_uri = _expand(key)
                targets = val if isinstance(val, list) else [val]
                for tgt in targets:
                    if not isinstance(tgt, dict):
                        continue
                    tgt_id = tgt.get("@id", "")
                    if not tgt_id:
                        continue
                    tgt_uri = _expand(tgt_id)
                    tgt_ds_node = tgt.get("void:inDataset", {}) or {}
                    tgt_ds = tgt_ds_node.get("dcterms:title", "") or src_ds
                    tgt_ep_node = tgt_ds_node.get("void:sparqlEndpoint") or {}
                    tgt_ep = (
                        tgt_ep_node.get("@id")
                        if isinstance(tgt_ep_node, dict)
                        else None
                    )
                    confidence = tgt.get("rdfsolve:confidence")
                    try:
                        edges.append(MappingEdge(
                            source_class=src_uri,
                            target_class=tgt_uri,
                            predicate=pred_uri,
                            source_dataset=src_ds,
                            target_dataset=tgt_ds,
                            source_endpoint=src_ep,
                            target_endpoint=tgt_ep,
                            confidence=float(confidence)
                            if confidence is not None
                            else None,
                        ))
                    except Exception:
                        pass

        about = AboutMetadata.model_validate(about_data)
        strategy = about_data.get("strategy", "unknown")
        return cls(edges=edges, about=about, mapping_type=strategy)

    # ---- NetworkX export ------------------------------------------

    def to_networkx(self) -> "Any":
        """Export the mapping as an ``nx.MultiDiGraph``.

        Nodes are class URIs.  Each :class:`MappingEdge` becomes a
        directed edge with attributes:

        * ``predicate`` — mapping predicate URI
        * ``source_dataset`` — source dataset name
        * ``target_dataset`` — target dataset name
        * ``strategy`` — ``mapping_type``
        * ``confidence`` — float or ``None``

        Node attributes:

        * ``dataset`` — dataset name for this node's side

        Returns:
            ``networkx.MultiDiGraph``
        """
        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for to_networkx(); "
                "install it with: pip install networkx"
            ) from exc

        G: "Any" = _nx.MultiDiGraph()

        for edge in self.edges:
            for uri, ds in (
                (edge.source_class, edge.source_dataset),
                (edge.target_class, edge.target_dataset),
            ):
                if uri not in G:
                    G.add_node(uri, dataset=ds)
            G.add_edge(
                edge.source_class,
                edge.target_class,
                predicate=edge.predicate,
                source_dataset=edge.source_dataset,
                target_dataset=edge.target_dataset,
                strategy=self.mapping_type,
                confidence=edge.confidence,
            )
        return G

    # ---- Dataset-level graph export --------------------------------

    @classmethod
    def dataset_graph(
        cls,
        paths: "Iterable[str | Path]",
        class_to_datasets: "Dict[str, set]",
        *,
        base_graph: "Any | None" = None,
        strategies: "Collection[str] | None" = None,
    ) -> "Any":
        """Stream mapping files into a weighted dataset-pair ``nx.Graph``.

        Algorithm (single pass per file):

        1. Parse the JSON-LD with ``ujson`` (falls back to stdlib ``json``).
        2. Build a per-file CURIE→URI expansion cache using the file's own
           ``@context`` / ``@about.curie_map``, with bioregistry as fallback.
        3. For every mapping edge whose both endpoint classes appear in
           *class_to_datasets*, increment the weight of the
           ``(dataset_a, dataset_b)`` pair in the output graph.
        4. Assemble the ``nx.Graph`` from the accumulated ``Counter`` at the
           end.

        Args:
            paths:
                Iterable of paths to mapping ``*.jsonld`` files.
            class_to_datasets:
                Mapping from class URI (fully expanded) to the set of
                dataset names that contain it as a subject class.  Typically
                built from the :attr:`MinedSchema.patterns` of all loaded
                schemas::

                    from collections import defaultdict
                    c2d = defaultdict(set)
                    for ms in schemas:
                        ds = ms.about.dataset_name or ""
                        for pat in ms.patterns:
                            if pat.subject_class not in ("Literal", "Resource"):
                                c2d[pat.subject_class].add(ds)

            base_graph:
                Optional ``nx.Graph`` to augment in-place (e.g. *G_schema*).
                If *None* a fresh empty ``nx.Graph`` is created.
            strategies:
                Optional allowlist of strategy strings (``@about.strategy``).
                When given, files whose strategy is **not** in this set are
                skipped entirely.  Pass ``None`` (default) to include all
                files.

        Returns:
            A ``networkx.Graph`` (undirected, weighted) whose nodes are
            dataset names and whose edge attribute ``weight`` counts the
            number of distinct class-pair bridges between each dataset pair.

        Raises:
            ImportError: if ``networkx`` is not installed.

        Example::

            from collections import defaultdict
            from rdfsolve.models import MinedSchema, Mapping

            # Build the class→dataset index from schema files
            c2d = defaultdict(set)
            for ms in schemas:
                ds = ms.about.dataset_name or ""
                for pat in ms.patterns:
                    if pat.subject_class not in ("Literal", "Resource"):
                        c2d[pat.subject_class].add(ds)

            # Build G_raw (sssom + semra only)
            G_raw = Mapping.dataset_graph(
                paths=sorted(MAPPINGS_SSSOM.glob("*.jsonld"))
                    + sorted(MAPPINGS_SEMRA.glob("*.jsonld")),
                class_to_datasets=c2d,
                base_graph=G_schema.copy(),
                strategies={"sssom_import", "semra_import", "instance_matcher"},
            )

            # Extend to G_inferred
            G_inferred = Mapping.dataset_graph(
                paths=sorted(MAPPINGS_INF.glob("*.jsonld")),
                class_to_datasets=c2d,
                base_graph=G_raw.copy(),
            )
        """
        from collections import Counter as _Counter
        from pathlib import Path as _Path

        try:
            import networkx as _nx
        except ImportError as exc:
            raise ImportError(
                "networkx is required for dataset_graph(); "
                "install it with: pip install networkx"
            ) from exc

        # ujson is ~3-5× faster than stdlib json for large files
        try:
            import ujson as _json  # type: ignore[import]
        except ImportError:
            import json as _json  # type: ignore[assignment]

        # Build a global prefix→namespace dict from bioregistry **once**.
        # This replaces calling get_iri(pfx, local) per edge — which is
        # catastrophically slow on multi-million-edge files because it calls
        # model_dump() internally.  A plain dict lookup is O(1).
        _br_prefix_ns: Dict[str, str] = {}
        try:
            from bioregistry import manager as _br_manager
            for _pfx, _res in _br_manager.registry.items():
                _fmt = _res.get_uri_format()
                if _fmt and "$1" in _fmt:
                    _ns = _fmt.replace("$1", "")
                    _br_prefix_ns[_pfx] = _ns
                    # Synonyms cover case variants: CHEBI, NCBITaxon, UniProtKB…
                    for _syn in (_res.get_synonyms() or []):
                        _br_prefix_ns.setdefault(_syn, _ns)
        except Exception:
            pass  # bioregistry not available; expansion will be best-effort

        _SKIP_KEYS: frozenset = frozenset({"void:inDataset", "dcterms:created"})

        weights: "_Counter[tuple]" = _Counter()

        for path in paths:
            try:
                raw = _json.loads(_Path(path).read_bytes())
            except Exception:
                continue

            about: Dict[str, Any] = raw.get("@about", {})
            strategy: str = about.get("strategy", "unknown")
            if strategies is not None and strategy not in strategies:
                continue

            context: Dict[str, str] = raw.get("@context", {})
            curie_map: Dict[str, str] = about.get("curie_map") or {}
            merged: Dict[str, str] = {**curie_map, **context}
            _cache: Dict[str, str] = {}

            def _expand(curie: str, _c: Dict = _cache, _m: Dict = merged, _b: Dict = _br_prefix_ns) -> str:  # type: ignore[type-arg]
                v = _c.get(curie)
                if v is not None:
                    return v
                result = curie
                if not curie.startswith(("http://", "https://", "urn:")):
                    if ":" in curie:
                        pfx, local = curie.split(":", 1)
                        ns = _m.get(pfx)
                        if ns and isinstance(ns, str):
                            result = ns + local
                        else:
                            # fast dict lookup — no model_dump() overhead
                            ns2 = _b.get(pfx)
                            if ns2:
                                result = ns2 + local
                _c[curie] = result
                return result

            for node in raw.get("@graph", ()):
                src_id: str = node.get("@id", "")
                if not src_id:
                    continue
                src_cls = _expand(src_id)
                src_datasets = class_to_datasets.get(src_cls)
                if not src_datasets:
                    continue

                for key, val in node.items():
                    if key[0] == "@" or key in _SKIP_KEYS:
                        continue
                    targets = val if isinstance(val, list) else (val,)
                    for tgt in targets:
                        if not isinstance(tgt, dict):
                            continue
                        tgt_id: str = tgt.get("@id", "")
                        if not tgt_id:
                            continue
                        tgt_cls = _expand(tgt_id)
                        tgt_datasets = class_to_datasets.get(tgt_cls)
                        if not tgt_datasets:
                            continue

                        for src_ds in src_datasets:
                            for tgt_ds in tgt_datasets:
                                if src_ds != tgt_ds:
                                    pair = (
                                        (src_ds, tgt_ds)
                                        if src_ds < tgt_ds
                                        else (tgt_ds, src_ds)
                                    )
                                    weights[pair] += 1

        G: "Any" = base_graph if base_graph is not None else _nx.Graph()
        for (a, b), w in weights.items():
            if G.has_edge(a, b):
                G[a][b]["weight"] += w
            else:
                G.add_edge(a, b, weight=w)
        return G

    # ---- JSON-LD export -------------------------------------------

    def to_jsonld(self) -> Dict[str, Any]:
        """Export as JSON-LD with ``@context``, ``@graph``, ``@about``.

        Each edge becomes a node in ``@graph``::

            {
              "@id": "<source_curie>",
              "void:inDataset": {
                "@id": "rdfsolve:dataset/<source_name>",
                "dcterms:title": "<source_name>",
                "foaf:homepage": {"@id": "<homepage_url>"}
              },
              "<predicate_curie>": {
                "@id": "<target_curie>",
                "void:inDataset": {
                  "@id": "rdfsolve:dataset/<target_name>",
                  "dcterms:title": "<target_name>",
                  "foaf:homepage": {"@id": "<homepage_url>"}
                }
              },
              "dcterms:created": "<generated_at>"
            }

        Edges are grouped by source_class so that a class with multiple
        mappings is represented as a single node with a list value for
        the predicate.

        The output is deliberately compatible with the existing frontend
        ``parseJSONLD()`` → ``CanonicalSchema`` pipeline so that mapping
        edges are walkable in the diagram without any frontend changes.
        """
        context: Dict[str, str] = {
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "rdfsolve": "https://w3id.org/rdfsolve/",
            "void": "http://rdfs.org/ns/void#",
            "dcterms": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "sd": "http://www.w3.org/ns/sparql-service-description#",
        }
        grouped: Dict[str, Dict[str, Any]] = {}
        labels: Dict[str, str] = {}
        created_at = self.about.generated_at

        def _dataset_node(name: str, homepage: Optional[str]) -> Dict[str, Any]:
            node: Dict[str, Any] = {
                "@id": f"rdfsolve:dataset/{name}",
                "dcterms:title": name,
            }
            if homepage:
                node["foaf:homepage"] = {"@id": homepage}
            return node

        for edge in self.edges:
            sc, sc_pfx, sc_ns = uri_to_curie(edge.source_class)
            tc, tc_pfx, tc_ns = uri_to_curie(edge.target_class)
            pp, pp_pfx, pp_ns = uri_to_curie(edge.predicate)

            for pfx, ns in (
                (sc_pfx, sc_ns),
                (tc_pfx, tc_ns),
                (pp_pfx, pp_ns),
            ):
                if pfx and ns:
                    context.setdefault(pfx, ns)

            # Build the target object — carries its own dataset node
            target_obj: Dict[str, Any] = {
                "@id": tc,
                "void:inDataset": _dataset_node(
                    edge.target_dataset, edge.target_endpoint
                ),
            }
            if edge.confidence is not None:
                target_obj["rdfsolve:confidence"] = edge.confidence

            if sc not in grouped:
                grouped[sc] = {
                    "@id": sc,
                    "void:inDataset": _dataset_node(
                        edge.source_dataset, edge.source_endpoint
                    ),
                    "dcterms:created": created_at,
                }

            existing = grouped[sc].get(pp)
            if existing is None:
                grouped[sc][pp] = target_obj
            else:
                if not isinstance(existing, list):
                    existing = [existing]
                if target_obj not in existing:
                    existing.append(target_obj)
                grouped[sc][pp] = existing

        result: Dict[str, Any] = {
            "@context": context,
            "@graph": list(grouped.values()),
            "@about": self.about.model_dump(exclude_none=True),
        }
        if labels:
            result["_labels"] = labels
        return result


class InstanceMapping(Mapping):
    """Mapping generated by instance-based matching.

    Probes SPARQL endpoints for instances matching bioregistry URI patterns
    to discover which classes across different datasets represent the same
    kind of entity, then creates mapping edges between them.

    Preferred construction via :meth:`from_bioregistry_resource`.
    """

    mapping_type: str = Field(default="instance_matcher")
    resource_prefix: str = Field(
        ..., description="Bioregistry prefix, e.g. 'ensembl'",
    )
    uri_formats: List[str] = Field(
        default_factory=list,
        description="URI format prefixes that were probed",
    )
    match_results: List[InstanceMatchResult] = Field(
        default_factory=list,
        description="Raw probe results before edge generation",
    )

    def to_jsonld(self) -> Dict[str, Any]:
        """Extend base JSON-LD with instance-matcher provenance."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["resource"] = self.resource_prefix
        about["uri_formats_queried"] = self.uri_formats
        about["strategy"] = "instance_matcher"
        doc["@about"] = about
        return doc

    @classmethod
    def from_bioregistry_resource(
        cls,
        prefix: str,
        datasources: "pd.DataFrame",
        predicate: str = SKOS_NARROW_MATCH,
        dataset_names: Optional[List[str]] = None,
        timeout: float = 60.0,
    ) -> "InstanceMapping":
        """Probe all endpoints for a bioregistry resource.

        Convenience constructor that drives the full probe workflow
        without importing :mod:`rdfsolve.instance_matcher` directly.

        Args:
            prefix: Bioregistry prefix (e.g. ``"ensembl"``).
            datasources: DataFrame with columns
                ``[dataset_name, endpoint_url]``.
            predicate: Mapping predicate URI.
            dataset_names: Optional subset of datasets to query.
            timeout: SPARQL request timeout in seconds.

        Returns:
            :class:`InstanceMapping` ready for :meth:`to_jsonld` export.
        """
        from rdfsolve.instance_matcher import probe_resource

        return probe_resource(
            prefix=prefix,
            datasources=datasources,
            predicate=predicate,
            dataset_names=dataset_names,
            timeout=timeout,
        )


# ---------------------------------------------------------------------------
# SeMRA-derived mapping models
# ---------------------------------------------------------------------------


class SemraMapping(Mapping):
    """Mapping imported from a SeMRA external source.

    Carries the semra source key (e.g. ``"biomappings"``) and, for
    per-prefix sources such as pyobo or Wikidata, the bioregistry prefix.
    The evidence chain is a list of serialisable dicts produced by
    :func:`rdfsolve.semra_converter.semra_evidence_to_jsonld_about`.
    """

    mapping_type: str = Field(default="semra_import")
    source_name: str = Field(
        ..., description="SeMRA source key, e.g. 'biomappings'",
    )
    source_prefix: Optional[str] = Field(
        None,
        description="Bioregistry prefix for per-prefix sources",
    )
    evidence_chain: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Serialised semra evidence objects",
    )

    def to_jsonld(self) -> Dict[str, Any]:
        """Extend base JSON-LD with SeMRA provenance in ``@about``."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["semra_source"] = self.source_name
        if self.source_prefix:
            about["semra_prefix"] = self.source_prefix
        if self.evidence_chain:
            about["evidence"] = self.evidence_chain
        doc["@about"] = about
        return doc


class InferencedMapping(Mapping):
    """Mapping produced by the rdfsolve/SeMRA inference pipeline.

    Carries the set of inference types applied (``"inversion"``,
    ``"transitivity"``, ``"generalisation"``), the source mapping files
    that were combined, an evidence chain for the inferred edges, and
    optional aggregate stats.
    """

    mapping_type: str = Field(default="inferenced")
    inference_types: List[str] = Field(
        default_factory=list,
        description=(
            "Inference operations applied, e.g. "
            "['inversion', 'transitivity']"
        ),
    )
    source_mapping_files: List[str] = Field(
        default_factory=list,
        description="Paths to the input mapping JSON-LD files",
    )
    evidence_chain: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Serialised semra evidence objects for inferred edges",
    )
    stats: Dict[str, Any] = Field(
        default_factory=dict,
        description="Aggregate inference stats (edge counts, etc.)",
    )

    def to_jsonld(self) -> Dict[str, Any]:
        """Extend base JSON-LD with inference provenance in ``@about``."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["inference_types"] = self.inference_types
        about["source_files"] = self.source_mapping_files
        if self.evidence_chain:
            about["evidence"] = self.evidence_chain
        if self.stats:
            about["stats"] = self.stats
        doc["@about"] = about
        return doc


# ---------------------------------------------------------------------------
# SSSOM-derived mapping models
# ---------------------------------------------------------------------------


class SsomMapping(Mapping):
    """Mapping imported from an SSSOM (Simple Standard for Sharing Ontology
    Mappings) source.

    Each instance corresponds to one ``.sssom.tsv`` file extracted from
    an SSSOM bundle (e.g. the EBI OLS SSSOM archive).  The ``source_name``
    records the logical name of the bundle (from ``sssom_sources.yaml``) and
    ``sssom_file`` records the original filename inside the archive.

    The ``mapping_set_id``, ``mapping_set_title``, and ``license`` fields are
    taken directly from the SSSOM file header, when present.
    """

    mapping_type: str = Field(default="sssom_import")
    source_name: str = Field(
        ..., description="Name of the SSSOM source bundle, e.g. 'ols_mappings'",
    )
    sssom_file: str = Field(
        ..., description="Original filename of the .sssom.tsv file",
    )
    mapping_set_id: Optional[str] = Field(
        None,
        description="SSSOM mapping_set_id from the file header (URI)",
    )
    mapping_set_title: Optional[str] = Field(
        None,
        description="SSSOM mapping_set_title from the file header",
    )
    license: Optional[str] = Field(
        None,
        description="License URI from the SSSOM file header",
    )
    curie_map: Dict[str, str] = Field(
        default_factory=dict,
        description="CURIE prefix map extracted from the SSSOM file header",
    )

    def to_jsonld(self) -> Dict[str, Any]:
        """Extend base JSON-LD with SSSOM provenance in ``@about``."""
        doc = super().to_jsonld()
        about = doc.get("@about", {})
        about["strategy"] = self.mapping_type
        about["sssom_source"] = self.source_name
        about["sssom_file"] = self.sssom_file
        if self.mapping_set_id:
            about["mapping_set_id"] = self.mapping_set_id
        if self.mapping_set_title:
            about["mapping_set_title"] = self.mapping_set_title
        if self.license:
            about["license"] = self.license
        if self.curie_map:
            about["curie_map"] = self.curie_map
            # Merge curie_map into @context so that Mapping.from_jsonld()
            # can expand CURIEs back to full URIs on round-trip.
            doc["@context"].update(self.curie_map)
        doc["@about"] = about
        return doc
