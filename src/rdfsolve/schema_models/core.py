"""Core schema models: SchemaPattern, AboutMetadata, MinedSchema.

These are the primary data structures for mined RDF schemas.
"""

from __future__ import annotations

import json as _json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from hashlib import md5
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

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


# -------------------------------------------------------------------
# SchemaPattern
# -------------------------------------------------------------------


class SchemaPattern(BaseModel):
    """A single schema pattern: subject_class -> property -> object.

    Captures three kinds of relationships:

    - **typed-object**:
      ``?s a ?sc . ?s ?p ?o . ?o a ?oc``
    - **literal**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isLiteral(?o))``
    - **untyped-uri**:
      ``?s a ?sc . ?s ?p ?o . FILTER(isURI(?o))``

    This model is shared contract between SchemaMiner (direct SPARQL)
    and VoidParser (VoID-based extraction).
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
        description=("URI of the object class, or the special sentinel 'Literal' / 'Resource'"),
    )
    count: int | None = Field(
        None,
        ge=0,
        description="Number of triples matching this pattern",
    )
    datatype: str | None = Field(
        None,
        description=("XSD datatype URI for literal objects (only when object_class == 'Literal')"),
    )
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


# -------------------------------------------------------------------
# AboutMetadata
# -------------------------------------------------------------------


class AboutMetadata(BaseModel):
    """Provenance metadata attached to every schema export."""

    generated_by: str = Field(
        ...,
        description="Tool and version string",
    )
    generated_at: str = Field(
        ...,
        description="ISO-8601 timestamp (UTC)",
    )
    endpoint: str | None = Field(
        None,
        description="SPARQL endpoint URL",
    )
    dataset_name: str | None = Field(
        None,
        description="Human-readable dataset name",
    )
    graph_uris: list[str] | None = Field(
        None,
        description="Named graph URIs queried",
    )
    pattern_count: int = Field(
        0,
        ge=0,
        description="Number of schema patterns",
    )
    strategy: str = Field(
        "unknown",
        description=("Mining strategy used (e.g. 'miner', 'void')"),
    )

    # Versions
    rdfsolve_version: str | None = Field(
        None,
        description="rdfsolve version string",
    )
    qlever_version: dict[str, str] | None = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's "
            '?cmd=stats: {"git_hash_server": str, '
            '"git_hash_index": str}'
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

    # Provenance
    authors: list[dict[str, str]] | None = Field(
        None,
        description='List of {"name": str, "orcid": str} dicts',
    )

    # Canonical URIs (auto-populated from dataset_name)
    schema_uri: str | None = Field(
        None,
        description=("Canonical URI where this schema is served"),
    )
    void_uri: str | None = Field(
        None,
        description=("Canonical URI where the VoID catalog is served"),
    )
    report_uri: str | None = Field(
        None,
        description=("Canonical URI where the run report is served"),
    )
    linkml_uri: str | None = Field(
        None,
        description=("Canonical URI where the LinkML schema is served"),
    )

    model_config = ConfigDict(extra="allow")

    @staticmethod
    def build(
        endpoint: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
        pattern_count: int = 0,
        strategy: str = "unknown",
        started_at: str | None = None,
        finished_at: str | None = None,
        total_duration_s: float | None = None,
        authors: list[dict[str, str]] | None = None,
        qlever_version: dict[str, str] | None = None,
    ) -> AboutMetadata:
        """Create metadata with auto-populated version + timestamp."""
        from rdfsolve.version import VERSION

        def _uri(suffix: str) -> str | None:
            return f"{_BASE_URI}/api/{suffix}/{dataset_name}" if dataset_name else None

        return AboutMetadata(
            generated_by=f"rdfsolve {VERSION}",
            generated_at=datetime.now(
                timezone.utc,
            ).isoformat(),
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
            schema_uri=_uri("schemas"),
            void_uri=_uri("void"),
            report_uri=_uri("reports"),
            linkml_uri=_uri("linkml"),
        )


# -------------------------------------------------------------------
# JSON-LD helpers
# -------------------------------------------------------------------


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
    """Return the JSON-LD object value dict and count-map key."""
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

    oc, oc_pfx, oc_ns = uri_to_curie(pat.object_class)
    if oc_pfx and oc_ns:
        context[oc_pfx] = oc_ns
    if pat.object_label:
        labels[oc] = pat.object_label
    return {"@id": oc}, oc


# -------------------------------------------------------------------
# MinedSchema
# -------------------------------------------------------------------


class MinedSchema(BaseModel):
    """Complete mined schema: patterns + provenance.

    Primary export format is JSON-LD.  Can also be converted to a
    VoID RDF graph for downstream conversion to LinkML / SHACL /
    RDF-config via VoidParser.
    """

    patterns: list[SchemaPattern] = Field(
        default_factory=list,
        description="Schema patterns",
    )
    about: AboutMetadata = Field(
        ...,
        description="Provenance metadata",
    )

    # ---- Service-namespace filtering -----------------------

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

    # ---- Queries -------------------------------------------

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

    # ---- JSON-LD import ------------------------------------

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

    # ---- NetworkX export -----------------------------------

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

    # ---- JSON-LD export ------------------------------------

    def to_jsonld(self) -> dict[str, Any]:
        """Export schema as JSON-LD with @context, @graph, @about.

        The @graph groups triples by subject class.  Labels are
        exported in a top-level ``_labels`` map keyed by CURIE.
        """
        context: dict[str, str] = {}
        grouped: dict[str, dict[str, Any]] = {}
        counts: dict[str, dict[str, dict[str, int]]] = {}
        labels: dict[str, str] = {}

        for pat in self.patterns:
            sc, sc_pfx, sc_ns = uri_to_curie(
                pat.subject_class,
            )
            pp, pp_pfx, pp_ns = uri_to_curie(
                pat.property_uri,
            )
            for pfx, ns in (
                (sc_pfx, sc_ns),
                (pp_pfx, pp_ns),
            ):
                if pfx and ns:
                    context[pfx] = ns
            if pat.subject_label:
                labels[sc] = pat.subject_label
            if pat.property_label:
                labels[pp] = pat.property_label

            o_val, o_key = _object_value_and_key(
                pat,
                context,
                labels,
            )

            if pat.count is not None:
                counts.setdefault(sc, {}).setdefault(
                    pp,
                    {},
                )[o_key] = pat.count

            _merge_into_list(grouped, sc, pp, o_val)

        for sc_curie, cmap in counts.items():
            if sc_curie in grouped:
                grouped[sc_curie]["_counts"] = cmap

        result: dict[str, Any] = {
            "@context": context,
            "@graph": list(grouped.values()),
            "@about": self.about.model_dump(
                exclude_none=True,
            ),
        }
        if labels:
            result["_labels"] = labels
        return result

    # ---- VoID graph export ---------------------------------

    def to_void_graph(self) -> Any:
        """Build an rdflib VoID Graph from the mined patterns.

        Allows feeding the result into VoidParser for downstream
        conversion to LinkML, SHACL, RDF-config, etc.
        """
        from rdflib import Graph, Namespace, URIRef
        from rdflib import Literal as RdfLiteral
        from rdflib.namespace import RDF, RDFS, XSD

        void = Namespace("http://rdfs.org/ns/void#")
        void_ext = Namespace("http://ldf.fi/void-ext#")

        g = Graph()
        for pfx, ns in (
            ("void", void),
            ("void-ext", void_ext),
            ("rdf", RDF),
            ("rdfs", RDFS),
            ("xsd", XSD),
        ):
            g.bind(pfx, ns)

        endpoint = self.about.endpoint or "urn:rdfsolve"
        base = endpoint.rstrip("/") + "/void/"

        def _pid(s: str, p: str, o: str) -> URIRef:
            h = md5(
                f"{s}|{p}|{o}".encode(),
                usedforsecurity=False,
            ).hexdigest()[:12]
            return URIRef(f"{base}pp_{h}")

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


# -------------------------------------------------------------------
# VoID graph helpers
# -------------------------------------------------------------------


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


# -------------------------------------------------------------------
# JSON-LD @graph parsers
# -------------------------------------------------------------------


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
