"""Mined schema model and format entry points."""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS, SERVICE_NAMESPACE_PREFIXES
from rdfsolve.schema_models.about import AboutMetadata
from rdfsolve.schema_models.enrichment import SchemaEnrichment
from rdfsolve.schema_models.exporters.text import trim_descriptions as trim_export_text
from rdfsolve.schema_models.metadata import RetainedMetadata
from rdfsolve.schema_models.navigation import NavigationSummary
from rdfsolve.schema_models.pattern import PatternType, SchemaPattern
from rdfsolve.schema_models.shacl_model import ShaclShapesGraph

if TYPE_CHECKING:
    from rdflib import Graph

    from rdfsolve.exploration import DatasetClient
    from rdfsolve.hydration import Hydrator
    from rdfsolve.schema_models.metadata import MetadataDocument
    from rdfsolve.sparql_helper import SparqlHelper


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
    enrichment: SchemaEnrichment = Field(default_factory=SchemaEnrichment)
    shapes: ShaclShapesGraph | None = Field(
        None, description="Supported source SHACL profile, separate from observed triple patterns"
    )
    about: AboutMetadata = Field(
        ...,
        description="Provenance metadata",
    )

    source_metadata: RetainedMetadata | None = Field(
        None, description="Original RDF evidence, separate from projected schema fields"
    )
    navigation: NavigationSummary | None = None

    def discover_paths(
        self, *, max_hops: int = 3, max_paths_per_length: int = 100
    ) -> NavigationSummary:
        """Compose candidate routes locally; do not verify instance joins."""
        from rdfsolve.navigation import discover_paths

        self.navigation = discover_paths(
            self, max_hops=max_hops, max_paths_per_length=max_paths_per_length
        )
        return self.navigation

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
        if self.shapes is not None:
            classes.update(
                shape.target_class for shape in self.shapes.node_shapes if shape.target_class
            )
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
    def from_dict(cls, raw: dict[str, Any] | list[dict[str, Any]]) -> MinedSchema:
        """Read canonical JSON or VoID JSON-LD.

        Only canonical JSON preserves all model fields. RDF imports use
        their adapter's supported fields. External contexts are rejected.
        """
        from rdfsolve.schema_models.readers.json import read_schema

        return read_schema(raw)

    def to_dict(self, *, trim_descriptions: int | None = None) -> dict[str, Any]:
        """Return canonical JSON. Optional text trimming is lossy."""
        return {
            "format": "rdfsolve.mined-schema",
            "version": 1,
            "schema": trim_export_text(self, trim_descriptions).model_dump(mode="json"),
        }

    @classmethod
    def from_json(cls, path: str | Path) -> MinedSchema:
        """Read a saved schema through format detection and validation."""
        return cls.from_dict(_json.loads(Path(path).read_text(encoding="utf-8")))

    @classmethod
    def from_jsonld(cls, path: str | Path) -> MinedSchema:
        """Reconstruct from a ``*_schema.jsonld`` file.

        Convenience wrapper around :meth:`from_dict` that reads and
        parses the file first.
        """
        return cls.from_json(path)

    def get_metadata(self) -> MetadataDocument:
        """Return retained source RDF, or generated metadata when none was retained."""
        from rdfsolve.schema_models.metadata import MetadataDocument

        if self.source_metadata is not None:
            return self.source_metadata.to_document()
        return MetadataDocument(
            graph=self.to_void_graph(),
            endpoint=self.about.endpoint,
            scope="rdfsolve export of stored schema fields",
        )

    @classmethod
    def from_void_source(cls, endpoint: str, name: str, **kwargs: Any) -> MinedSchema:
        """Retrieve published VoID and return its canonical schema.

        This does not fill missing statistics by querying instance data.
        Pass discovery options such as graph_uris and get_graphs_from_store.
        """
        from rdfsolve.api import discover_void_source

        return discover_void_source(endpoint, name, **kwargs).to_mined_schema()

    @classmethod
    def from_void(cls, void_ttl: str) -> MinedSchema:
        """Parse VoID Turtle into MinedSchema.

        Args:
            void_ttl: VoID document in Turtle format

        Returns:
            MinedSchema with patterns reconstructed from VoID

        Example:
            >>> void_ttl = Path("dataset_void.ttl").read_text()
            >>> schema = MinedSchema.from_void(void_ttl)
            >>> print(len(schema.patterns))
            435
        """
        from rdfsolve.schema_models.readers.void import void_to_minedschema

        return void_to_minedschema(void_ttl)

    @classmethod
    def from_shacl(cls, shacl_ttl: str) -> MinedSchema:
        """Parse SHACL Turtle into MinedSchema.

        Args:
            shacl_ttl: SHACL shapes in Turtle format

        Returns:
            MinedSchema with patterns reconstructed from shapes

        Example:
            >>> shacl_ttl = Path("shapes.ttl").read_text()
            >>> schema = MinedSchema.from_shacl(shacl_ttl)
            >>> print(len(schema.patterns))
            10
        """
        from rdfsolve.schema_models.readers.shacl import shacl_to_minedschema

        return shacl_to_minedschema(shacl_ttl)

    # NetworkX export

    def to_networkx(self, *, trim_descriptions: int | None = None) -> Any:
        """Export typed class relationships."""
        from rdfsolve.schema_models.exporters.networkx import to_networkx

        return to_networkx(trim_export_text(self, trim_descriptions))

    def to_jsonld(self, *, trim_descriptions: int | None = None) -> dict[str, Any]:
        """Export schema as JSON-LD by serializing the VoID graph.

        This is an RDF export, not the internal storage format. VoID
        does not preserve every model field. Use :meth:`to_dict` for
        lossless storage and :meth:`from_dict` to read either profile.

        Returns a JSON-LD document with:
        - void:Dataset for the schema metadata
        - void:propertyPartition for each pattern
        - void:triples for counts
        - rdfs:label for labels
        """
        import json

        # Get the VoID graph (proper semantic RDF)
        void_graph = self.to_void_graph(trim_descriptions=trim_descriptions)

        # Serialize as JSON-LD
        jsonld_str = void_graph.serialize(format="json-ld", auto_compact=True)
        result: dict[str, Any] = json.loads(jsonld_str)
        return result

    # VoID graph export

    def to_void_graph(
        self, base_url: str | None = None, *, trim_descriptions: int | None = None
    ) -> Graph:
        """Export the supported VoID fields."""
        from rdfsolve.schema_models.exporters.void import to_void_graph

        if self.shapes is not None or self.navigation is not None:
            import logging

            logging.getLogger(__name__).warning(
                "VoID does not encode SHACL profiles or composed navigation. Keep canonical JSON."
            )
        return to_void_graph(
            trim_export_text(self, trim_descriptions), base_url, trim_descriptions=trim_descriptions
        )

    def to_linkml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
        *,
        trim_descriptions: int | None = None,
    ) -> Any:
        """Convert to LinkML SchemaDefinition with full metadata.

        Returns LinkML SchemaDefinition object.
        """
        from rdfsolve.schema_models.exporters.linkml import to_linkml
        from rdfsolve.schema_models.exporters.text import clip_description

        return to_linkml(
            trim_export_text(self, trim_descriptions),
            schema_name=schema_name or self.about.dataset_name,
            schema_description=clip_description(schema_description, trim_descriptions),
        )

    def to_linkml_yaml(
        self,
        schema_name: str | None = None,
        schema_description: str | None = None,
        *,
        trim_descriptions: int | None = None,
    ) -> str:
        """Convert to LinkML YAML with full metadata.

        Returns YAML string.
        """
        from typing import cast

        from linkml.generators.yamlgen import YAMLGenerator

        linkml_schema = self.to_linkml(
            schema_name, schema_description, trim_descriptions=trim_descriptions
        )
        return cast(str, YAMLGenerator(linkml_schema).serialize())

    def to_rdfconfig(
        self,
        *,
        endpoint_url: str | None = None,
        endpoint_name: str | None = None,
        graph_uri: str | None = None,
        trim_descriptions: int | None = None,
    ) -> dict[str, str]:
        """Export RDF-config from canonical patterns and source examples."""
        from rdfsolve.schema_models.exporters.rdfconfig import to_rdfconfig

        return to_rdfconfig(
            trim_export_text(self, trim_descriptions),
            endpoint_url=endpoint_url,
            endpoint_name=endpoint_name,
            graph_uri=graph_uri,
        )

    def to_pydantic_classes(self) -> dict[str, type[BaseModel]]:
        """Generate runtime classes using the same definitions as the Python export."""
        from rdfsolve.schema_models.exporters.pydantic import build_pydantic_classes

        return build_pydantic_classes(self)

    def client(
        self, source: str | SparqlHelper | Graph | None = None, **kwargs: Any
    ) -> DatasetClient:
        """Explore generated models by name and follow their recorded links."""
        from rdfsolve.exploration import DatasetClient

        return DatasetClient(self, source, **kwargs)

    def hydrator(self, source: str | SparqlHelper | Graph | None = None, **kwargs: Any) -> Hydrator:
        """Read generated model fields from a source. See Hydrator for request budgets."""
        from rdfsolve.hydration import Hydrator

        return Hydrator(self, source, **kwargs)

    def to_pydantic(
        self, schema_name: str | None = None, *, trim_descriptions: int | None = None
    ) -> str:
        """Generate label-named Pydantic views of observed RDF patterns."""
        from rdfsolve.schema_models.exporters.pydantic import to_pydantic

        return to_pydantic(
            trim_export_text(self, trim_descriptions),
            schema_name,
            trim_descriptions=trim_descriptions,
        )

    def annotate_rdf(
        self, graph: Graph, *, include_examples: bool = True, trim_descriptions: int | None = None
    ) -> None:
        """Attach source annotations and provenance."""
        from rdfsolve.schema_models.exporters.rdf import annotate_rdf

        annotate_rdf(
            trim_export_text(self, trim_descriptions), graph, include_examples=include_examples
        )

    def to_shacl(
        self,
        base_uri: str = "http://example.org/shapes/",
        *,
        activate_observed: bool = False,
        trim_descriptions: int | None = None,
    ) -> str:
        """Convert to SHACL shapes.

        Returns SHACL Turtle string.

        Args:
            base_uri: Base URI for shape URIs
            activate_observed: Enforce generated one-hop templates; source profiles stay unchanged.

        Example:
            >>> schema = MinedSchema.from_jsonld("schema.jsonld")
            >>> shacl_ttl = schema.to_shacl()
            >>> print(shacl_ttl[:100])
            @prefix sh: <http://www.w3.org/ns/shacl#> .
        """
        from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

        schema = trim_export_text(self, trim_descriptions)
        shapes = minedschema_to_shacl(
            schema, base_uri=base_uri, activate_observed=activate_observed
        )
        graph = trim_export_text(shapes, trim_descriptions).to_rdf()
        # VoID statistics remain dataset metadata, not validation constraints.
        from rdfsolve.schema_models.exporters.void import to_void_graph

        graph += to_void_graph(schema, trim_descriptions=trim_descriptions)
        schema.annotate_rdf(graph)
        result: str = graph.serialize(format="turtle")
        return result


# MiningResult


class MiningResult(BaseModel):
    """Complete mining output with data patterns, ontology, and metadata."""

    data_schema: MinedSchema
    """VoID partitions for instance data patterns (ABox)."""

    ontology: Any | None = None
    """Class hierarchies and property metadata (TBox)."""

    metadata: Any | None = None
    """Infrastructure descriptions (DCAT/VoID)."""

    def export(self, output_dir: Path) -> None:
        """Export three separate files."""
        output_dir.mkdir(parents=True, exist_ok=True)

        (output_dir / "schema.ttl").write_text(
            self.data_schema.to_void_graph().serialize(format="turtle")
        )

        if self.ontology is not None:
            (output_dir / "ontology.ttl").write_text(self.ontology.to_turtle())

        if self.metadata is not None:
            (output_dir / "metadata.ttl").write_text(self.metadata.to_turtle())
