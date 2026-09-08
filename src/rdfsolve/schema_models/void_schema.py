"""Published VoID RDF and its supported typed views."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from rdflib import Dataset, Graph, Namespace
from rdflib.namespace import RDF

from rdfsolve.schema_models.void_model import VoidClassPartition, VoidDataset

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.metadata import MetadataDocument

logger = logging.getLogger(__name__)
VOID = Namespace("http://rdfs.org/ns/void#")


@dataclass
class VoidSchema:
    """Keep source RDF intact; convert supported content on request."""

    graph: Graph = field(repr=False)
    endpoint: str
    name: str
    graph_uris: list[str] = field(default_factory=list)
    default_graph: bool = False
    files: dict[str, str] = field(default_factory=dict)
    rdf_dataset: Dataset | None = field(default=None, repr=False)

    @property
    def has_void(self) -> bool:
        """Report VoID vocabulary use, independent of pattern availability."""
        return any(str(predicate).startswith(str(VOID)) for _, predicate, _ in self.graph) or any(
            str(kind).startswith(str(VOID)) for kind in self.graph.objects(None, RDF.type)
        )

    @property
    def has_partitions(self) -> bool:
        """Report declared partitions, including class-count-only partitions."""
        return any(self.graph.triples((None, VOID["class"], None))) or any(
            self.graph.triples((None, VOID.propertyPartition, None))
        )

    @property
    def has_patterns(self) -> bool:
        """Report patterns supported by the canonical reader, not all RDF shapes."""
        from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

        return bool(void_graph_to_minedschema(self.graph).patterns)

    @property
    def datasets(self) -> list[VoidDataset]:
        """Read dataset descriptions without inventing absent partitions."""
        return [
            VoidDataset.from_rdf(self.graph, node)
            for node in sorted(
                set(self.graph.subjects(RDF.type, VOID.Dataset))
                | set(self.graph.subjects(VOID.classPartition, None)),
                key=str,
            )
            if not any(self.graph.subjects(VOID.classPartition, node))
            and not any(self.graph.subjects(VOID.propertyPartition, node))
        ]

    @property
    def class_partitions(self) -> list[VoidClassPartition]:
        """Read class partitions, including those without a dataset root."""
        return [
            VoidClassPartition.from_rdf(self.graph, node)
            for node in sorted(set(self.graph.subjects(VOID["class"], None)), key=str)
        ]

    def for_graph(self, graph_uri: str | None) -> VoidSchema:
        """Select one description context. None selects the default graph."""
        if self.rdf_dataset is None:
            raise ValueError("No dataset contexts were retained")
        if graph_uri is not None and graph_uri not in self.graph_uris:
            raise ValueError(f"No VoID was retrieved from {graph_uri}")
        graph = (
            self.rdf_dataset.default_context
            if graph_uri is None
            else self.rdf_dataset.graph(graph_uri)
        )
        dataset = Dataset()
        target = dataset.default_context if graph_uri is None else dataset.graph(graph_uri)
        target += graph
        return VoidSchema(
            graph + Graph(),
            self.endpoint,
            self.name,
            [graph_uri] if graph_uri else [],
            graph_uri is None,
            rdf_dataset=dataset,
        )

    def get_metadata(self) -> MetadataDocument:
        """Return retained RDF, without a new endpoint request."""
        from rdfsolve.schema_models.metadata import MetadataDocument

        return MetadataDocument(
            graph=self.graph + Graph(),
            endpoint=self.endpoint,
            rdf_dataset=self.rdf_dataset,
            graph_uris=self.graph_uris,
            scope="retained VoID RDF",
        )

    def to_mined_schema(self) -> MinedSchema:
        """Read supported patterns and metadata; do not mine instance data."""
        from rdfsolve.schema_models.metadata import RetainedMetadata
        from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

        schema = void_graph_to_minedschema(self.graph, endpoint=self.endpoint)
        schema.source_metadata = RetainedMetadata.from_document(self.get_metadata())
        schema.about.endpoint = schema.about.endpoint or self.endpoint
        schema.about.dataset_name = schema.about.dataset_name or self.name
        # Description locations are not the instance graphs they describe.
        schema.about.metadata_graph_uris = self.graph_uris or None
        if len(self.graph) and not schema.patterns:
            logger.warning("Published VoID contains no supported class/property patterns")
        return schema

    def to_trig(self) -> str:
        """Export retained graph boundaries. Turtle is only a union view."""
        if self.rdf_dataset is None:
            raise ValueError("This object has no retained dataset contexts")
        return self.rdf_dataset.serialize(format="trig")

    def to_turtle(self) -> str:
        """Return the retrieved RDF, including fields absent from typed views."""
        return self.graph.serialize(format="turtle")
