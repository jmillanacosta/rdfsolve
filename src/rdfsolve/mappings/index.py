"""Entity-to-class index for deriving class mappings from instance evidence."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from rdfsolve.sparql_helper import SparqlHelper

_log = logging.getLogger(__name__)

__all__ = ["ClassIndex", "EntityClassInfo"]


class EntityClassInfo(BaseModel):
    """Class information for a single entity."""

    entity_iri: str = Field(..., description="The entity IRI")
    graph_classes: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Map from graph URI to list of class URIs",
    )

    def all_classes(self) -> set[str]:
        """Get all classes across all graphs."""
        classes: set[str] = set()
        for class_list in self.graph_classes.values():
            classes.update(class_list)
        return classes


class ClassIndex(BaseModel):
    """Index of entities to their RDF classes.

    Built by querying SPARQL endpoints to discover the rdf:type
    of each entity. Used to derive class-level mappings from
    instance-level mapping evidence.

    Example
    -------
    >>> idx = ClassIndex(endpoint_url="https://sparql.example.org/")
    >>> idx.index_entities(["http://example.org/entity/1"])
    >>> info = idx.entities.get("http://example.org/entity/1")
    >>> info.all_classes()
    {'http://example.org/Person'}
    """

    endpoint_url: str = Field(..., description="SPARQL endpoint for class queries")
    entities: dict[str, EntityClassInfo] = Field(
        default_factory=dict,
        description="Map from entity IRI to class info",
    )

    dataset_graphs: dict[str, list[str]] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_classes(self, entity_iri: str, dataset: str | None = None) -> set[str]:
        """Get all classes for an entity, or empty set if not indexed."""
        info = self.entities.get(entity_iri)
        if info is None:
            return set()
        if dataset is None:
            return info.all_classes()
        graphs = self.dataset_graphs.get(dataset, [dataset])
        return {cls for graph in graphs for cls in info.graph_classes.get(graph, [])}

    def index_entities(
        self,
        entity_iris: list[str],
        *,
        graph_uris: list[str] | None = None,
        batch_size: int = 100,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Query endpoint to index classes for entities.

        Parameters
        ----------
        entity_iris
            List of entity IRIs to index.
        graph_uris
            Optional list of graphs to query.
        batch_size
            Number of entities per SPARQL query.
        timeout
            Query timeout in seconds.

        Returns
        -------
        dict
            Statistics about the indexing operation.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        iris = list(dict.fromkeys(entity_iris))
        indexed = not_found = errors = 0
        with SparqlHelper(endpoint_url=self.endpoint_url, timeout=timeout) as helper:
            for i in range(0, len(iris), batch_size):
                batch = iris[i : i + batch_size]
                try:
                    results = self._query_classes_batch(helper, batch, graph_uris)
                    for iri in batch:
                        classes = results.get(iri, {})
                        if classes:
                            self.entities[iri] = EntityClassInfo(
                                entity_iri=iri, graph_classes=classes
                            )
                            indexed += 1
                        else:
                            not_found += 1
                except Exception as exc:
                    _log.warning("Class-index batch %d failed: %s", i, exc)
                    errors += len(batch)
        return {
            "total_entities": len(iris),
            "indexed": indexed,
            "not_found": not_found,
            "errors": errors,
        }

    def _query_classes_batch(self, helper, iris, graph_uris):
        """Retrieve types with explicit graph scope through the shared helper."""
        from rdflib import URIRef

        from rdfsolve.schema_models.paths import absolute_iri

        def term(value):
            return URIRef(absolute_iri(value)).n3()

        if not iris:
            return {}
        pattern = "?entity a ?class ."
        if graph_uris:
            pattern = f"VALUES ?graph {{ {' '.join(term(g) for g in graph_uris)} }} GRAPH ?graph {{ {pattern} }}"
        elif graph_uris is None:
            pattern = f"{{ {pattern} }} UNION {{ GRAPH ?graph {{ {pattern} }} }}"
        query = f"SELECT DISTINCT ?entity ?graph ?class WHERE {{ VALUES ?entity {{ {' '.join(term(i) for i in iris)} }} {pattern} }}"
        result = helper.select_with_fallback(query, exhaustive=True, purpose="class_index")
        if helper.last_select_execution.get("status") != "complete":
            raise ValueError("Class indexing returned incomplete results")
        grouped: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        for row in result.get("results", {}).get("bindings", []):
            grouped[row["entity"]["value"]][row.get("graph", {}).get("value", "")].add(
                row["class"]["value"]
            )
        return {
            iri: {g: sorted(classes) for g, classes in graphs.items()}
            for iri, graphs in grouped.items()
        }

    @classmethod
    def from_endpoint(
        cls,
        endpoint_url: str,
        entity_iris: list[str],
        **kwargs: Any,
    ) -> ClassIndex:
        """Build index for a list of entities from an endpoint."""
        idx = cls(endpoint_url=endpoint_url)
        idx.index_entities(entity_iris, **kwargs)
        return idx
