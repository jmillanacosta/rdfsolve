"""Entity-to-class index for deriving class mappings from instance evidence."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from pydantic import BaseModel, Field

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
    >>> await idx.index_entities(["http://example.org/entity/1"])
    >>> info = idx.entities.get("http://example.org/entity/1")
    >>> info.all_classes()
    {'http://example.org/Person'}
    """

    endpoint_url: str = Field(..., description="SPARQL endpoint for class queries")
    entities: dict[str, EntityClassInfo] = Field(
        default_factory=dict,
        description="Map from entity IRI to class info",
    )

    class Config:
        """Pydantic config."""

        arbitrary_types_allowed = True

    def get_classes(self, entity_iri: str) -> set[str]:
        """Get all classes for an entity, or empty set if not indexed."""
        info = self.entities.get(entity_iri)
        return info.all_classes() if info else set()

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
        helper = SparqlHelper(
            endpoint_url=self.endpoint_url,
            timeout=timeout,
        )

        indexed = 0
        not_found = 0
        errors = 0

        for i in range(0, len(entity_iris), batch_size):
            batch = entity_iris[i : i + batch_size]
            try:
                results = self._query_classes_batch(helper, batch, graph_uris)
                for iri, graph_classes in results.items():
                    if graph_classes:
                        self.entities[iri] = EntityClassInfo(
                            entity_iri=iri,
                            graph_classes=graph_classes,
                        )
                        indexed += 1
                    else:
                        not_found += 1
            except Exception as e:
                _log.warning("Error indexing batch %d: %s", i, e)
                errors += len(batch)

        return {
            "total_entities": len(entity_iris),
            "indexed": indexed,
            "not_found": not_found,
            "errors": errors,
        }

    def _query_classes_batch(
        self,
        helper: SparqlHelper,
        iris: list[str],
        graph_uris: list[str] | None,
    ) -> dict[str, dict[str, list[str]]]:
        """Query classes for a batch of IRIs."""
        if not iris:
            return {}

        # Build VALUES clause
        values = " ".join(f"<{iri}>" for iri in iris)

        # Build query with optional graph clause
        if graph_uris:
            graph_clause = " ".join(f"FROM <{g}>" for g in graph_uris)
            query = f"""
            SELECT ?entity ?graph ?class
            {graph_clause}
            WHERE {{
                VALUES ?entity {{ {values} }}
                GRAPH ?graph {{ ?entity a ?class }}
            }}
            """
        else:
            query = f"""
            SELECT ?entity ?graph ?class
            WHERE {{
                VALUES ?entity {{ {values} }}
                GRAPH ?graph {{ ?entity a ?class }}
            }}
            """

        try:
            result = helper.select(query, purpose="class_index")
            bindings = result.get("results", {}).get("bindings", [])
        except Exception as e:
            _log.debug("Query failed: %s", e)
            return {}

        # Group results by entity and graph
        entity_graphs: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        for row in bindings:
            entity = row.get("entity", {}).get("value", "")
            graph = row.get("graph", {}).get("value", "")
            cls = row.get("class", {}).get("value", "")
            if entity and cls:
                entity_graphs[entity][graph].append(cls)

        return {k: dict(v) for k, v in entity_graphs.items()}

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
