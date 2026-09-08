"""Mining strategy interface for schema pattern extraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from rdfsolve.mining.report_tracking import ReportCollector
    from rdfsolve.models import SchemaPattern
    from rdfsolve.sparql_helper import SparqlHelper

__all__ = ["MiningContext", "MiningStrategy"]


class MiningContext:
    """Shared context passed to mining strategies."""

    def __init__(
        self,
        helper: SparqlHelper,
        graph_uris: list[str] | None,
        report: ReportCollector,
        collect_bindings: Callable[[str, str, int | None], list[dict[str, Any]]],
        untyped_as_classes: bool = False,
        class_chunk_size: int | None = None,
        class_batch_size: int = 15,
        ontology_classes: list[str] | None = None,
        chunk_size: int = 10_000,
        unsafe_paging: bool = False,
    ) -> None:
        """Initialize mining context.

        Args:
            helper: SPARQL helper for query execution
            graph_uris: List of graph URIs to restrict queries
            report: Report collector for tracking progress
            collect_bindings: Function to execute queries and collect bindings (query, purpose, chunk_size)
            untyped_as_classes: Treat untyped URIs as owl:Class
            class_chunk_size: Chunk size for batched class processing
            class_batch_size: Batch size for class discovery
            ontology_classes: Pre-discovered ontology classes
            chunk_size: Page size for pattern queries
            unsafe_paging: Permit paging without a stable order
        """
        self.helper = helper
        self.graph_uris = graph_uris
        self.report = report
        self.collect_bindings = collect_bindings
        self.untyped_as_classes = untyped_as_classes
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = class_batch_size
        self.ontology_classes = ontology_classes or []
        self.chunk_size = chunk_size
        self.unsafe_paging = unsafe_paging


class MiningStrategy(ABC):
    """Abstract base class for schema mining strategies."""

    @abstractmethod
    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Execute the mining strategy.

        Args:
            context: Mining context with all necessary dependencies

        Returns:
            List of mined schema patterns
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the strategy name for reporting."""
