"""Mining strategy interface for schema pattern extraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from rdfsolve.mining.report_tracking import ReportCollector
    from rdfsolve.models import SchemaPattern
    from rdfsolve.schema_models.structural import StructuralPattern
    from rdfsolve.sparql_helper import SparqlHelper

__all__ = ["ClassListingLimitError", "MiningContext", "MiningStrategy"]


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
        excluded_graph_prefixes: tuple[str, ...] = (),
        type_context_graph_uris: list[str] | None = None,
        ontology_graph_uris: list[str] | None = None,
        ontology_term_budget: int | None = None,
        group_before_mining: int | None = None,
        ontology_hierarchy_files: list[str] | None = None,
        pagination: str = "offset",
    ) -> None:
        """Initialize mining context.

        Args:
            helper: SPARQL helper for query execution
            graph_uris: Data graphs
            type_context_graph_uris: Extra graphs for subject and object types
            ontology_graph_uris: Interpretation graphs excluded from data discovery
            report: Report collector for tracking progress
            collect_bindings: Function to execute queries and collect bindings (query, purpose, chunk_size)
            untyped_as_classes: Treat untyped URIs as owl:Class
            class_chunk_size: Chunk size for batched class processing
            class_batch_size: Batch size for class discovery
            ontology_classes: Pre-discovered ontology classes
            chunk_size: Page size for pattern queries
            unsafe_paging: Permit paging without a stable order
            excluded_graph_prefixes: Graph IRI prefixes to skip when discovering graphs
            ontology_term_budget: Target class count when ontology terms are grouped
            group_before_mining: Group terms before per-class mining above this class count
            ontology_hierarchy_files: Files of (child, parent) pairs for terms whose
                hierarchy is not in the data
            pagination: How collect_bindings pages: "offset" or "cursor" (keyset)
        """
        self.helper = helper
        self.graph_uris = graph_uris
        self.type_context_graph_uris = type_context_graph_uris
        self.ontology_graph_uris = ontology_graph_uris
        self.report = report
        self.collect_bindings = collect_bindings
        self.untyped_as_classes = untyped_as_classes
        self.class_chunk_size = class_chunk_size
        self.class_batch_size = class_batch_size
        self.ontology_classes = ontology_classes or []
        self.chunk_size = chunk_size
        self.unsafe_paging = unsafe_paging
        self.excluded_graph_prefixes = excluded_graph_prefixes
        self.ontology_term_budget = ontology_term_budget
        self.group_before_mining = group_before_mining
        self.ontology_hierarchy_files = ontology_hierarchy_files or []
        self.pagination = pagination
        # Classes that typed mining discovered, and type values that it skipped (not IRIs).
        self.discovered_classes: list[str] | None = None
        self.skipped_type_values = 0
        # Representative -> member terms, when terms were grouped before mining.
        self.grouped_members: dict[str, list[str]] = {}
        # Class batches chosen by the strategy; the counts phase reuses them.
        self.class_batches: list[list[str]] | None = None
        self.structural_patterns: list[StructuralPattern] = []
        # Completed class batches reused from an earlier run's checkpoint.
        self.resumed: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        # Typed-subject populations from batch planning, and classes whose member
        # sets were verified identical to an already mined class (copy -> source).
        self.class_weights: dict[str, int] = {}
        self.shared_extensions: dict[str, str] = {}
        # Most type values that class discovery lists through an endpoint (None: no limit);
        # see ClassListingLimitError.
        self.class_listing_limit: int | None = None
        self.class_listing_stopped = False


class ClassListingLimitError(Exception):
    """Class discovery listed more type values than the run's limit, and stopped listing.

    A source whose records are classes, or whose individuals are typed by per-record IRIs,
    has millions of type values (BioGateway: 10.8 M, about one instance each; GO-CAM: 1.7 M
    gene products). Listing them all took hours (BioGateway, job 115328: 1,084 pages of about
    5 s and still listing), and per-class mining of such a list is not feasible remotely.
    """

    def __init__(self, rows: list[dict[str, Any]], limit: int) -> None:
        """Keep the rows listed so far and the limit."""
        super().__init__(f"more than {limit} type values listed")
        self.rows = rows
        self.limit = limit


class MiningStrategy(ABC):
    """Abstract base class for schema mining strategies."""

    # Classes come from this property instead of rdf:type (e.g. Wikibase "instance of").
    # Phases that read rdf:type (structural coverage, counts) do not apply then.
    membership_property: str | None = None
    # The rows describe a part of the data that the strategy chose, with its own labels and
    # examples. Phases that read all the data (structural coverage, counts, enrichment) do not
    # apply then.
    scoped: bool = False

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
