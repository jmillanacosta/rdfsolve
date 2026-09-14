"""Internal query solver that advances until a semantic decision point.

The solver handles routine query-planning work internally while preserving
query correctness. The model receives only decision-relevant observations.

Milestone C adds binding-aware query fragments:
- Explicit binding identities for same-class roles
- Composable fragments (join, union, filter)
- Shape-derived planning constraints
"""

from __future__ import annotations

import hashlib
import json
import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import BaseModel, Field

from rdfsolve.client_routes import Route

if TYPE_CHECKING:
    from rdfsolve.client_api import Client
    from rdfsolve.registry import Registry, TypeDescription

logger = logging.getLogger(__name__)


class SolverState(str, Enum):
    """States a query solver can be in."""

    INIT = "init"  # Not started
    INTERPRETING = "interpreting"  # Waiting for intent
    CHOOSE = "choose"  # Waiting for model to choose between options
    READY = "ready"  # Query compiled and ready to execute
    COMPLETE = "complete"  # Query executed with results
    BLOCKED = "blocked"  # Cannot progress without intervention
    FAILED = "failed"  # Unrecoverable error


class RequirementStatus(str, Enum):
    """Resolution status for a requirement."""

    PENDING = "pending"  # Not yet resolved
    RESOLVED = "resolved"  # Bound to schema/data
    UNSUPPORTED = "unsupported"  # Cannot be expressed in query
    PARTIAL = "partial"  # Partially resolved


@dataclass
class Requirement:
    """A single requirement extracted from the question."""

    id: str
    clause: str  # Original or normalized text
    kind: str  # filter, target, source, projection, etc.
    status: RequirementStatus = RequirementStatus.PENDING
    bindings: dict[str, Any] = field(default_factory=dict)
    reason: str | None = None  # For unsupported/partial


@dataclass
class DecisionOption:
    """One option for a semantic decision."""

    id: str
    meaning: str
    route_id: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0


@dataclass
class Decision:
    """A point where the model must choose between alternatives."""

    id: str
    clause: str  # What we're deciding
    kind: str  # route_choice, entity_resolution, filter_interpretation
    options: list[DecisionOption]
    selected: str | None = None
    more_available: bool = False


class Intent(BaseModel):
    """Normalized interpretation of the question."""

    question: str = ""
    source_class: str | None = None
    target_classes: list[str] = Field(default_factory=list)
    filters: list[dict[str, Any]] = Field(default_factory=list)
    projections: list[str] = Field(default_factory=list)
    selection: str = ""
    via_classes: list[str] = Field(default_factory=list)


class CompactObservation(BaseModel):
    """Minimal observation returned to the model."""

    plan_id: str
    revision: int
    state: SolverState
    decision: Decision | None = None
    reference: str | None = None
    execution: str | None = None
    rows: int | None = None
    meaning: str | None = None
    unresolved: list[str] = Field(default_factory=list)


# Milestone C: Binding-aware query fragments


class FragmentKind(str, Enum):
    """Type of query fragment."""

    NODE = "node"  # Single class binding
    EDGE = "edge"  # Predicate connecting two nodes
    JOIN = "join"  # Sequential composition
    UNION = "union"  # Alternative paths
    FILTER = "filter"  # Value constraint


@dataclass
class Binding:
    """Explicit binding identity for a class occurrence.

    When the same class appears multiple times in a query (e.g., source
    and target are both ChemicalCompound), each occurrence gets a distinct
    binding with a unique variable and role.
    """

    variable: str  # SPARQL variable name (e.g., "source", "via1", "target")
    class_iri: str  # The RDF class IRI
    role: str  # Semantic role: "source", "target", "via", "filter_anchor"
    occurrence: int = 1  # Which occurrence of this class (1-indexed)
    label: str | None = None  # Human-readable class label


@dataclass
class QueryFragment:
    """Composable query building block with explicit bindings.

    Fragments know their bindings, making it clear when same-class nodes
    represent different entities in the query.
    """

    kind: FragmentKind
    bindings: list[Binding] = field(default_factory=list)
    children: list[QueryFragment] = field(default_factory=list)
    predicate: str | None = None  # For EDGE fragments
    inverse: bool = False  # For EDGE fragments
    condition: dict[str, Any] | None = None  # For FILTER fragments
    route_id: str | None = None  # Source route ID if derived from route

    def variables(self) -> set[str]:
        """Return all SPARQL variables used in this fragment."""
        result = {b.variable for b in self.bindings}
        for child in self.children:
            result.update(child.variables())
        return result

    def classes(self) -> set[str]:
        """Return all class IRIs referenced in this fragment."""
        result = {b.class_iri for b in self.bindings}
        for child in self.children:
            result.update(child.classes())
        return result

    def binding_for_class(self, class_iri: str, occurrence: int = 1) -> Binding | None:
        """Find binding for a specific class occurrence."""
        count = 0
        for binding in self.bindings:
            if binding.class_iri == class_iri:
                count += 1
                if count == occurrence:
                    return binding
        for child in self.children:
            found = child.binding_for_class(class_iri, occurrence)
            if found:
                return found
        return None


@dataclass
class BindingContext:
    """Tracks binding allocations across query composition.

    Ensures unique variable names and tracks class occurrences.
    """

    _counter: int = 0
    _class_counts: dict[str, int] = field(default_factory=dict)
    _bindings: dict[str, Binding] = field(default_factory=dict)

    def allocate(self, class_iri: str, role: str, label: str | None = None) -> Binding:
        """Allocate a new binding for a class."""
        occurrence = self._class_counts.get(class_iri, 0) + 1
        self._class_counts[class_iri] = occurrence

        # Generate variable name based on role and occurrence
        if role == "source":
            variable = "source"
        elif role == "target":
            variable = "target" if occurrence == 1 else f"target{occurrence}"
        elif role == "via":
            variable = f"via{self._counter}"
            self._counter += 1
        else:
            variable = f"n{self._counter}"
            self._counter += 1

        binding = Binding(
            variable=variable,
            class_iri=class_iri,
            role=role,
            occurrence=occurrence,
            label=label,
        )
        self._bindings[variable] = binding
        return binding

    def get(self, variable: str) -> Binding | None:
        """Look up binding by variable name."""
        return self._bindings.get(variable)

    def occurrences(self, class_iri: str) -> int:
        """Count how many times a class has been bound."""
        return self._class_counts.get(class_iri, 0)

    def all_bindings(self) -> list[Binding]:
        """Return all allocated bindings."""
        return list(self._bindings.values())


@dataclass
class ShapeConstraint:
    """Constraint derived from schema shape.

    Used to inform planning decisions based on schema structure.
    """

    class_iri: str
    field_name: str
    predicate_iri: str
    required: bool = False
    targets: list[str] = field(default_factory=list)  # Target class IRIs
    cardinality: str = "many"  # "one", "many", "optional"


@dataclass
class PlanningFragment:
    """High-level planning structure derived from shapes.

    Represents what can be queried for a class, not the query itself.
    """

    class_iri: str
    class_label: str
    outgoing: list[ShapeConstraint] = field(default_factory=list)
    incoming: list[ShapeConstraint] = field(default_factory=list)

    def reachable_classes(self) -> set[str]:
        """Return classes reachable in one hop."""
        result: set[str] = set()
        for constraint in self.outgoing:
            result.update(constraint.targets)
        for constraint in self.incoming:
            result.add(constraint.class_iri)
        return result


class QuerySolver:
    """Coordinates query planning with bounded internal advancement.

    The solver performs deterministic work internally and pauses at
    semantic decision points for model input.
    """

    def __init__(
        self,
        client: Client,
        *,
        source_id: str,
        registry: Registry | None = None,
        max_candidates: int = 50,
        display_options: int = 4,
        auto_select_unique: bool = True,
        probe_routes: bool = False,
    ) -> None:
        self.client = client
        self.source_id = source_id
        self.registry = registry or client.registry(source_id=source_id)
        self.max_candidates = max_candidates
        self.display_options = display_options
        self.auto_select_unique = auto_select_unique
        self.probe_routes = probe_routes

        # State tracking
        self._plan_id = uuid4().hex[:8]
        self._revision = 0
        self._state = SolverState.INIT
        self._intent: Intent | None = None
        self._requirements: list[Requirement] = []
        self._decisions: dict[str, Decision] = {}
        self._current_decision: str | None = None
        self._pending_targets: list[str] = []  # Targets awaiting route decisions

        # Route candidates (full set, not just displayed)
        self._candidates: dict[str, list[dict[str, Any]]] = {}  # target -> routes
        self._displayed: dict[str, list[str]] = {}  # target -> displayed route ids
        self._rejected: dict[str, set[str]] = {}  # target -> rejected route ids
        self._selected_routes: list[str] = []

        # Route storage (compatible with existing system)
        self._routes: dict[str, Route] = {}  # route_id -> Route

        # Filter conditions from intent
        self._filters: list[dict[str, Any]] = []

        # Operation replay cache: hash -> (revision, result)
        self._operation_cache: dict[str, tuple[int, dict[str, Any]]] = {}

        # Results
        self._compiled_query: str | None = None
        self._compiled_only: bool = False  # True if compile() was called instead of execute()
        self._result_reference: str | None = None
        self._final_query: dict[str, Any] | None = None
        self._execution_log: list[dict[str, Any]] = []

        # Milestone C: Binding-aware fragments
        self._binding_context = BindingContext()
        self._planning_fragments: dict[str, PlanningFragment] = {}
        self._query_fragments: list[QueryFragment] = []
        self._build_planning_fragments()

    @property
    def plan_id(self) -> str:
        return self._plan_id

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def state(self) -> SolverState:
        return self._state

    def _increment_revision(self) -> None:
        self._revision += 1

    def _log(self, action: str, data: dict[str, Any]) -> None:
        self._execution_log.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "plan_id": self._plan_id,
                "revision": self._revision,
                "action": action,
                **data,
            }
        )
        logger.debug("Solver %s rev %d: %s %s", self._plan_id, self._revision, action, data)

    def interpret(self, intent: Intent) -> CompactObservation:
        """Accept an intent and advance to the first decision or ready state."""
        self._intent = intent
        self._state = SolverState.INTERPRETING
        self._log("interpret", {"intent": intent.model_dump()})

        # Build requirements from intent
        self._requirements = []

        if intent.source_class:
            self._requirements.append(
                Requirement(
                    id=f"req-{len(self._requirements):02d}",
                    clause=f"Source class: {intent.source_class}",
                    kind="source",
                    status=RequirementStatus.RESOLVED,
                    bindings={"class": intent.source_class},
                )
            )

        for target in intent.target_classes:
            self._requirements.append(
                Requirement(
                    id=f"req-{len(self._requirements):02d}",
                    clause=f"Target class: {target}",
                    kind="target",
                    status=RequirementStatus.PENDING,
                    bindings={"class": target},
                )
            )

        for _i, filter_spec in enumerate(intent.filters):
            self._requirements.append(
                Requirement(
                    id=f"req-{len(self._requirements):02d}",
                    clause=f"Filter: {filter_spec}",
                    kind="filter",
                    status=RequirementStatus.PENDING,
                    bindings={"filter": filter_spec},
                )
            )

        self._increment_revision()
        return self._advance()

    def choose(self, decision_id: str, option_id: str) -> CompactObservation:
        """Submit a choice for a decision and advance."""
        if decision_id not in self._decisions:
            raise ValueError(f"Unknown decision: {decision_id}")

        decision = self._decisions[decision_id]
        if decision.id != self._current_decision:
            raise ValueError(
                f"Decision {decision_id} is not current (expected {self._current_decision})"
            )

        valid_ids = {opt.id for opt in decision.options}
        if option_id not in valid_ids:
            raise ValueError(f"Invalid option {option_id}. Valid: {', '.join(sorted(valid_ids))}")

        decision.selected = option_id
        self._log("choose", {"decision": decision_id, "option": option_id})

        # Apply the choice
        selected_option = next(opt for opt in decision.options if opt.id == option_id)
        if selected_option.route_id:
            self._selected_routes.append(selected_option.route_id)

        self._current_decision = None
        self._increment_revision()
        return self._advance()

    def show_more(self, decision_id: str) -> CompactObservation:
        """Request more options for a decision."""
        if decision_id not in self._decisions:
            raise ValueError(f"Unknown decision: {decision_id}")

        decision = self._decisions[decision_id]
        target = self._get_decision_target(decision)

        if target and target in self._candidates:
            all_routes = self._candidates[target]
            displayed = self._displayed.get(target, [])
            rejected = self._rejected.get(target, set())
            remaining = [
                r for r in all_routes if r["id"] not in displayed and r["id"] not in rejected
            ]

            if remaining:
                # Add more options (routes are already sorted by score)
                new_options = []
                for route in remaining[: self.display_options]:
                    opt_id = f"opt-{len(decision.options) + len(new_options):02d}"
                    new_options.append(
                        DecisionOption(
                            id=opt_id,
                            meaning=route.get("description", route["id"]),
                            route_id=route["id"],
                            evidence=route.get("evidence", {}),
                            score=route.get("score", 0.0),
                        )
                    )
                    displayed.append(route["id"])

                decision.options.extend(new_options)
                self._displayed[target] = displayed
                decision.more_available = len(remaining) > self.display_options

        self._log(
            "show_more",
            {
                "decision": decision_id,
                "total_options": len(decision.options),
                "more_available": decision.more_available,
            },
        )
        return self._observe()

    def reject_all(self, decision_id: str) -> CompactObservation:
        """Reject all displayed options and request new ones."""
        if decision_id not in self._decisions:
            raise ValueError(f"Unknown decision: {decision_id}")

        decision = self._decisions[decision_id]
        target = self._get_decision_target(decision)

        if target and target in self._candidates:
            # Mark current options as rejected
            for opt in decision.options:
                if opt.route_id:
                    self._rejected.setdefault(target, set()).add(opt.route_id)

            displayed = self._displayed.get(target, [])
            rejected = self._rejected.get(target, set())
            all_routes = self._candidates[target]
            remaining = [
                r for r in all_routes if r["id"] not in displayed and r["id"] not in rejected
            ]

            if not remaining:
                self._state = SolverState.BLOCKED
                self._log(
                    "reject_all_exhausted",
                    {
                        "decision": decision_id,
                        "total_candidates": len(all_routes),
                        "rejected": len(rejected),
                    },
                )
            else:
                # Replace options with new ones
                new_options = []
                for route in remaining[: self.display_options]:
                    opt_id = f"opt-{len(new_options):02d}"
                    new_options.append(
                        DecisionOption(
                            id=opt_id,
                            meaning=route.get("description", route["id"]),
                            route_id=route["id"],
                            evidence=route.get("evidence", {}),
                            score=self._rank_route(route),
                        )
                    )
                    displayed.append(route["id"])

                decision.options = new_options
                self._displayed[target] = displayed
                decision.more_available = len(remaining) > self.display_options
                self._log(
                    "reject_all_replaced",
                    {
                        "decision": decision_id,
                        "new_options": len(new_options),
                        "remaining": len(remaining) - len(new_options),
                    },
                )

        return self._observe()

    def _get_decision_target(self, decision: Decision) -> str | None:
        """Extract target class from decision clause."""
        # Decision clause is like "Route to <target_iri>"
        if "Route to " in decision.clause:
            return decision.clause.split("Route to ")[-1]
        return None

    def _rank_route(self, route: dict[str, Any]) -> float:
        """Rank a route by structural properties.

        Lower scores are better. Considers:
        - Hop count (fewer is better, primary factor)
        - Direction consistency (all forward or all backward is slightly better)
        """
        hops = route.get("hops", len(route.get("route", [])))
        route_data = route.get("route", [])

        # Base score from hop count (primary ranking factor)
        score = hops * 10.0

        # Minor bonus for direction consistency
        if route_data:
            directions = [edge[3] for edge in route_data]  # inverse flags
            if all(d == directions[0] for d in directions):
                score -= 1.0

        return score

    def compile(self) -> CompactObservation:
        """Compile the query without executing it.

        Returns the compiled state. Use execute() to run the query.
        """
        if self._state != SolverState.READY:
            raise ValueError(f"Cannot compile in state {self._state}")

        self._compiled_only = True
        self._log("compile_only", {"routes": self._selected_routes, "filters": len(self._filters)})
        return self._observe()

    def execute(self, session: Any = None) -> CompactObservation:
        """Execute the compiled query using execute_answer.

        If session is provided, use it. Otherwise use internal execution.
        """
        if self._state != SolverState.READY:
            raise ValueError(f"Cannot execute in state {self._state}")

        # Check for replay
        cache_key = self._execution_cache_key()
        if cache_key in self._operation_cache:
            cached_revision, cached_result = self._operation_cache[cache_key]
            self._log(
                "execute_replay", {"cache_key": cache_key, "cached_revision": cached_revision}
            )
            self._state = SolverState.COMPLETE
            self._result_reference = cached_result.get("reference")
            self._final_query = cached_result.get("final_query")
            return self._observe()

        self._log("execute_start", {"query": self._compiled_query})

        try:
            if session is not None:
                # Use session's execute_answer
                from rdfsolve.answer_query import execute_answer
                from rdfsolve.rdf_operations import PathFilter

                # Make sure routes are in session
                for route_id in self._selected_routes:
                    if route_id in self._routes:
                        session.routes[route_id] = self._routes[route_id]

                # Build filters
                filters = [PathFilter.model_validate(f) for f in self._filters]

                # Execute
                name = self._intent.selection if self._intent else "Answer"
                final = execute_answer(
                    session,
                    references=[],  # No source references needed for direct execution
                    name=name,
                    fields={},  # Default fields
                    paths=self._selected_routes,
                    where=filters,
                    expand_links=False,
                )

                self._final_query = final
                self._result_reference = uuid4().hex
                self._state = SolverState.COMPLETE

                # Cache the result
                self._operation_cache[cache_key] = (
                    self._revision,
                    {"reference": self._result_reference, "final_query": final},
                )

                self._log(
                    "execute_complete",
                    {
                        "reference": self._result_reference,
                        "rows": len(final.get("bindings", [])),
                    },
                )
            else:
                # Internal execution (minimal)
                self._state = SolverState.COMPLETE
                self._result_reference = uuid4().hex
                self._operation_cache[cache_key] = (
                    self._revision,
                    {"reference": self._result_reference, "final_query": None},
                )
                self._log("execute_complete", {"reference": self._result_reference})

        except Exception as e:
            self._state = SolverState.FAILED
            self._log("execute_failed", {"error": str(e)})
            raise

        self._increment_revision()
        return self._observe()

    def _execution_cache_key(self) -> str:
        """Generate cache key for execution replay."""
        key_data = {
            "routes": sorted(self._selected_routes),
            "filters": self._filters,
            "intent": self._intent.model_dump() if self._intent else None,
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()[:16]

    def revise(self, **updates: Any) -> CompactObservation:
        """Revise the current interpretation with updates.

        Allows changing filters, adding/removing targets, etc. without
        starting over completely.
        """
        if not self._intent:
            raise ValueError("No intent to revise. Call interpret() first.")

        self._log("revise_start", {"updates": updates})

        # Apply updates
        if "filters" in updates:
            self._intent = Intent(**{**self._intent.model_dump(), "filters": updates["filters"]})
            # Re-process filter requirements
            self._requirements = [r for r in self._requirements if r.kind != "filter"]
            for filter_spec in updates["filters"]:
                self._requirements.append(
                    Requirement(
                        id=f"req-{len(self._requirements):02d}",
                        clause=f"Filter: {filter_spec}",
                        kind="filter",
                        status=RequirementStatus.PENDING,
                        bindings={"filter": filter_spec},
                    )
                )

        if "targets" in updates:
            new_targets = updates["targets"]
            self._intent = Intent(**{**self._intent.model_dump(), "target_classes": new_targets})
            # Re-process target requirements
            self._requirements = [r for r in self._requirements if r.kind != "target"]
            for target in new_targets:
                self._requirements.append(
                    Requirement(
                        id=f"req-{len(self._requirements):02d}",
                        clause=f"Target class: {target}",
                        kind="target",
                        status=RequirementStatus.PENDING,
                        bindings={"class": target},
                    )
                )
            # Clear route decisions for removed targets
            self._pending_targets = []

        # Reset to interpreting state
        self._state = SolverState.INTERPRETING
        self._current_decision = None
        self._increment_revision()

        return self._advance()

    def _advance(self) -> CompactObservation:
        """Advance internally until a decision point or completion."""
        max_iterations = 100  # Safety bound
        iteration = 0

        while iteration < max_iterations:
            iteration += 1
            self._log("advance_iteration", {"iteration": iteration, "state": self._state.value})

            if self._state == SolverState.INTERPRETING:
                # Try to resolve deterministic bindings and find routes
                if not self._intent:
                    self._state = SolverState.BLOCKED
                    break

                # Collect all pending target requirements
                pending_targets = [
                    req
                    for req in self._requirements
                    if req.kind == "target" and req.status == RequirementStatus.PENDING
                ]

                if not pending_targets:
                    # All targets resolved, check filters
                    pending_filters = [
                        req
                        for req in self._requirements
                        if req.kind == "filter" and req.status == RequirementStatus.PENDING
                    ]
                    # Mark filters as resolved (they're validated at execution time)
                    for req in pending_filters:
                        req.status = RequirementStatus.RESOLVED

                    # Compile query
                    self._compile_query()
                    self._state = SolverState.READY
                    break

                # Process each pending target
                needs_decision = False
                for req in pending_targets:
                    target = req.bindings.get("class")
                    if not target or not self._intent.source_class:
                        req.status = RequirementStatus.UNSUPPORTED
                        req.reason = "Missing source or target class"
                        continue

                    routes = self._find_routes(self._intent.source_class, target)
                    # Filter out rejected routes
                    rejected = self._rejected.get(target, set())
                    available = [r for r in routes if r["id"] not in rejected]

                    if len(available) == 0:
                        if len(routes) > 0:
                            req.status = RequirementStatus.UNSUPPORTED
                            req.reason = "All routes rejected"
                        else:
                            req.status = RequirementStatus.UNSUPPORTED
                            req.reason = "No route found"
                    elif len(available) == 1 and self.auto_select_unique:
                        # Automatic selection when only one route exists
                        self._selected_routes.append(available[0]["id"])
                        req.status = RequirementStatus.RESOLVED
                        self._log("auto_select", {"target": target, "route": available[0]["id"]})
                    else:
                        # Need model to choose
                        needs_decision = True
                        self._create_route_decision(target, available)
                        break  # Handle one decision at a time

                if needs_decision:
                    self._state = SolverState.CHOOSE
                    break

            elif self._state == SolverState.CHOOSE:
                # Waiting for model decision
                break

            elif self._state == SolverState.READY:
                # Query compiled, waiting for execution
                break

            elif (
                self._state == SolverState.COMPLETE
                or self._state == SolverState.BLOCKED
                or self._state == SolverState.FAILED
            ):
                break

        if iteration >= max_iterations:
            self._state = SolverState.BLOCKED
            self._log("max_iterations", {"iteration": iteration})

        return self._observe()

    def _find_routes(self, source: str, target: str) -> list[dict[str, Any]]:
        """Find possible routes between source and target classes."""
        if target in self._candidates:
            return self._candidates[target]

        # Use client's path finding
        routes = []
        try:
            table = self.client.paths_between(
                source, target, max_hops=3, max_paths=self.max_candidates
            )
            for route in table.attrs.get("routes", []):
                route_id = self._route_id(route)
                # Store the route for later use
                self._routes[route_id] = route
                route_info = {
                    "id": route_id,
                    "route": route,
                    "hops": len(route),
                    "description": self._route_description(route),
                    "steps": self._route_steps(route),
                    "evidence": {},
                }
                route_info["score"] = self._rank_route(route_info)
                routes.append(route_info)
        except Exception as e:
            self._log("route_error", {"source": source, "target": target, "error": str(e)})

        # Sort by score (lower is better)
        routes.sort(key=lambda r: r.get("score", float("inf")))
        self._candidates[target] = routes
        self._log("routes_found", {"source": source, "target": target, "count": len(routes)})
        return routes

    def _route_id(self, route: Route) -> str:
        """Generate stable ID for a route."""
        key = self.registry.revision + json.dumps(route)
        return "path-" + hashlib.sha256(key.encode()).hexdigest()[:16]

    def _route_description(self, route: Route) -> str:
        """Generate human-readable route description."""
        parts = [self._class_label(route[0][0])]
        for _s, p, o, inverse in route:
            pred_name = p.rsplit("/", 1)[-1].rsplit("#", 1)[-1]
            arrow = " <- " if inverse else " -> "
            parts.append(f"{arrow}{pred_name}{arrow}{self._class_label(o)}")
        return "".join(parts)

    def _route_steps(self, route: Route) -> list[dict[str, Any]]:
        """Generate step details for a route."""
        steps = []
        for s, p, o, inverse in route:
            steps.append(
                {
                    "from": self._class_label(s),
                    "to": self._class_label(o),
                    "predicate": p,
                    "predicate_label": self._predicate_label(p),
                    "inverse": inverse,
                }
            )
        return steps

    def _class_label(self, class_iri: str) -> str:
        """Get label for a class IRI."""
        item = next((t for t in self.registry.types if t.id == class_iri), None)
        if item:
            return item.label
        return class_iri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

    def _predicate_label(self, predicate_iri: str) -> str:
        """Get label for a predicate IRI."""
        texts = self.client._schema.enrichment
        label = next(
            (item.text.value for item in texts.labels if item.term_iri == predicate_iri),
            None,
        )
        if label:
            return label
        return predicate_iri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

    def _create_route_decision(self, target: str, routes: list[dict[str, Any]]) -> None:
        """Create a decision for route selection."""
        decision_id = f"d-{len(self._decisions):02d}"

        # Filter out rejected routes
        rejected = self._rejected.get(target, set())
        available = [r for r in routes if r["id"] not in rejected]

        # Select top options to display (already sorted by score)
        displayed_routes = available[: self.display_options]
        self._displayed[target] = [r["id"] for r in displayed_routes]

        # Get target label for better clause
        target_label = self._class_label(target)

        options = [
            DecisionOption(
                id=f"opt-{i:02d}",
                meaning=route["description"],
                route_id=route["id"],
                evidence=route.get("evidence", {}),
                score=route.get("score", 0.0),
            )
            for i, route in enumerate(displayed_routes)
        ]

        decision = Decision(
            id=decision_id,
            clause=f"Route to {target}",
            kind="route_choice",
            options=options,
            more_available=len(available) > self.display_options,
        )

        self._decisions[decision_id] = decision
        self._current_decision = decision_id
        self._log(
            "create_decision",
            {
                "decision_id": decision_id,
                "target": target,
                "target_label": target_label,
                "options": len(options),
                "total_available": len(available),
            },
        )

    def _compile_query(self) -> None:
        """Compile the selected routes into a query using answer_query."""
        from rdfsolve.rdf_operations import PathFilter

        # Build filters from intent
        filters = []
        if self._intent:
            for filter_spec in self._intent.filters:
                kind = filter_spec.get("kind")
                terms = filter_spec.get("terms", [])
                iris = filter_spec.get("iris", [])
                fields = filter_spec.get("fields", [])
                if kind and (terms or iris):
                    filters.append(
                        PathFilter(
                            kind=kind,
                            terms=terms if terms else [],
                            iris=iris if iris else [],
                            fields=fields,
                        )
                    )

        self._filters = [f.model_dump() for f in filters]
        self._compiled_query = f"Routes: {self._selected_routes}, Filters: {len(filters)}"

        # Build binding-aware fragments from selected routes
        self._query_fragments = self._build_query_fragments()

        self._log(
            "compile_query",
            {
                "routes": self._selected_routes,
                "filters": len(filters),
                "fragments": len(self._query_fragments),
                "bindings": len(self._binding_context.all_bindings()),
            },
        )

    # =========================================================================
    # Milestone C: Fragment composition methods
    # =========================================================================

    def _build_planning_fragments(self) -> None:
        """Build planning fragments from registry shapes.

        Extracts navigable structure from schema to inform route planning.
        """
        for type_desc in self.registry.types:
            outgoing = []
            for field_desc in type_desc.fields:
                path_binding = field_desc.binding.get("path", {})
                if path_binding.get("operator") != "predicate":
                    continue
                predicate_iri = path_binding.get("iri", "")
                if not predicate_iri:
                    continue
                outgoing.append(
                    ShapeConstraint(
                        class_iri=type_desc.id,
                        field_name=field_desc.name,
                        predicate_iri=predicate_iri,
                        targets=list(field_desc.targets),
                        cardinality="one" if len(field_desc.targets) == 1 else "many",
                    )
                )

            self._planning_fragments[type_desc.id] = PlanningFragment(
                class_iri=type_desc.id,
                class_label=type_desc.label,
                outgoing=outgoing,
                incoming=[],  # Populated in second pass
            )

        # Second pass: populate incoming edges
        for source_iri, source_frag in self._planning_fragments.items():
            for constraint in source_frag.outgoing:
                for target_iri in constraint.targets:
                    if target_iri in self._planning_fragments:
                        self._planning_fragments[target_iri].incoming.append(
                            ShapeConstraint(
                                class_iri=source_iri,
                                field_name=constraint.field_name,
                                predicate_iri=constraint.predicate_iri,
                                targets=[source_iri],
                            )
                        )

        self._log(
            "build_planning_fragments",
            {
                "classes": len(self._planning_fragments),
                "total_constraints": sum(
                    len(f.outgoing) + len(f.incoming) for f in self._planning_fragments.values()
                ),
            },
        )

    def _build_query_fragments(self) -> list[QueryFragment]:
        """Build query fragments from selected routes with explicit bindings.

        Returns a list of JOIN fragments, one per selected route.
        """
        fragments = []
        self._binding_context = BindingContext()  # Reset for fresh compilation

        for route_id in self._selected_routes:
            if route_id not in self._routes:
                continue
            route = self._routes[route_id]
            fragment = self._route_to_fragment(route_id, route)
            fragments.append(fragment)

        return fragments

    def _route_to_fragment(self, route_id: str, route: Route) -> QueryFragment:
        """Convert a route to a JOIN fragment with explicit bindings."""
        children = []
        classes = [route[0][0], *[edge[2] for edge in route]]

        # Determine roles for each position
        roles = ["source"]
        for i in range(1, len(classes)):
            if i == len(classes) - 1:
                roles.append("target")
            else:
                roles.append("via")

        # Create node fragments with bindings
        prev_binding = None
        for i, (cls, role) in enumerate(zip(classes, roles, strict=True)):
            label = self._class_label(cls)
            binding = self._binding_context.allocate(cls, role, label)

            # Node fragment
            node_frag = QueryFragment(
                kind=FragmentKind.NODE,
                bindings=[binding],
            )
            children.append(node_frag)

            # Edge fragment connecting to previous node
            if prev_binding and i > 0:
                edge = route[i - 1]
                edge_frag = QueryFragment(
                    kind=FragmentKind.EDGE,
                    bindings=[prev_binding, binding],
                    predicate=edge[1],
                    inverse=edge[3],
                )
                children.append(edge_frag)

            prev_binding = binding

        return QueryFragment(
            kind=FragmentKind.JOIN,
            children=children,
            route_id=route_id,
        )

    def compose_union(self, fragments: list[QueryFragment]) -> QueryFragment:
        """Compose multiple fragments as alternatives (UNION)."""
        return QueryFragment(
            kind=FragmentKind.UNION,
            children=fragments,
        )

    def compose_filter(self, fragment: QueryFragment, condition: dict[str, Any]) -> QueryFragment:
        """Add a filter condition to a fragment."""
        filter_frag = QueryFragment(
            kind=FragmentKind.FILTER,
            condition=condition,
        )
        return QueryFragment(
            kind=FragmentKind.JOIN,
            children=[fragment, filter_frag],
        )

    def fragment_summary(self) -> dict[str, Any]:
        """Summarize current query fragments for observation."""
        bindings_by_class: dict[str, list[str]] = {}
        for binding in self._binding_context.all_bindings():
            bindings_by_class.setdefault(binding.class_iri, []).append(binding.variable)

        return {
            "fragments": len(self._query_fragments),
            "total_bindings": len(self._binding_context.all_bindings()),
            "bindings_by_class": {
                self._class_label(cls): vars for cls, vars in bindings_by_class.items()
            },
            "same_class_bindings": [
                {
                    "class": self._class_label(cls),
                    "variables": vars,
                    "count": len(vars),
                }
                for cls, vars in bindings_by_class.items()
                if len(vars) > 1
            ],
        }

    def get_planning_fragment(self, class_iri: str) -> PlanningFragment | None:
        """Get planning fragment for a class."""
        return self._planning_fragments.get(class_iri)

    def _observe(self) -> CompactObservation:
        """Create a compact observation of current state."""
        decision = None
        if self._current_decision:
            decision = self._decisions[self._current_decision]

        unresolved = [
            r.clause
            for r in self._requirements
            if r.status in (RequirementStatus.PENDING, RequirementStatus.UNSUPPORTED)
        ]

        # Get row count from final query if available
        rows = None
        if self._final_query:
            rows = len(self._final_query.get("bindings", []))

        return CompactObservation(
            plan_id=self._plan_id,
            revision=self._revision,
            state=self._state,
            decision=decision,
            reference=self._result_reference,
            execution="complete" if self._state == SolverState.COMPLETE else None,
            rows=rows,
            meaning=self._intent.selection if self._intent else None,
            unresolved=unresolved,
        )

    def get_log(self) -> list[dict[str, Any]]:
        """Return the execution log for debugging."""
        return deepcopy(self._execution_log)

    def export_state(self) -> dict[str, Any]:
        """Export full state for debugging/persistence."""
        return {
            "plan_id": self._plan_id,
            "revision": self._revision,
            "state": self._state.value,
            "intent": self._intent.model_dump() if self._intent else None,
            "requirements": [
                {
                    "id": r.id,
                    "clause": r.clause,
                    "kind": r.kind,
                    "status": r.status.value,
                    "bindings": r.bindings,
                    "reason": r.reason,
                }
                for r in self._requirements
            ],
            "decisions": {
                k: {
                    "id": v.id,
                    "clause": v.clause,
                    "kind": v.kind,
                    "options": [
                        {"id": o.id, "meaning": o.meaning, "route_id": o.route_id}
                        for o in v.options
                    ],
                    "selected": v.selected,
                    "more_available": v.more_available,
                }
                for k, v in self._decisions.items()
            },
            "selected_routes": self._selected_routes,
            "routes": dict(self._routes.items()),
            "filters": self._filters,
            "compiled_query": self._compiled_query,
            "result_reference": self._result_reference,
            "final_query": {
                "rows": len(self._final_query.get("bindings", [])),
                "coverage": self._final_query.get("coverage"),
            }
            if self._final_query
            else None,
        }
