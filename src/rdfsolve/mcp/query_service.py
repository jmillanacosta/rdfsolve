"""Query service implementing the 4-operation contract.

Operations:
- query_start: Begin a new query session with intent
- query_decide: Choose options, revise, expand, reject
- query_inspect: Read-only inspection of state
- query_finish: Emit query or execute

This replaces the compact_session/compact_mcp with a clean contract.
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

from pydantic import BaseModel, Field, field_validator

from rdfsolve.sparql_helper import SparqlHelper

if TYPE_CHECKING:
    from rdflib import Graph

    from rdfsolve.client_api import Client
    from rdfsolve.registry import Registry

logger = logging.getLogger(__name__)


# =============================================================================
# State enumeration
# =============================================================================


class QueryState(str, Enum):
    """External stable states for a query session."""

    CHOOSE = "choose"  # Waiting for model decision
    READY = "ready"  # Query compiled, ready to execute
    COMPLETE = "complete"  # Execution finished successfully
    BLOCKED = "blocked"  # Cannot proceed (unsupported, exhausted, etc.)
    FAILED = "failed"  # Unrecoverable error


class BlockedReason(str, Enum):
    """Reason codes for blocked state."""

    CANDIDATE_BUDGET = "candidate_budget"
    SEARCH_EXHAUSTED = "search_exhausted"
    OPTIONS_REJECTED = "options_rejected"
    UNSUPPORTED_REQUIREMENT = "unsupported_requirement"


class ErrorCode(str, Enum):
    """Error codes for operation failures."""

    INVALID_INPUT = "invalid_input"
    STALE_REVISION = "stale_revision"
    OPERATION_CONFLICT = "operation_conflict"
    INVALID_OPTION = "invalid_option"
    UNKNOWN_SESSION = "unknown_session"
    QUERY_NOT_READY = "query_not_ready"
    BACKEND_UNAVAILABLE = "backend_unavailable"


# =============================================================================
# Intent grammar models
# =============================================================================


class RoleDef(BaseModel):
    """Role definition in intent."""

    id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    class_hint: str | None = Field(default=None, description="Class label or IRI")


class RDFTerm(BaseModel):
    """RDF term representation."""

    type: str = Field(pattern=r"^(iri|literal)$")
    value: str
    datatype: str | None = None
    language: str | None = None

    @field_validator("datatype", "language")
    @classmethod
    def validate_exclusive(cls, v: str | None, info) -> str | None:
        """Validate datatype and language are mutually exclusive."""
        return v


class RelationRequirement(BaseModel):
    """Relation requirement in where clause."""

    id: str
    op: str = "relation"
    from_: str = Field(alias="from")
    to: str
    meaning: str
    via: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class BindRequirement(BaseModel):
    """Bind requirement in where clause."""

    id: str
    op: str = "bind"
    role: str
    term: RDFTerm


class CompareRequirement(BaseModel):
    """Compare requirement in where clause."""

    id: str
    op: str = "compare"
    role: str
    field: str
    operator: str = Field(pattern=r"^(eq|lt|le|gt|ge)$")
    term: RDFTerm


class DifferentRequirement(BaseModel):
    """Different requirement in where clause."""

    id: str
    op: str = "different"
    left: str
    right: str


class UnparsedRequirement(BaseModel):
    """Unparsed requirement that blocks readiness."""

    id: str
    text: str


class WhereClause(BaseModel):
    """Where clause expression tree."""

    op: str = Field(pattern=r"^(all|any|relation|bind|compare|different)$")
    args: list[Any] = Field(default_factory=list)
    # For leaf nodes
    id: str | None = None
    # For relation
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    meaning: str | None = None
    via: list[str] | None = None
    # For bind
    role: str | None = None
    term: RDFTerm | None = None
    # For compare
    field: str | None = None
    operator: str | None = None
    # For different
    left: str | None = None
    right: str | None = None

    model_config = {"populate_by_name": True}


class Intent(BaseModel):
    """Typed intent for query_start."""

    roles: list[RoleDef] = Field(min_length=1, max_length=20)
    where: WhereClause
    select: list[str] = Field(min_length=1, max_length=20)
    distinct: bool = True
    unparsed_requirements: list[UnparsedRequirement] = Field(default_factory=list)

    @field_validator("select")
    @classmethod
    def validate_select_roles(cls, v: list[str], info) -> list[str]:
        """Validate select references valid roles."""
        return v  # Full validation done in service


# =============================================================================
# Decision models
# =============================================================================


@dataclass
class DecisionOption:
    """A single option in a decision."""

    id: str
    meaning: str
    route_id: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class Decision:
    """A pending decision requiring model input."""

    id: str
    requirement_id: str
    options: list[DecisionOption]
    more_retained: bool = False
    search_exhausted: bool = False
    can_expand: bool = False


# =============================================================================
# Session and artifact models
# =============================================================================


@dataclass
class QueryArtifact:
    """Immutable query artifact."""

    id: str
    query: str
    projection: dict[str, str]  # role -> variable
    scope: str
    schema_revision: str
    plan_revision: int
    coverage: dict[str, list[str]]  # requirement_id -> represented elements


@dataclass
class ResultArtifact:
    """Immutable result artifact."""

    id: str
    query_artifact_id: str
    bindings: list[dict[str, Any]]
    row_count: int


@dataclass
class Session:
    """Query session state."""

    id: str
    question: str
    intent: Intent
    scope: str
    revision: int = 1
    state: QueryState = QueryState.CHOOSE
    blocked_reason: BlockedReason | None = None
    blocked_requirement_ids: list[str] = field(default_factory=list)

    # Requirements tracking
    requirements: dict[str, dict[str, Any]] = field(default_factory=dict)
    resolved_requirements: set[str] = field(default_factory=set)

    # Decision tracking
    current_decision: Decision | None = None
    decisions_made: dict[str, str] = field(default_factory=dict)  # decision_id -> option_id

    # Route tracking
    selected_routes: list[str] = field(default_factory=list)
    route_candidates: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    displayed_options: dict[str, list[str]] = field(default_factory=dict)
    rejected_options: dict[str, set[str]] = field(default_factory=dict)

    # Artifacts
    query_artifact: QueryArtifact | None = None
    result_artifact: ResultArtifact | None = None


# =============================================================================
# Query Service
# =============================================================================


class QueryService:
    """Service implementing the 4-operation query contract.

    Operations:
    - query_start: Begin session with intent
    - query_decide: Choose/revise/expand/reject
    - query_inspect: Read-only state inspection
    - query_finish: Emit or execute
    """

    def __init__(
        self,
        client: Client,
        *,
        source_id: str | None = None,
        candidate_batch: int = 50,
        display_window: int = 4,
        total_ceiling: int = 200,
        probes_enabled: bool = False,
    ) -> None:
        self.client = client
        self.source_id = source_id or client._schema.about.dataset_name or "rdf"
        self.registry = client.registry(source_id=self.source_id)

        # Configuration
        self.candidate_batch = candidate_batch
        self.display_window = display_window
        self.total_ceiling = total_ceiling
        self.probes_enabled = probes_enabled

        # Session storage
        self._sessions: dict[str, Session] = {}

        # Operation replay cache: (session_id, operation_id) -> (revision, response)
        self._operation_cache: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}

        # Artifact storage
        self._artifacts: dict[str, QueryArtifact | ResultArtifact] = {}

        # Backend call counter for testing
        self._backend_calls: int = 0

        # Execution lock (for concurrent identical execution)
        self._executing: set[str] = set()

    # =========================================================================
    # Public operations
    # =========================================================================

    def query_start(
        self,
        operation_id: str,
        question: str,
        intent: dict[str, Any],
        scope: str | None = None,
    ) -> dict[str, Any]:
        """Start a new query session.

        Args:
            operation_id: Unique operation identifier
            question: Original question text (retained verbatim)
            intent: Typed intent dictionary
            scope: Source scope (defaults to configured source)

        Returns:
            Observation with session, revision, state, and decision if needed
        """
        scope = scope or self.source_id

        # Check for replay
        cache_key = self._operation_cache_key(None, operation_id, "start", intent)
        if cache_key in self._operation_cache:
            cached_revision, cached_response = self._operation_cache[cache_key]
            return cached_response

        # Validate and parse intent
        try:
            parsed_intent = Intent.model_validate(intent)
        except Exception as e:
            return self._error_response(ErrorCode.INVALID_INPUT, str(e))

        # Validate intent consistency
        validation_error = self._validate_intent(parsed_intent)
        if validation_error:
            return self._error_response(ErrorCode.INVALID_INPUT, validation_error)

        # Create session
        session_id = uuid4().hex[:16]
        session = Session(
            id=session_id,
            question=question,
            intent=parsed_intent,
            scope=scope,
        )
        self._sessions[session_id] = session

        # Build requirements from intent
        self._build_requirements(session)

        # Check for unparsed requirements
        if parsed_intent.unparsed_requirements:
            session.state = QueryState.BLOCKED
            session.blocked_reason = BlockedReason.UNSUPPORTED_REQUIREMENT
            session.blocked_requirement_ids = [r.id for r in parsed_intent.unparsed_requirements]
            response = self._observation(session)
            self._operation_cache[cache_key] = (session.revision, response)
            return response

        # Advance to first decision or ready
        self._advance(session)

        # Cache and return
        response = self._observation(session)
        self._operation_cache[cache_key] = (session.revision, response)
        return response

    def query_decide(
        self,
        session_id: str,
        revision: int,
        operation_id: str,
        action: dict[str, Any],
    ) -> dict[str, Any]:
        """Submit a decision action.

        Actions:
        - {"type": "choose", "decision": "d1", "option": "o1"}
        - {"type": "more", "decision": "d1"}
        - {"type": "reject", "decision": "d1"}
        - {"type": "expand", "decision": "d1"}
        - {"type": "revise", "intent": {...}}

        Returns:
            Updated observation
        """
        # Check session exists
        if session_id not in self._sessions:
            return self._error_response(ErrorCode.UNKNOWN_SESSION, f"Unknown session: {session_id}")

        session = self._sessions[session_id]

        # Check for replay
        cache_key = self._operation_cache_key(session_id, operation_id, "decide", action)
        if cache_key in self._operation_cache:
            cached_revision, cached_response = self._operation_cache[cache_key]
            return cached_response

        # Check stale revision (after replay check per spec)
        if revision != session.revision:
            return self._error_response(
                ErrorCode.STALE_REVISION,
                f"Expected revision {session.revision}, got {revision}",
            )

        action_type = action.get("type")

        if action_type == "choose":
            return self._handle_choose(session, operation_id, action, cache_key)
        elif action_type == "more":
            return self._handle_more(session, operation_id, action, cache_key)
        elif action_type == "reject":
            return self._handle_reject(session, operation_id, action, cache_key)
        elif action_type == "expand":
            return self._handle_expand(session, operation_id, action, cache_key)
        elif action_type == "revise":
            return self._handle_revise(session, operation_id, action, cache_key)
        else:
            return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown action type: {action_type}")

    def query_inspect(
        self,
        session_id: str,
        revision: int,
        target: str,
    ) -> dict[str, Any]:
        """Inspect session state (read-only).

        Targets:
        - "status": Current state summary
        - "decision:d1": Decision details
        - "option:d1:o1": Option details
        - "requirement:r1": Requirement details

        Returns:
            Inspection result (no state change)
        """
        if session_id not in self._sessions:
            return self._error_response(ErrorCode.UNKNOWN_SESSION, f"Unknown session: {session_id}")

        session = self._sessions[session_id]

        if target == "status":
            return self._inspect_status(session)
        elif target.startswith("decision:"):
            decision_id = target.split(":", 1)[1]
            return self._inspect_decision(session, decision_id)
        elif target.startswith("option:"):
            parts = target.split(":")
            if len(parts) == 3:
                return self._inspect_option(session, parts[1], parts[2])
            return self._error_response(ErrorCode.INVALID_INPUT, "Invalid option target format")
        elif target.startswith("requirement:"):
            req_id = target.split(":", 1)[1]
            return self._inspect_requirement(session, req_id)
        else:
            return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown target: {target}")

    def query_finish(
        self,
        session_id: str,
        revision: int,
        action: dict[str, Any],
    ) -> dict[str, Any]:
        """Emit query or execute.

        Actions:
        - {"type": "emit_query"}: Return compiled query artifact (read-only)
        - {"type": "execute", "operation_id": "e1"}: Execute and return result

        Returns:
            Artifact reference or execution result
        """
        if session_id not in self._sessions:
            return self._error_response(ErrorCode.UNKNOWN_SESSION, f"Unknown session: {session_id}")

        session = self._sessions[session_id]

        action_type = action.get("type")

        if action_type == "emit_query":
            return self._handle_emit(session, revision)
        elif action_type == "execute":
            operation_id = action.get("operation_id")
            if not operation_id:
                return self._error_response(ErrorCode.INVALID_INPUT, "execute requires operation_id")
            return self._handle_execute(session, revision, operation_id)
        else:
            return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown action type: {action_type}")

    def read_artifact(self, artifact_id: str) -> dict[str, Any]:
        """Read an artifact by ID."""
        if artifact_id not in self._artifacts:
            return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown artifact: {artifact_id}")

        artifact = self._artifacts[artifact_id]
        if isinstance(artifact, QueryArtifact):
            return {
                "type": "query",
                "id": artifact.id,
                "query": artifact.query,
                "projection": artifact.projection,
                "scope": artifact.scope,
                "schema_revision": artifact.schema_revision,
                "plan_revision": artifact.plan_revision,
                "coverage": artifact.coverage,
            }
        else:
            return {
                "type": "result",
                "id": artifact.id,
                "query_artifact_id": artifact.query_artifact_id,
                "bindings": artifact.bindings,
                "row_count": artifact.row_count,
            }

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _operation_cache_key(
        self,
        session_id: str | None,
        operation_id: str,
        op_type: str,
        payload: Any,
    ) -> str:
        """Generate cache key for operation replay."""
        key_data = {
            "session": session_id,
            "operation": operation_id,
            "type": op_type,
            "payload": payload,
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()[:24]

    def _validate_intent(self, intent: Intent) -> str | None:
        """Validate intent consistency. Returns error message or None."""
        role_ids = {r.id for r in intent.roles}

        # Check select references valid roles
        for sel in intent.select:
            if sel not in role_ids:
                return f"select references unknown role: {sel}"

        # Validate where clause role references
        error = self._validate_where_roles(intent.where, role_ids)
        if error:
            return error

        # Check for duplicate requirement IDs
        req_ids: set[str] = set()
        error = self._collect_requirement_ids(intent.where, req_ids)
        if error:
            return error

        for up in intent.unparsed_requirements:
            if up.id in req_ids:
                return f"Duplicate requirement ID: {up.id}"
            req_ids.add(up.id)

        return None

    def _validate_where_roles(self, where: WhereClause, role_ids: set[str]) -> str | None:
        """Recursively validate role references in where clause."""
        if where.op in ("all", "any"):
            for arg in where.args:
                if isinstance(arg, dict):
                    try:
                        child = WhereClause.model_validate(arg)
                        error = self._validate_where_roles(child, role_ids)
                        if error:
                            return error
                    except Exception as e:
                        return f"Invalid where clause: {e}"
        elif where.op == "relation":
            if where.from_ and where.from_ not in role_ids:
                return f"relation references unknown role: {where.from_}"
            if where.to and where.to not in role_ids:
                return f"relation references unknown role: {where.to}"
            for via_role in where.via or []:
                if via_role not in role_ids:
                    return f"via references unknown role: {via_role}"
        elif where.op in ("bind", "compare"):
            if where.role and where.role not in role_ids:
                return f"{where.op} references unknown role: {where.role}"
        elif where.op == "different":
            if where.left and where.left not in role_ids:
                return f"different references unknown role: {where.left}"
            if where.right and where.right not in role_ids:
                return f"different references unknown role: {where.right}"

        return None

    def _collect_requirement_ids(self, where: WhereClause, req_ids: set[str]) -> str | None:
        """Collect requirement IDs and check for duplicates."""
        if where.op in ("all", "any"):
            for arg in where.args:
                if isinstance(arg, dict):
                    try:
                        child = WhereClause.model_validate(arg)
                        error = self._collect_requirement_ids(child, req_ids)
                        if error:
                            return error
                    except Exception:
                        pass
        else:
            if where.id:
                if where.id in req_ids:
                    return f"Duplicate requirement ID: {where.id}"
                req_ids.add(where.id)
        return None

    def _build_requirements(self, session: Session) -> None:
        """Build requirements from intent where clause."""
        self._extract_requirements(session, session.intent.where)

    def _extract_requirements(self, session: Session, where: WhereClause) -> None:
        """Recursively extract requirements from where clause."""
        if where.op in ("all", "any"):
            for arg in where.args:
                if isinstance(arg, dict):
                    child = WhereClause.model_validate(arg)
                    self._extract_requirements(session, child)
        else:
            if where.id:
                session.requirements[where.id] = {
                    "op": where.op,
                    "resolved": False,
                    "clause": where.model_dump(by_alias=True, exclude_none=True),
                }

    def _advance(self, session: Session) -> None:
        """Advance session to next decision or ready state."""
        # Find unresolved relation requirements
        for req_id, req in session.requirements.items():
            if req["resolved"]:
                continue

            if req["op"] == "relation":
                # Need to find routes
                clause = req["clause"]
                from_role = clause.get("from")
                to_role = clause.get("to")
                meaning = clause.get("meaning", "")
                via = clause.get("via", [])

                # Resolve class hints
                from_class = self._resolve_class(session, from_role)
                to_class = self._resolve_class(session, to_role)

                if not from_class or not to_class:
                    session.state = QueryState.BLOCKED
                    session.blocked_reason = BlockedReason.SEARCH_EXHAUSTED
                    session.blocked_requirement_ids = [req_id]
                    return

                # Find routes
                routes = self._find_routes(from_class, to_class, meaning, via)

                if not routes:
                    session.state = QueryState.BLOCKED
                    session.blocked_reason = BlockedReason.SEARCH_EXHAUSTED
                    session.blocked_requirement_ids = [req_id]
                    return

                # Store candidates
                session.route_candidates[req_id] = routes

                # Check for unique unambiguous match
                if len(routes) == 1:
                    # Auto-select unique route
                    route = routes[0]
                    session.selected_routes.append(route["id"])
                    session.requirements[req_id]["resolved"] = True
                    session.resolved_requirements.add(req_id)
                    continue

                # Create decision
                self._create_decision(session, req_id, routes)
                session.state = QueryState.CHOOSE
                return

            elif req["op"] == "bind":
                # Bind is immediately resolved
                session.requirements[req_id]["resolved"] = True
                session.resolved_requirements.add(req_id)

            elif req["op"] == "compare":
                # Compare is resolved at compile time
                session.requirements[req_id]["resolved"] = True
                session.resolved_requirements.add(req_id)

            elif req["op"] == "different":
                # Different is resolved at compile time
                session.requirements[req_id]["resolved"] = True
                session.resolved_requirements.add(req_id)

        # All requirements resolved - compile query
        self._compile_query(session)

    def _resolve_class(self, session: Session, role_id: str) -> str | None:
        """Resolve role to class IRI."""
        for role in session.intent.roles:
            if role.id == role_id:
                if role.class_hint:
                    # Try to find class by label or IRI
                    for type_desc in self.registry.types:
                        if type_desc.label == role.class_hint or type_desc.id == role.class_hint:
                            return type_desc.id
                    # Not found - return hint as-is (might be full IRI)
                    return role.class_hint
        return None

    def _find_routes(
        self,
        from_class: str,
        to_class: str,
        meaning: str,
        via: list[str],
    ) -> list[dict[str, Any]]:
        """Find routes between classes."""
        routes = []
        try:
            table = self.client.paths_between(
                from_class, to_class, max_hops=3, max_paths=self.candidate_batch
            )
            for route in table.attrs.get("routes", []):
                route_id = self._route_id(route)
                description = self._route_description(route)
                routes.append({
                    "id": route_id,
                    "route": route,
                    "description": description,
                    "hops": len(route),
                    "meaning": meaning,
                })
        except Exception as e:
            logger.warning("Route search failed: %s", e)

        return routes

    def _route_id(self, route: list) -> str:
        """Generate stable route ID."""
        key = json.dumps(route)
        return "route-" + hashlib.sha256(key.encode()).hexdigest()[:16]

    def _route_description(self, route: list) -> str:
        """Generate human-readable route description."""
        parts = []
        for s, p, o, inverse in route:
            pred_label = self._get_predicate_label(p)
            direction = "<-" if inverse else "->"
            parts.append(f"{direction} {pred_label}")
        return " ".join(parts)

    def _get_predicate_label(self, predicate_iri: str) -> str:
        """Get label for predicate."""
        texts = self.client._schema.enrichment
        label = next(
            (item.text.value for item in texts.labels if item.term_iri == predicate_iri),
            None,
        )
        if label:
            return label
        return predicate_iri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

    def _create_decision(
        self,
        session: Session,
        req_id: str,
        routes: list[dict[str, Any]],
    ) -> None:
        """Create a decision for route selection."""
        decision_id = f"d-{len(session.decisions_made):03d}"

        # Get displayed window
        displayed_routes = routes[:self.display_window]
        session.displayed_options[req_id] = [r["id"] for r in displayed_routes]

        options = [
            DecisionOption(
                id=f"o-{i:03d}",
                meaning=r["description"],
                route_id=r["id"],
            )
            for i, r in enumerate(displayed_routes)
        ]

        session.current_decision = Decision(
            id=decision_id,
            requirement_id=req_id,
            options=options,
            more_retained=len(routes) > self.display_window,
            search_exhausted=len(routes) <= self.candidate_batch,
            can_expand=len(routes) == self.candidate_batch,
        )

    def _compile_query(self, session: Session) -> None:
        """Compile the query from resolved requirements."""
        # Build SPARQL query
        sparql_parts = []
        projection = {}
        filters = []

        # Build triple patterns from selected routes
        for route_id in session.selected_routes:
            # Find the route in candidates
            for req_id, candidates in session.route_candidates.items():
                for candidate in candidates:
                    if candidate["id"] == route_id:
                        route = candidate["route"]
                        # Generate triple patterns
                        patterns = self._route_to_patterns(session, req_id, route)
                        sparql_parts.extend(patterns)
                        break

        # Build filters from compare requirements
        for req_id, req in session.requirements.items():
            if req["op"] == "compare":
                clause = req["clause"]
                role = clause.get("role")
                field_name = clause.get("field")
                operator = clause.get("operator")
                term = clause.get("term", {})

                var = self._role_variable(session, role)
                filter_expr = self._build_filter(var, field_name, operator, term)
                if filter_expr:
                    filters.append(filter_expr)

        # Build bind constraints
        for req_id, req in session.requirements.items():
            if req["op"] == "bind":
                clause = req["clause"]
                role = clause.get("role")
                term = clause.get("term", {})

                var = self._role_variable(session, role)
                if term.get("type") == "iri":
                    sparql_parts.append(f"FILTER(?{var} = <{term['value']}>)")

        # Build different constraints
        for req_id, req in session.requirements.items():
            if req["op"] == "different":
                clause = req["clause"]
                left = clause.get("left")
                right = clause.get("right")

                left_var = self._role_variable(session, left)
                right_var = self._role_variable(session, right)
                sparql_parts.append(f"FILTER(?{left_var} != ?{right_var})")

        # Build projection
        for role_id in session.intent.select:
            var = self._role_variable(session, role_id)
            projection[role_id] = var

        # Assemble query
        select_vars = " ".join(f"?{v}" for v in projection.values())
        distinct = "DISTINCT " if session.intent.distinct else ""
        where_body = " . ".join(sparql_parts)
        if filters:
            where_body += " . " + " . ".join(filters)

        query = f"SELECT {distinct}{select_vars} WHERE {{ {where_body} }}"

        # Create artifact
        artifact_id = f"query-{uuid4().hex[:12]}"
        coverage = {req_id: ["represented"] for req_id in session.resolved_requirements}

        artifact = QueryArtifact(
            id=artifact_id,
            query=query,
            projection=projection,
            scope=session.scope,
            schema_revision=self.registry.revision,
            plan_revision=session.revision,
            coverage=coverage,
        )

        self._artifacts[artifact_id] = artifact
        session.query_artifact = artifact
        session.state = QueryState.READY

    def _route_to_patterns(
        self,
        session: Session,
        req_id: str,
        route: list,
    ) -> list[str]:
        """Convert route to SPARQL triple patterns."""
        patterns = []
        clause = session.requirements[req_id]["clause"]
        from_role = clause.get("from")
        to_role = clause.get("to")

        # Get or create variables for each step
        current_var = self._role_variable(session, from_role)

        # Add type constraint for source
        from_class = self._resolve_class(session, from_role)
        if from_class:
            patterns.append(f"?{current_var} a <{from_class}>")

        for i, (s, p, o, inverse) in enumerate(route):
            if i == len(route) - 1:
                # Last step - use target role variable
                next_var = self._role_variable(session, to_role)
            else:
                # Intermediate step - create new variable
                next_var = f"v{len(patterns)}"

            if inverse:
                patterns.append(f"?{next_var} <{p}> ?{current_var}")
            else:
                patterns.append(f"?{current_var} <{p}> ?{next_var}")

            current_var = next_var

        # Add type constraint for target
        to_class = self._resolve_class(session, to_role)
        if to_class:
            patterns.append(f"?{current_var} a <{to_class}>")

        return patterns

    def _role_variable(self, session: Session, role_id: str) -> str:
        """Get SPARQL variable name for a role."""
        # Simple mapping - role_id becomes variable name
        return role_id

    def _build_filter(
        self,
        var: str,
        field_name: str,
        operator: str,
        term: dict,
    ) -> str | None:
        """Build FILTER expression for comparison."""
        # Need to find predicate for field
        # For now, assume field_name is predicate local name
        term_value = term.get("value")
        term_type = term.get("type")
        datatype = term.get("datatype")

        if operator == "eq":
            if term_type == "literal":
                if datatype:
                    return f"FILTER(?{var}_field = \"{term_value}\"^^<{datatype}>)"
                return f"FILTER(?{var}_field = \"{term_value}\")"
            return f"FILTER(?{var}_field = <{term_value}>)"

        op_map = {"lt": "<", "le": "<=", "gt": ">", "ge": ">="}
        sparql_op = op_map.get(operator, "=")

        if datatype:
            return f"FILTER(?{var}_field {sparql_op} \"{term_value}\"^^<{datatype}>)"
        return f"FILTER(?{var}_field {sparql_op} {term_value})"

    def _observation(self, session: Session) -> dict[str, Any]:
        """Build observation response."""
        result: dict[str, Any] = {
            "session": session.id,
            "revision": session.revision,
            "state": session.state.value,
        }

        if session.state == QueryState.CHOOSE and session.current_decision:
            d = session.current_decision
            result["decision"] = {
                "id": d.id,
                "requirement_id": d.requirement_id,
                "options": [
                    {"id": o.id, "meaning": o.meaning} for o in d.options
                ],
                "more_retained": d.more_retained,
                "search_exhausted": d.search_exhausted,
                "can_expand": d.can_expand,
            }

        if session.state == QueryState.BLOCKED:
            result["reason"] = session.blocked_reason.value if session.blocked_reason else None
            result["blocked_requirement_ids"] = session.blocked_requirement_ids

        if session.state == QueryState.READY and session.query_artifact:
            result["query_ref"] = f"rdfsolve://sessions/{session.id}/artifacts/{session.query_artifact.id}"

        if session.state == QueryState.COMPLETE:
            result["execution"] = "complete"
            if session.result_artifact:
                result["result_ref"] = f"rdfsolve://sessions/{session.id}/artifacts/{session.result_artifact.id}"
                result["rows"] = session.result_artifact.row_count

        # Always include unresolved requirements
        unresolved = [
            req_id for req_id, req in session.requirements.items()
            if not req["resolved"]
        ]
        if unresolved:
            result["unresolved"] = unresolved

        return result

    def _error_response(self, code: ErrorCode, message: str) -> dict[str, Any]:
        """Build error response."""
        return {
            "error": {
                "code": code.value,
                "message": message,
            }
        }

    # =========================================================================
    # Action handlers
    # =========================================================================

    def _handle_choose(
        self,
        session: Session,
        operation_id: str,
        action: dict[str, Any],
        cache_key: str,
    ) -> dict[str, Any]:
        """Handle choose action."""
        decision_id = action.get("decision")
        option_id = action.get("option")

        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_OPTION,
                f"Decision {decision_id} is not current",
            )

        # Find the option
        option = next(
            (o for o in session.current_decision.options if o.id == option_id),
            None,
        )
        if not option:
            return self._error_response(
                ErrorCode.INVALID_OPTION,
                f"Unknown option: {option_id}",
            )

        # Apply choice
        if option.route_id:
            session.selected_routes.append(option.route_id)

        req_id = session.current_decision.requirement_id
        session.requirements[req_id]["resolved"] = True
        session.resolved_requirements.add(req_id)
        session.decisions_made[decision_id] = option_id
        session.current_decision = None

        # Increment revision
        session.revision += 1

        # Advance to next decision or ready
        self._advance(session)

        response = self._observation(session)
        self._operation_cache[cache_key] = (session.revision, response)
        return response

    def _handle_more(
        self,
        session: Session,
        operation_id: str,
        action: dict[str, Any],
        cache_key: str,
    ) -> dict[str, Any]:
        """Handle more action - show next window without search."""
        decision_id = action.get("decision")

        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_OPTION,
                f"Decision {decision_id} is not current",
            )

        req_id = session.current_decision.requirement_id
        candidates = session.route_candidates.get(req_id, [])
        displayed = session.displayed_options.get(req_id, [])
        rejected = session.rejected_options.get(req_id, set())

        # Find undisplayed, non-rejected routes
        available = [r for r in candidates if r["id"] not in displayed and r["id"] not in rejected]

        if not available:
            # No more to show
            return self._observation(session)

        # Add next window
        new_routes = available[:self.display_window]
        new_options = [
            DecisionOption(
                id=f"o-{len(session.current_decision.options) + i:03d}",
                meaning=r["description"],
                route_id=r["id"],
            )
            for i, r in enumerate(new_routes)
        ]

        session.current_decision.options.extend(new_options)
        displayed.extend([r["id"] for r in new_routes])
        session.displayed_options[req_id] = displayed
        session.current_decision.more_retained = len(available) > self.display_window

        return self._observation(session)

    def _handle_reject(
        self,
        session: Session,
        operation_id: str,
        action: dict[str, Any],
        cache_key: str,
    ) -> dict[str, Any]:
        """Handle reject action - reject current window."""
        decision_id = action.get("decision")

        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_OPTION,
                f"Decision {decision_id} is not current",
            )

        req_id = session.current_decision.requirement_id
        candidates = session.route_candidates.get(req_id, [])

        # Mark current options as rejected
        rejected = session.rejected_options.setdefault(req_id, set())
        for opt in session.current_decision.options:
            if opt.route_id:
                rejected.add(opt.route_id)

        # Find remaining non-rejected routes
        available = [r for r in candidates if r["id"] not in rejected]

        if not available:
            session.state = QueryState.BLOCKED
            session.blocked_reason = BlockedReason.OPTIONS_REJECTED
            session.blocked_requirement_ids = [req_id]
            session.current_decision = None
            session.revision += 1
        else:
            # Create new decision with fresh options
            self._create_decision(session, req_id, available)
            session.revision += 1

        response = self._observation(session)
        self._operation_cache[cache_key] = (session.revision, response)
        return response

    def _handle_expand(
        self,
        session: Session,
        operation_id: str,
        action: dict[str, Any],
        cache_key: str,
    ) -> dict[str, Any]:
        """Handle expand action - search for more candidates."""
        decision_id = action.get("decision")

        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_OPTION,
                f"Decision {decision_id} is not current",
            )

        if not session.current_decision.can_expand:
            return self._observation(session)

        # TODO: Actually search for more candidates
        # For now, just mark as exhausted
        session.current_decision.can_expand = False
        session.current_decision.search_exhausted = True

        return self._observation(session)

    def _handle_revise(
        self,
        session: Session,
        operation_id: str,
        action: dict[str, Any],
        cache_key: str,
    ) -> dict[str, Any]:
        """Handle revise action - replace intent."""
        new_intent = action.get("intent")
        if not new_intent:
            return self._error_response(ErrorCode.INVALID_INPUT, "revise requires intent")

        try:
            parsed_intent = Intent.model_validate(new_intent)
        except Exception as e:
            return self._error_response(ErrorCode.INVALID_INPUT, str(e))

        validation_error = self._validate_intent(parsed_intent)
        if validation_error:
            return self._error_response(ErrorCode.INVALID_INPUT, validation_error)

        # Reset session with new intent
        session.intent = parsed_intent
        session.requirements = {}
        session.resolved_requirements = set()
        session.current_decision = None
        session.selected_routes = []
        session.route_candidates = {}
        session.displayed_options = {}
        session.rejected_options = {}
        session.query_artifact = None
        session.result_artifact = None
        session.revision += 1

        # Rebuild requirements and advance
        self._build_requirements(session)

        if parsed_intent.unparsed_requirements:
            session.state = QueryState.BLOCKED
            session.blocked_reason = BlockedReason.UNSUPPORTED_REQUIREMENT
            session.blocked_requirement_ids = [r.id for r in parsed_intent.unparsed_requirements]
        else:
            self._advance(session)

        response = self._observation(session)
        self._operation_cache[cache_key] = (session.revision, response)
        return response

    def _handle_emit(self, session: Session, revision: int) -> dict[str, Any]:
        """Handle emit_query action."""
        if session.state != QueryState.READY:
            return self._error_response(
                ErrorCode.QUERY_NOT_READY,
                f"Cannot emit in state {session.state.value}",
            )

        if not session.query_artifact:
            return self._error_response(
                ErrorCode.QUERY_NOT_READY,
                "No query artifact available",
            )

        # Read-only - return artifact reference
        return {
            "session": session.id,
            "revision": session.revision,
            "state": session.state.value,
            "query_ref": f"rdfsolve://sessions/{session.id}/artifacts/{session.query_artifact.id}",
        }

    def _handle_execute(
        self,
        session: Session,
        revision: int,
        operation_id: str,
    ) -> dict[str, Any]:
        """Handle execute action."""
        if session.state not in (QueryState.READY, QueryState.COMPLETE):
            return self._error_response(
                ErrorCode.QUERY_NOT_READY,
                f"Cannot execute in state {session.state.value}",
            )

        # Check for replay
        cache_key = self._operation_cache_key(session.id, operation_id, "execute", None)
        if cache_key in self._operation_cache:
            _, cached_response = self._operation_cache[cache_key]
            return cached_response

        if not session.query_artifact:
            return self._error_response(
                ErrorCode.QUERY_NOT_READY,
                "No query artifact available",
            )

        # Check for concurrent execution
        exec_key = f"{session.id}:{operation_id}"
        if exec_key in self._executing:
            # Return existing result if available
            if session.result_artifact:
                return self._observation(session)
            # Still executing - shouldn't happen in single-threaded
        self._executing.add(exec_key)

        try:
            # Execute query
            query = session.query_artifact.query
            self._backend_calls += 1

            # Use client's SPARQL helper
            helper = SparqlHelper(self.client._endpoint)
            try:
                result = helper.query(query)
                bindings = list(result)
            except Exception as e:
                self._executing.discard(exec_key)
                return self._error_response(
                    ErrorCode.BACKEND_UNAVAILABLE,
                    str(e),
                )

            # Create result artifact
            result_id = f"result-{uuid4().hex[:12]}"
            result_artifact = ResultArtifact(
                id=result_id,
                query_artifact_id=session.query_artifact.id,
                bindings=[dict(row.asdict()) for row in bindings],
                row_count=len(bindings),
            )

            self._artifacts[result_id] = result_artifact
            session.result_artifact = result_artifact
            session.state = QueryState.COMPLETE
            session.revision += 1

            response = self._observation(session)
            self._operation_cache[cache_key] = (session.revision, response)
            return response

        finally:
            self._executing.discard(exec_key)

    def _inspect_status(self, session: Session) -> dict[str, Any]:
        """Return session status summary."""
        return {
            "session": session.id,
            "revision": session.revision,
            "state": session.state.value,
            "question": session.question,
            "scope": session.scope,
            "resolved_requirements": list(session.resolved_requirements),
            "selected_routes": session.selected_routes,
            "has_query_artifact": session.query_artifact is not None,
            "has_result_artifact": session.result_artifact is not None,
        }

    def _inspect_decision(self, session: Session, decision_id: str) -> dict[str, Any]:
        """Return decision details."""
        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_INPUT,
                f"Decision {decision_id} not found or not current",
            )

        d = session.current_decision
        return {
            "id": d.id,
            "requirement_id": d.requirement_id,
            "options": [
                {
                    "id": o.id,
                    "meaning": o.meaning,
                    "route_id": o.route_id,
                    "evidence": o.evidence,
                }
                for o in d.options
            ],
            "more_retained": d.more_retained,
            "search_exhausted": d.search_exhausted,
            "can_expand": d.can_expand,
        }

    def _inspect_option(
        self,
        session: Session,
        decision_id: str,
        option_id: str,
    ) -> dict[str, Any]:
        """Return option details with evidence."""
        if not session.current_decision or session.current_decision.id != decision_id:
            return self._error_response(
                ErrorCode.INVALID_INPUT,
                f"Decision {decision_id} not found or not current",
            )

        option = next(
            (o for o in session.current_decision.options if o.id == option_id),
            None,
        )
        if not option:
            return self._error_response(
                ErrorCode.INVALID_INPUT,
                f"Option {option_id} not found",
            )

        # Get full route details if available
        route_details = None
        if option.route_id:
            for candidates in session.route_candidates.values():
                for c in candidates:
                    if c["id"] == option.route_id:
                        route_details = c
                        break

        return {
            "id": option.id,
            "meaning": option.meaning,
            "route_id": option.route_id,
            "evidence": option.evidence,
            "route_details": route_details,
        }

    def _inspect_requirement(self, session: Session, req_id: str) -> dict[str, Any]:
        """Return requirement details."""
        if req_id not in session.requirements:
            return self._error_response(
                ErrorCode.INVALID_INPUT,
                f"Requirement {req_id} not found",
            )

        req = session.requirements[req_id]
        return {
            "id": req_id,
            "op": req["op"],
            "resolved": req["resolved"],
            "clause": req["clause"],
        }
