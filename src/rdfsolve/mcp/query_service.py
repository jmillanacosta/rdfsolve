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
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal as TypingLiteral
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.plugins.sparql import prepareQuery
from rdfsolve.hydration import HydrationLimitError, _iri, _term
import re
from functools import wraps
from threading import RLock

from rdfsolve.sparql_helper import EndpointError

if TYPE_CHECKING:
    from rdfsolve.client_api import Client

logger = logging.getLogger(__name__)


def _locked(function):
    """Serialize mutations, including duplicate submissions during execution."""
    @wraps(function)
    def call(self, *args, **kwargs):
        with self._lock:
            return function(self, *args, **kwargs)
    return call


class _InputModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)


# State enumeration


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


# Intent grammar models


class RoleDef(_InputModel):
    """Role definition in intent."""

    id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    class_hint: str | None = Field(default=None, description="Class label or IRI")


class RDFTerm(_InputModel):
    """RDF term representation."""

    type: str = Field(pattern=r"^(iri|literal)$")
    value: str
    datatype: str | None = None
    language: str | None = None

    @model_validator(mode="after")
    def validate_term(self):
        if self.type == "iri":
            _iri(self.value)
            if self.datatype or self.language:
                raise ValueError("IRI terms cannot have a datatype or language")
        else:
            if self.datatype and self.language:
                raise ValueError("Use a datatype or a language, not both")
            if self.datatype:
                _iri(self.datatype)
            if self.language is not None and not re.fullmatch(r"[A-Za-z]+(?:-[A-Za-z0-9]+)*", self.language):
                raise ValueError("Invalid RDF language tag")
            value = Literal(self.value, datatype=self.datatype, lang=self.language, normalize=False)
            if value.ill_typed is True:
                raise ValueError("Invalid lexical value for the supplied RDF datatype")
        return self


class RelationRequirement(_InputModel):
    """Relation requirement in where clause."""

    id: str
    op: str = "relation"
    from_: str = Field(alias="from")
    to: str
    meaning: str
    via: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class BindRequirement(_InputModel):
    """Bind requirement in where clause."""

    id: str
    op: str = "bind"
    role: str
    term: RDFTerm


class CompareRequirement(_InputModel):
    """Compare requirement in where clause."""

    id: str
    op: str = "compare"
    role: str
    field: str
    operator: str = Field(pattern=r"^(eq|lt|le|gt|ge)$")
    term: RDFTerm


class DifferentRequirement(_InputModel):
    """Different requirement in where clause."""

    id: str
    op: str = "different"
    left: str
    right: str


class UnparsedRequirement(_InputModel):
    """Unparsed requirement that blocks readiness."""

    id: str
    text: str


# One source for leaf-shape requirements and advertised schema.
WHERE_REQUIRED = {
    "all": ("args",), "any": ("args",),
    "relation": ("id", "from_", "to", "meaning"),
    "bind": ("id", "role", "term"),
    "compare": ("id", "role", "field", "operator", "term"),
    "different": ("id", "left", "right"),
    "field": ("id", "role", "field", "to"),
}
WHERE_OPTIONAL = {"relation": {"via"}, "field": {"optional"}}
COMPARE_OPERATORS = ("eq", "lt", "le", "gt", "ge", "contains", "icontains")


class WhereClause(_InputModel):
    """Where clause expression tree."""

    op: TypingLiteral["all", "any", "relation", "bind", "compare", "different", "field"]
    args: list[WhereClause] = Field(default_factory=list)
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
    operator: TypingLiteral["eq", "lt", "le", "gt", "ge", "contains", "icontains"] | None = None
    optional: bool = False
    # For different
    left: str | None = None
    right: str | None = None

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_shape(self):
        required = WHERE_REQUIRED[self.op]
        allowed = {"op", *required, *WHERE_OPTIONAL.get(self.op, set())}
        for name in required:
            if getattr(self, name) in (None, ""):
                raise ValueError(f"{self.op} requires {name}")
        unexpected = {name for name in self.model_fields_set - allowed
                      if getattr(self, name) is not None and getattr(self, name) != []
                      and not (name == "optional" and getattr(self, name) is False)}
        if unexpected:
            raise ValueError(f"Fields not supported for {self.op}: {sorted(unexpected)}")
        if self.op == "any" and not self.args:
            raise ValueError("any requires at least one branch")
        if self.op == "compare" and self.operator in ("contains", "icontains"):
            if self.term.type != "literal" or self.term.datatype or self.term.language:
                raise ValueError("Text matching requires a plain literal search string")
        return self

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        schema = handler(core_schema)
        properties = schema.get("properties", {})
        branches = []
        for op, required_names in WHERE_REQUIRED.items():
            aliases = ["from" if x == "from_" else x for x in required_names]
            allowed = ["op", *aliases, *sorted(WHERE_OPTIONAL.get(op, set()))]
            props = {name: deepcopy(properties[name]) for name in allowed}
            props["op"] = {"const": op, "type": "string"}
            for name in aliases:
                spec = props[name]
                spec.pop("default", None)
                nonnull = [x for x in spec.get("anyOf", []) if x != {"type": "null"}]
                if len(nonnull) == 1:
                    props[name] = nonnull[0]
            if op == "any":
                props["args"]["minItems"] = 1
            branches.append({"type": "object", "properties": props,
                             "required": ["op", *aliases], "additionalProperties": False})
        return {"oneOf": branches, "title": "WhereClause"}


class Intent(_InputModel):
    """Typed intent for query_start."""

    roles: list[RoleDef] = Field(min_length=1, max_length=20)
    where: WhereClause
    select: list[str] = Field(min_length=1, max_length=20)
    distinct: bool = True
    unparsed_requirements: list[UnparsedRequirement] = Field(default_factory=list)



# Decision models


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


# Session and artifact models


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
class ExecutionArtifact:
    """Diagnostic record, never an invented successful result."""
    id: str
    query_artifact_id: str
    status: str
    strategy: dict[str, Any]
    queries: list[dict[str, Any]]
    error: str | None = None


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

    # A choice belongs to one requirement, not every route with matching endpoints.
    selected_by_requirement: dict[str, str] = field(default_factory=dict)
    class_bindings: dict[str, str] = field(default_factory=dict)
    blocked_message: str | None = None

    # Artifacts
    query_artifact: QueryArtifact | None = None
    result_artifact: ResultArtifact | None = None
    execution_artifact: ExecutionArtifact | None = None


# Query Service


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
        if any(type(x) is not int or x < 1 for x in (candidate_batch, display_window, total_ceiling)):
            raise ValueError("Candidate and display budgets must be positive integers")
        self._lock = RLock()
        self.client = client
        self.source_id = source_id or client._schema.about.dataset_name or "rdf"
        self.registry = client.registry(source_id=self.source_id)

        # Configuration
        self.candidate_batch = candidate_batch
        self.display_window = display_window
        self.total_ceiling = total_ceiling
        self.probes_enabled = probes_enabled

        self._operation_payloads: dict[tuple[str | None, str], str] = {}

        # Session storage
        self._sessions: dict[str, Session] = {}

        # Operation replay cache: (session_id, operation_id) -> (revision, response)
        self._operation_cache: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}

        # Artifact storage
        self._artifacts: dict[str, QueryArtifact | ResultArtifact | ExecutionArtifact] = {}
        self._artifact_owners: dict[str, str] = {}

        # Backend call counter for testing
        self._backend_calls: int = 0

        # Execution lock (for concurrent identical execution)
        self._executing: set[str] = set()

    # =========================================================================
    # Public operations
    # =========================================================================

    @_locked
    def query_start(self, operation_id: str, question: str, intent: dict[str, Any], scope: str | None = None) -> dict[str, Any]:
        """Ground and compile a new query. No final query executes here."""
        scope = scope or self.source_id
        if scope != self.source_id:
            return self._error_response(ErrorCode.INVALID_INPUT, "scope must match the configured source; graph scope comes from the client")
        if not isinstance(question, str) or not question.strip():
            return self._error_response(ErrorCode.INVALID_INPUT, "Supply the original question")
        try:
            key = self._operation_cache_key(None, operation_id, "start", {"question": question, "intent": intent, "scope": scope})
        except ValueError as exc:
            return self._error_response(ErrorCode.OPERATION_CONFLICT, str(exc))
        if key in self._operation_cache:
            return deepcopy(self._operation_cache[key][1])
        try:
            parsed = Intent.model_validate(intent)
            error = self._validate_intent(parsed)
            if error:
                raise ValueError(error)
        except (ValueError, TypeError) as exc:
            return self._error_response(ErrorCode.INVALID_INPUT, str(exc))
        session = Session(id=uuid4().hex[:16], question=question, intent=parsed, scope=scope)
        self._sessions[session.id] = session
        self._build_requirements(session)
        self._advance(session)
        return self._cache_response(key, session)


    @_locked
    def query_decide(self, session_id: str, revision: int, operation_id: str, action: dict[str, Any]) -> dict[str, Any]:
        if session_id not in self._sessions:
            return self._error_response(ErrorCode.UNKNOWN_SESSION, f"Unknown session: {session_id}")
        session = self._sessions[session_id]
        try:
            key = self._operation_cache_key(session_id, operation_id, "decide", {"revision": revision, "action": action})
        except ValueError as exc:
            return self._error_response(ErrorCode.OPERATION_CONFLICT, str(exc))
        if key in self._operation_cache:
            return deepcopy(self._operation_cache[key][1])
        if revision != session.revision:
            return self._error_response(ErrorCode.STALE_REVISION, f"Expected revision {session.revision}, got {revision}")
        handlers = {"choose": self._handle_choose, "more": self._handle_more, "reject": self._handle_reject,
                    "expand": self._handle_expand, "revise": self._handle_revise}
        handler = handlers.get(action.get("type"))
        if handler is None:
            return self._error_response(ErrorCode.INVALID_INPUT, "Unknown decision action")
        return handler(session, operation_id, action, key)


    @_locked
    def query_inspect(self, session_id: str | None = None, revision: int | None = None, target: str = "status") -> dict[str, Any]:
        """Inspect retained data. schema:<words> is available before starting a session."""
        if target.startswith("schema:"):
            text = target.partition(":")[2]
            cards = self.registry.find(text, types=True, limit=100)
            return {"source": self.source_id, "types": cards[:20], "more": len(cards) > 20}
        if target.startswith("type:"):
            identifier = target.partition(":")[2]
            matches = [x for x in self.registry.types if x.id == identifier]
            if len(matches) != 1:
                return self._error_response(ErrorCode.INVALID_INPUT, "Unknown type IRI; use schema:<words>")
            t = matches[0]
            return {"id": t.id, "label": t.label, "fields": [
                {"name": f.name, "label": f.label, "binding": f.binding, "description": f.description}
                for f in t.fields]}
        if session_id not in self._sessions:
            return self._error_response(ErrorCode.UNKNOWN_SESSION, f"Unknown session: {session_id}")
        session = self._sessions[session_id]
        if revision != session.revision:
            return self._error_response(ErrorCode.STALE_REVISION, f"Expected revision {session.revision}, got {revision}")
        if target == "status":
            return self._inspect_status(session)
        if target.startswith("decision:"):
            return self._inspect_decision(session, target.split(":", 1)[1])
        if target.startswith("option:"):
            parts = target.split(":")
            if len(parts) == 3:
                return self._inspect_option(session, parts[1], parts[2])
        if target.startswith("requirement:"):
            return self._inspect_requirement(session, target.split(":", 1)[1])
        return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown inspection target: {target}")


    @_locked
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
                return self._error_response(
                    ErrorCode.INVALID_INPUT, "execute requires operation_id"
                )
            return self._handle_execute(session, revision, operation_id)
        else:
            return self._error_response(
                ErrorCode.INVALID_INPUT, f"Unknown action type: {action_type}"
            )

    @_locked
    def read_artifact(self, artifact_id: str) -> dict[str, Any]:
        """Read a retained artifact. The caller receives an independent value."""
        if artifact_id not in self._artifacts:
            return self._error_response(ErrorCode.INVALID_INPUT, f"Unknown artifact: {artifact_id}")
        artifact = self._artifacts[artifact_id]
        if isinstance(artifact, QueryArtifact):
            result = {"type": "query", "id": artifact.id, "query": artifact.query,
                      "projection": artifact.projection, "scope": artifact.scope,
                      "schema_revision": artifact.schema_revision, "plan_revision": artifact.plan_revision,
                      "coverage": artifact.coverage}
        elif isinstance(artifact, ExecutionArtifact):
            result = {"type": "execution", **asdict(artifact)}
        else:
            result = {"type": "result", "id": artifact.id, "query_artifact_id": artifact.query_artifact_id,
                      "bindings": artifact.bindings, "row_count": artifact.row_count,
                      "retrieval": "see_execution_artifact", "endpoint_completeness": "unknown"}
        return deepcopy(result)

    @_locked
    def read_resource(self, uri: str) -> dict[str, Any]:
        match = re.fullmatch(r"rdfsolve://sessions/([a-f0-9]+)/artifacts/([A-Za-z0-9-]+)", uri)
        if not match or match[1] not in self._sessions:
            raise ValueError("Unknown rdfsolve resource URI")
        # Retained artifacts remain readable after revisions; URI ownership is checked.
        if self._artifact_owners.get(match[2]) != match[1]:
            raise ValueError("Artifact does not belong to this session")
        result = self.read_artifact(match[2])
        if "type" not in result and "error" in result:
            raise ValueError(result["error"]["message"])
        return result

    def _cache_response(self, key, session):
        response = self._observation(session)
        self._operation_cache[key] = (session.revision, deepcopy(response))
        return deepcopy(response)


    def _operation_cache_key(self, session_id, operation_id, op_type, payload):
        if not isinstance(operation_id, str) or not operation_id.strip():
            raise ValueError("operation_id must be a nonempty string")
        identity = (session_id, operation_id)
        fingerprint = json.dumps({"type": op_type, "payload": payload}, sort_keys=True)
        # Validation/state errors do not claim an operation ID. A corrected
        # submission may reuse it; an accepted/replayed operation remains immutable.
        previous = self._operation_payloads.get(identity) if identity in self._operation_cache else None
        if previous is not None and previous != fingerprint:
            raise ValueError("operation_id was already used with different arguments")
        self._operation_payloads[identity] = fingerprint
        return identity


    def _validate_intent(self, intent: Intent) -> str | None:
        role_ids = [r.id for r in intent.roles]
        if len(role_ids) != len(set(role_ids)):
            return "Role IDs must be unique"
        if len(intent.select) != len(set(intent.select)) or any(r not in role_ids for r in intent.select):
            return "select must contain unique, declared role IDs"
        error = self._validate_where_roles(intent.where, set(role_ids))
        if error:
            return error
        ids = set()
        error = self._collect_requirement_ids(intent.where, ids)
        if error:
            return error
        for item in intent.unparsed_requirements:
            if item.id in ids:
                return f"Duplicate requirement ID: {item.id}"
            ids.add(item.id)
        return None


    def _validate_where_roles(self, where: WhereClause, role_ids: set[str]) -> str | None:
        required = WHERE_REQUIRED
        optional = WHERE_OPTIONAL
        if where.op in ("all", "any"):
            if where.op == "any" and not where.args:
                return "any requires at least one branch"
            allowed = {"op", "args"}
        else:
            allowed = {"op", *required[where.op], *optional.get(where.op, set())}
            for name in required[where.op]:
                if getattr(where, name) in (None, ""):
                    return f"{where.op} requires {name}"
        unexpected = {name for name in where.model_fields_set - allowed
                      if getattr(where, name) is not None and getattr(where, name) != []
                      and not (name == "optional" and getattr(where, name) is False)}
        # Empty args is the model default, not an extra requested condition.
        if unexpected:
            return f"Fields not supported for {where.op}: {sorted(unexpected)}"
        if where.op in ("all", "any"):
            for child in where.args:
                error = self._validate_where_roles(child, role_ids)
                if error:
                    return error
        elif where.op == "relation":
            if any(r not in role_ids for r in [where.from_, where.to, *(where.via or [])]):
                return "relation references an unknown role"
        elif where.op in ("bind", "compare", "field"):
            if where.role not in role_ids:
                return f"{where.op} references an unknown role"
            if where.op == "compare" and where.operator not in COMPARE_OPERATORS:
                return "Unsupported comparison operator"
            if where.op == "field" and where.to not in role_ids:
                return "field references an unknown output role"
        elif where.left not in role_ids or where.right not in role_ids:
            return "different references an unknown role"
        return None


    def _collect_requirement_ids(self, where: WhereClause, req_ids: set[str]) -> str | None:
        if where.op in ("all", "any"):
            for child in where.args:
                error = self._collect_requirement_ids(child, req_ids)
                if error:
                    return error
        elif where.id in req_ids:
            return f"Duplicate requirement ID: {where.id}"
        else:
            req_ids.add(where.id)
        return None


    def _build_requirements(self, session: Session) -> None:
        """Build requirements from intent where clause."""
        self._extract_requirements(session, session.intent.where)

    def _extract_requirements(self, session: Session, where: WhereClause) -> None:
        if where.op in ("all", "any"):
            for child in where.args:
                self._extract_requirements(session, child)
        else:
            session.requirements[where.id] = {"op": where.op, "resolved": False,
                "clause": where.model_dump(by_alias=True, exclude_none=True)}


    def _advance(self, session: Session) -> None:
        session.blocked_reason = None
        session.blocked_message = None
        session.blocked_requirement_ids = []
        if session.intent.unparsed_requirements:
            self._block(session, BlockedReason.UNSUPPORTED_REQUIREMENT,
                        [r.id for r in session.intent.unparsed_requirements], "Unparsed requirements remain")
            return
        for role in session.intent.roles:
            if role.class_hint and self._resolve_class(session, role.id) is None:
                self._block(session, BlockedReason.UNSUPPORTED_REQUIREMENT, [],
                            f"Unknown or ambiguous class for role {role.id}: {role.class_hint}. Inspect schema:<words>.")
                return
        for req_id, req in session.requirements.items():
            if req["resolved"]:
                continue
            if req["op"] == "relation":
                clause = req["clause"]
                first = self._resolve_class(session, clause["from"])
                last = self._resolve_class(session, clause["to"])
                if not first or not last:
                    self._block(session, BlockedReason.UNSUPPORTED_REQUIREMENT, [req_id], "Relation endpoints require grounded classes")
                    return
                try:
                    routes = self._find_routes(first, last, clause["meaning"], clause.get("via", []))
                except HydrationLimitError as exc:
                    self._block(session, BlockedReason.CANDIDATE_BUDGET, [req_id], str(exc))
                    return
                except (ValueError, KeyError) as exc:
                    self._block(session, BlockedReason.UNSUPPORTED_REQUIREMENT, [req_id], str(exc))
                    return
                via_roles = clause.get("via", [])
                if via_roles:
                    via_classes = [self._resolve_class(session, r) for r in via_roles]
                    # Retain only routes with the ordered, explicit intermediate roles.
                    filtered = []
                    for candidate in routes:
                        nodes = [candidate["route"][0][0]] + [step[2] for step in candidate["route"]]
                        positions = []
                        start = 1
                        for cls in via_classes:
                            matches = [i for i in range(start, len(nodes)-1) if nodes[i] == cls]
                            if len(matches) != 1:
                                break
                            positions.append(matches[0]); start = matches[0] + 1
                        else:
                            candidate["via_positions"] = dict(zip(via_roles, positions))
                            filtered.append(candidate)
                    routes = filtered
                if not routes:
                    self._block(session, BlockedReason.SEARCH_EXHAUSTED, [req_id], "No route within the supported class-path search")
                    return
                session.route_candidates[req_id] = routes
                if len(routes) != 1:
                    self._create_decision(session, req_id, routes)
                    session.state = QueryState.CHOOSE
                    return
                session.selected_by_requirement[req_id] = routes[0]["id"]
                session.selected_routes.append(routes[0]["id"])
            req["resolved"] = True
            session.resolved_requirements.add(req_id)
        self._compile_query(session)

    def _block(self, session, reason, ids, message):
        session.state = QueryState.BLOCKED
        session.blocked_reason = reason
        session.blocked_requirement_ids = ids
        session.blocked_message = message
        session.current_decision = None


    def _resolve_class(self, session: Session, role_id: str) -> str | None:
        if role_id in session.class_bindings:
            return session.class_bindings[role_id]
        role = next((r for r in session.intent.roles if r.id == role_id), None)
        if not role or not role.class_hint:
            return None
        hint = role.class_hint.casefold().strip()
        matches = {t.id for t in self.registry.types if hint in (t.id.casefold(), t.label.casefold())}
        for name, model in self.client.models.items():
            if name.casefold() == hint:
                matches.add(str(model.rdf_class_iri))
        # Local graphs provide direct type evidence without an endpoint discovery call.
        if isinstance(self.client.source, Graph):
            classes = {str(t) for t in self.client.source.objects(None, RDF.type) if isinstance(t, URIRef)}
            matches.update(c for c in classes if c.casefold() == hint)
            matches.update(a.term_iri for a in self.client._schema.enrichment.labels
                           if a.term_iri in classes and a.text.value.casefold() == hint)
        if len(matches) == 1:
            result = next(iter(matches)); _iri(result)
            session.class_bindings[role_id] = result
            return result
        return None


    def _find_routes(self, from_class: str, to_class: str, meaning: str, via: list[str]) -> list[dict[str, Any]]:
        """Use the package's existing class-path enumerator; never turn overflow into absence."""
        table = self.client.paths_between(from_class, to_class, max_hops=3, max_paths=self.total_ceiling)
        return [{"id": self._route_id(route), "route": route, "description": self._route_description(route),
                 "hops": len(route), "meaning": meaning}
                for route in table.attrs.get("routes", [])]


    def _route_id(self, route: list) -> str:
        """Generate stable route ID."""
        key = json.dumps(route)
        return "route-" + hashlib.sha256(key.encode()).hexdigest()[:16]

    def _route_description(self, route: list) -> str:
        """Generate human-readable route description."""
        parts = []
        for _s, p, _o, inverse in route:
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

    def _create_decision(self, session: Session, req_id: str, routes: list[dict[str, Any]]) -> None:
        window = routes[:self.display_window]
        session.displayed_options.setdefault(req_id, [])
        session.displayed_options[req_id].extend(r["id"] for r in window if r["id"] not in session.displayed_options[req_id])
        session.current_decision = Decision(
            id="d-" + uuid4().hex[:12], requirement_id=req_id,
            options=[DecisionOption(id="o-" + r["id"].removeprefix("route-"), meaning=r["description"], route_id=r["id"],
                                    evidence={"basis": "retained class patterns", "predicates": [step[1] for step in r["route"]]}) for r in window],
            more_retained=len(routes) > self.display_window, search_exhausted=True, can_expand=False)


    def _compile_query(self, session: Session) -> None:
        """Lower the actual expression tree and validate the exact emitted SELECT."""
        self._allocated = {r.id for r in session.intent.roles}
        coverage = {}
        try:
            def typed(role_id):
                cls = self._resolve_class(session, role_id)
                return f"?{role_id} a {_iri(cls)} ." if cls else ""

            def lower(node):
                if node.op == "all":
                    parts = [lower(child) for child in node.args]
                    return " ".join(part for part in parts if part)
                if node.op == "any":
                    return "{ " + " UNION ".join("{ " + lower(child) + " }" for child in node.args) + " }"
                if node.op == "relation":
                    route_id = session.selected_by_requirement.get(node.id)
                    candidate = next((r for r in session.route_candidates.get(node.id, []) if r["id"] == route_id), None)
                    if candidate is None:
                        raise ValueError(f"No selected route for requirement {node.id}")
                    body = " . ".join(self._route_to_patterns(session, node.id, candidate["route"])) + " ."
                elif node.op == "bind":
                    body = f"VALUES ?{node.role} {{ {self._term_sparql(node.term)} }} " + typed(node.role)
                elif node.op == "field":
                    predicate = self._resolve_field(session, node.role, node.field)
                    pattern = f"?{node.role} {_iri(predicate)} ?{node.to} . " + typed(node.to)
                    body = typed(node.role) + " " + (
                        "OPTIONAL { " + pattern + " }" if node.optional else pattern)
                elif node.op == "compare":
                    predicate = self._resolve_field(session, node.role, node.field)
                    variable = self._new_variable()
                    term = self._term_sparql(node.term)
                    if node.operator in ("contains", "icontains"):
                        left, right = f"STR(?{variable})", f"STR({term})"
                        if node.operator == "icontains":
                            left, right = f"LCASE({left})", f"LCASE({right})"
                        expression = f"isLiteral(?{variable}) && CONTAINS({left}, {right})"
                    else:
                        operator = {"eq": "=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[node.operator]
                        expression = f"?{variable} {operator} {term}"
                    body = (typed(node.role) + f" ?{node.role} {_iri(predicate)} ?{variable} . "
                            + f"FILTER({expression})")
                else:
                    body = typed(node.left) + " " + typed(node.right) + f" FILTER(?{node.left} != ?{node.right})"
                coverage[node.id] = [body]
                return body

            body = lower(session.intent.where)
            # A standalone class listing needs a real graph pattern. In alternatives,
            # class patterns for referenced roles stay inside their own branches.
            referenced = set()
            for req in session.requirements.values():
                c = req["clause"]
                referenced.update(c[k] for k in ("from", "to", "role", "left", "right") if c.get(k))
                referenced.update(c.get("via", []))
            for role in session.intent.roles:
                if role.id not in referenced:
                    if not role.class_hint:
                        raise ValueError(f"Role {role.id} is neither typed nor bound")
                    body += " " + typed(role.id)
            if self.client.graph_uris:
                graph_var = self._new_variable()
                graphs = " ".join(_iri(g) for g in self.client.graph_uris)
                body = f"VALUES ?{graph_var} {{ {graphs} }} GRAPH ?{graph_var} {{ {body} }}"
            projection = {role: role for role in session.intent.select}
            query = ("SELECT " + ("DISTINCT " if session.intent.distinct else "")
                     + " ".join("?" + v for v in projection.values()) + " WHERE { " + body + " }")
            prepareQuery(query)
        except (ValueError, KeyError, TypeError) as exc:
            session.query_artifact = None
            missing = [r for r in session.requirements if r not in coverage]
            for req_id in missing:
                session.requirements[req_id]["resolved"] = False
                session.resolved_requirements.discard(req_id)
            self._block(session, BlockedReason.UNSUPPORTED_REQUIREMENT, missing, str(exc))
            return
        artifact = QueryArtifact(id="query-" + uuid4().hex[:12], query=query, projection=projection,
                                 scope=session.scope, schema_revision=self.registry.revision,
                                 plan_revision=session.revision, coverage=coverage)
        self._artifacts[artifact.id] = artifact
        self._artifact_owners[artifact.id] = session.id
        session.query_artifact = artifact
        session.state = QueryState.READY

    def _new_variable(self):
        i = 0
        while f"_rs{i}" in self._allocated:
            i += 1
        value = f"_rs{i}"
        self._allocated.add(value)
        return value

    @staticmethod
    def _term_sparql(term: RDFTerm) -> str:
        if term.type == "iri":
            return _iri(term.value)
        return Literal(term.value, datatype=term.datatype, lang=term.language, normalize=False).n3()

    def _resolve_field(self, session, role_id, hint):
        cls = self._resolve_class(session, role_id)
        matches = set()
        for typ in self.registry.types:
            if typ.id != cls:
                continue
            for f in typ.fields:
                path = f.binding.get("path", {})
                if path.get("operator") == "predicate" and hint.casefold() in {
                    f.name.casefold(), f.label.casefold(), path.get("iri", "").casefold()}:
                    matches.add(path["iri"])
        # Local observations and retained predicate descriptions are also grounded evidence.
        known = {p.property_uri for p in self.client._schema.patterns}
        if isinstance(self.client.source, Graph):
            known.update(str(p) for p in self.client.source.predicates())
        if hint in known:
            matches.add(hint)
        matches.update(a.term_iri for a in self.client._schema.enrichment.labels
                       if a.term_iri in known and a.text.value.casefold() == hint.casefold())
        if len(matches) != 1:
            raise ValueError(f"Unknown or ambiguous field {hint!r} for role {role_id}; inspect type:{cls}")
        return next(iter(matches))


    def _route_to_patterns(self, session: Session, req_id: str, route: list) -> list[str]:
        clause = session.requirements[req_id]["clause"]
        first, last = clause["from"], clause["to"]
        selected = session.selected_by_requirement[req_id]
        candidate = next(c for c in session.route_candidates[req_id] if c["id"] == selected)
        via = {position: role for role, position in candidate.get("via_positions", {}).items()}
        variables = [first] + [via.get(i) or self._new_variable() for i in range(1, len(route))] + [last]
        patterns = []
        for i, (start, predicate, end, inverse) in enumerate(route):
            if i == 0:
                patterns.append(f"?{variables[0]} a {_iri(start)}")
            left, right = (variables[i+1], variables[i]) if inverse else (variables[i], variables[i+1])
            patterns.append(f"?{left} {_iri(predicate)} ?{right}")
            patterns.append(f"?{variables[i+1]} a {_iri(end)}")
        return list(dict.fromkeys(patterns))


    def _role_variable(self, session: Session, role_id: str) -> str:
        """Get SPARQL variable name for a role."""
        # Simple mapping - role_id becomes variable name
        return role_id

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
                "options": [{"id": o.id, "meaning": o.meaning} for o in d.options],
                "more_retained": d.more_retained,
                "search_exhausted": d.search_exhausted,
                "can_expand": d.can_expand,
            }

        if session.state == QueryState.BLOCKED:
            result["reason"] = session.blocked_reason.value if session.blocked_reason else None
            result["blocked_requirement_ids"] = session.blocked_requirement_ids
            result["message"] = session.blocked_message

        if session.query_artifact:
            result["query_ref"] = (
                f"rdfsolve://sessions/{session.id}/artifacts/{session.query_artifact.id}"
            )

        if session.state == QueryState.COMPLETE:
            result["execution"] = "complete"
            if session.result_artifact:
                result["result_ref"] = (
                    f"rdfsolve://sessions/{session.id}/artifacts/{session.result_artifact.id}"
                )
                result["rows"] = session.result_artifact.row_count
                result["preview"] = deepcopy(session.result_artifact.bindings[:5])
                result["preview_truncated"] = session.result_artifact.row_count > 5
                result["endpoint_completeness"] = "unknown"

        if session.execution_artifact:
            result["execution_ref"] = f"rdfsolve://sessions/{session.id}/artifacts/{session.execution_artifact.id}"
            if session.state == QueryState.FAILED:
                result["execution"] = "failed"
                result["error"] = {"code": "backend_unavailable", "message": session.execution_artifact.error}

        # Always include unresolved requirements
        unresolved = [req_id for req_id, req in session.requirements.items() if not req["resolved"]]
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
        session.selected_by_requirement[req_id] = option.route_id
        session.requirements[req_id]["resolved"] = True
        session.resolved_requirements.add(req_id)
        session.decisions_made[decision_id] = option_id
        session.current_decision = None

        # Increment revision
        session.revision += 1

        # Advance to next decision or ready
        self._advance(session)

        response = self._observation(session)
        self._operation_cache[cache_key] = (session.revision, deepcopy(response))
        return response

    def _handle_more(self, session, operation_id, action, cache_key):
        decision = session.current_decision
        if not decision or decision.id != action.get("decision"):
            return self._error_response(ErrorCode.INVALID_OPTION, "Decision is not current")
        req_id = decision.requirement_id
        seen = set(session.displayed_options.get(req_id, []))
        rejected = session.rejected_options.get(req_id, set())
        candidates = [r for r in session.route_candidates[req_id] if r["id"] not in seen | rejected]
        if candidates:
            self._create_decision(session, req_id, candidates)
            session.revision += 1
        return self._cache_response(cache_key, session)


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
        self._operation_cache[cache_key] = (session.revision, deepcopy(response))
        return response

    def _handle_expand(self, session, operation_id, action, cache_key):
        return self._error_response(ErrorCode.INVALID_INPUT,
            "Search has already enumerated the configured hop/candidate scope. Use more for retained candidates; increase total_ceiling to search beyond a budget blocker.")


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
        session.selected_by_requirement = {}
        session.class_bindings = {}
        session.route_candidates = {}
        session.displayed_options = {}
        session.rejected_options = {}
        session.query_artifact = None
        session.result_artifact = None
        session.execution_artifact = None
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
        self._operation_cache[cache_key] = (session.revision, deepcopy(response))
        return response

    def _handle_emit(self, session: Session, revision: int) -> dict[str, Any]:
        if revision != session.revision:
            return self._error_response(ErrorCode.STALE_REVISION, f"Expected revision {session.revision}, got {revision}")
        if session.state not in (QueryState.READY, QueryState.COMPLETE, QueryState.FAILED) or session.query_artifact is None:
            return self._error_response(ErrorCode.QUERY_NOT_READY, "No compiled query is ready")
        return {"session": session.id, "revision": session.revision, "state": session.state.value,
                "query_ref": f"rdfsolve://sessions/{session.id}/artifacts/{session.query_artifact.id}",
                "query": session.query_artifact.query}


    def _handle_execute(self, session: Session, revision: int, operation_id: str) -> dict[str, Any]:
        try:
            key = self._operation_cache_key(session.id, operation_id, "execute", {"revision": revision})
        except ValueError as exc:
            return self._error_response(ErrorCode.OPERATION_CONFLICT, str(exc))
        if key in self._operation_cache:
            return deepcopy(self._operation_cache[key][1])
        if revision != session.revision:
            return self._error_response(ErrorCode.STALE_REVISION, f"Expected revision {session.revision}, got {revision}")
        if session.state not in (QueryState.READY, QueryState.COMPLETE, QueryState.FAILED) or session.query_artifact is None:
            return self._error_response(ErrorCode.QUERY_NOT_READY, "No compiled query is ready")
        record_start = len(self.client._records())
        error_message = None
        try:
            self._backend_calls += 1
            bindings = self.client._select(session.query_artifact.query)
            for row in bindings:
                for term in row.values():
                    _term(term)
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
        records = [{k: v for k, v in asdict(record).items() if k != "result"}
                   for record in self.client._records()[record_start:]]
        execution = ExecutionArtifact(
            id="execution-" + uuid4().hex[:12], query_artifact_id=session.query_artifact.id,
            status="failed" if error_message else "complete",
            strategy=deepcopy(self.client.last_query_execution), queries=records, error=error_message)
        self._artifacts[execution.id] = execution
        self._artifact_owners[execution.id] = session.id
        session.execution_artifact = execution
        if error_message:
            session.result_artifact = None
            session.state = QueryState.FAILED
            session.revision += 1
            return self._cache_response(key, session)
        artifact = ResultArtifact(id="result-" + uuid4().hex[:12],
                                  query_artifact_id=session.query_artifact.id,
                                  bindings=deepcopy(bindings), row_count=len(bindings))
        self._artifacts[artifact.id] = artifact
        self._artifact_owners[artifact.id] = session.id
        session.result_artifact = artifact
        session.state = QueryState.COMPLETE
        session.revision += 1
        return self._cache_response(key, session)


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