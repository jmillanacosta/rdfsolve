"""Check retrieval queries against grounded question clauses."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from functools import lru_cache
from typing import TYPE_CHECKING, Any
from typing import Literal as Choice

from pydantic import BaseModel, ConfigDict, Field, model_validator
from rdflib import RDF, Literal, URIRef, Variable
from rdflib.paths import Path
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.client.query_fragments import PreparedQuery, compile_query, walk
from rdfsolve.schema_models.exporters.paths import path_to_sparql

if TYPE_CHECKING:
    from rdflib.term import Node

    from rdfsolve.client.catalogue import Catalogue


class QueryValidationError(ValueError):
    """A query failed an identified retrieval requirement."""

    def __init__(self, code: str, message: str):
        """Associate a stable error code with a concrete query failure."""
        self.code = code
        self.goal = ""
        self.requirement: dict[str, Any] = {}
        self.evidence: list[str] = []
        super().__init__(message)


class Requirement(BaseModel):
    """An atomic semantic commitment made before query construction."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    clause: str
    kind: Choice["output", "entity_filter", "text_filter", "relation", "scope"] = Field(
        description="output lists resources or field values; entity_filter restricts to a particular named member; text_filter matches literal wording; relation requires a connection; scope names the configured database."
    )
    concept: str = Field(
        description="Short class, field or relationship name. Keep the full qualification in clause and a named restriction in value."
    )
    binding: str = Field(
        default="", description="Requested output column, separate from its meaning."
    )
    owner: str = Field(
        default="",
        description="Subject variable or class for a field/relationship, e.g. project. Omit for a resource output.",
    )
    target: str = Field(
        default="",
        description="For a relation: its target role or class, including unprojected roles.",
    )
    value: str = Field(
        default="",
        description="For filters only: the particular name or text requested, such as Alice. Listing all class members requires an output goal with no value.",
    )
    required: bool = Field(
        default=True,
        description="False for available metadata or optional relationships; true for required outputs and filters.",
    )

    @model_validator(mode="after")
    def valid_request(self) -> Requirement:
        """Reject incomplete interpretations before retaining any commitment."""
        if not self.clause.strip() or not self.concept.strip():
            raise ValueError("Each goal needs its original clause and a concept.")
        if self.kind in {"entity_filter", "text_filter"} and not self.value.strip():
            raise ValueError(
                "A named-value restriction needs value. Use output for listing resources or fields."
            )
        if self.kind in {"entity_filter", "text_filter"} and not self.required:
            raise ValueError(
                "Filters are required constraints. Use optional outputs for available metadata."
            )
        return self


def validate_retrieval(query: PreparedQuery, catalogue: Catalogue) -> list[str]:
    """Check the supported SELECT algebra, paths, owners and term constraints."""
    algebra = prepareQuery(query.sparql).algebra
    nodes = list(walk(algebra))
    forbidden = {
        "Extend",
        "AggregateJoin",
        "Group",
        "Minus",
        "Reduced",
        "Slice",
        "Builtin_EXISTS",
        "Builtin_NOTEXISTS",
    }
    if sum(isinstance(n, CompValue) and n.name == "Project" for n in nodes) > 1:
        raise QueryValidationError(
            "retrieval_operator",
            "Nested SELECT scopes need separate goal validation. Use a single retrieval projection.",
        )
    for node in nodes:
        if isinstance(node, CompValue) and (
            node.name in forbidden or node.name.startswith("Aggregate_")
        ):
            raise QueryValidationError(
                "retrieval_operator",
                f"{node.name} is outside the supported retrieval subset. Retrieve RDF bindings and apply subsequent transformations in Python.",
            )
    triples = [
        triple
        for node in nodes
        if isinstance(node, CompValue) and node.name == "BGP"
        for triple in node.triples
    ]
    if not triples:
        raise QueryValidationError(
            "empty_pattern", "A retrieval query needs a grounded RDF pattern."
        )
    types: dict[Node, set[str]] = {}
    for subject, predicate, obj in triples:
        if predicate == RDF.type and isinstance(obj, URIRef):
            types.setdefault(subject, set()).add(str(obj))
    fields = [f for f in catalogue.fragments.values() if f.kind == "field"]
    supported: set[tuple[Node, Node, Node]] = set()
    for use in query.uses:
        if use.get("pattern"):
            fragment_algebra = prepareQuery("SELECT * WHERE {" + use["pattern"] + "}").algebra
            supported.update(
                t
                for n in walk(fragment_algebra)
                if isinstance(n, CompValue) and n.name == "BGP"
                for t in n.triples
            )
    paths = {path_to_sparql(f.path): f for f in catalogue.fragments.values() if f.path}
    bound_terms: dict[Variable, set[Node]] = {}
    for node in nodes:
        if isinstance(node, CompValue) and node.name == "values":
            for row in node.res:
                for variable, term in row.items():
                    bound_terms.setdefault(variable, set()).add(term)
    warnings = []
    edges = []
    for subject, predicate, obj in triples:
        if predicate == RDF.type:
            if isinstance(obj, Variable) and (subject, predicate, obj) in supported:
                continue
            if not isinstance(obj, URIRef) or str(obj) not in catalogue.type_refs:
                raise QueryValidationError(
                    "class_binding", "Use a retained class for each rdf:type constraint."
                )
            continue
        if isinstance(predicate, Variable):
            raise QueryValidationError(
                "variable_predicate", "Discover a field or path before using its predicate."
            )
        candidates = [
            f
            for f in fields
            if f.path is not None
            and f.path.operator == "predicate"
            and str(predicate) == f.path.iri
        ]
        if isinstance(predicate, Path):
            candidates = [
                f
                for f in fields
                if f.path is not None
                and canonical_path(predicate.n3()) == canonical_path(path_to_sparql(f.path))
            ]
            candidates += [
                f
                for key, f in paths.items()
                if canonical_path(predicate.n3()) == canonical_path(key)
            ]
        if candidates:
            owner_types = types.get(subject, set())
            matched = [
                f for f in candidates if not f.owner or not owner_types or f.owner in owner_types
            ]
            if not matched:
                raise QueryValidationError(
                    "field_owner",
                    f"{predicate.n3()} belongs to a different class than {subject.n3()}. Inspect the field owner.",
                )
            metadata = [
                catalogue.metadata.get(
                    catalogue.field_refs.get((f.owner or "", f.field_name or ""), ""), {}
                )
                for f in matched
            ]
            kinds = {k for m in metadata for k in m.get("node_kinds", [])}
            terms = bound_terms.get(obj, {obj}) if isinstance(obj, Variable) else {obj}
            for term in terms:
                if isinstance(term, URIRef) and kinds and not kinds.intersection({"iri", "uri"}):
                    raise QueryValidationError(
                        "field_term_kind",
                        f"{predicate.n3()} has literal-valued evidence. Use a resource-valued relationship for an entity restriction.",
                    )
            if isinstance(obj, Literal) and kinds and "literal" not in kinds:
                raise QueryValidationError(
                    "field_term_kind",
                    f"{predicate.n3()} needs a resource; ground the entity first.",
                )
            datatypes = {d for m in metadata for d in m.get("datatypes", [])}
            if (
                isinstance(obj, Literal)
                and obj.datatype
                and datatypes
                and str(obj.datatype) not in datatypes
            ):
                raise QueryValidationError(
                    "field_datatype", f"{predicate.n3()} has incompatible datatype {obj.datatype}."
                )
            targets = {t for m in metadata for t in m.get("targets", [])}
            if targets and types.get(obj) and not targets.intersection(types[obj]):
                warnings.append(
                    f"Target class on {obj.n3()} lacks support in the retained field evidence."
                )
        elif isinstance(predicate, Path):
            raise QueryValidationError(
                "unknown_path",
                "Use a retained path; this compound traversal has no package evidence.",
            )
        elif (subject, predicate, obj) not in supported:
            raise QueryValidationError(
                "unknown_field", f"No retained field supports {predicate.n3()}."
            )
        if isinstance(subject, Variable) and isinstance(obj, Variable):
            edges.append((subject, obj))
    resources = {s for s, _, _ in triples if isinstance(s, Variable)}
    resources |= {v for v in types if isinstance(v, Variable)}
    if resources:
        reached = {next(iter(resources))}
        while True:
            expanded = reached | {v for edge in edges if reached.intersection(edge) for v in edge}
            if expanded == reached:
                break
            reached = expanded
        if not resources <= reached:
            raise QueryValidationError(
                "disconnected_roles",
                "Required resources are disconnected. Join their retained paths on shared bindings.",
            )
    _optional_scope(algebra)
    return list(dict.fromkeys(warnings))


@lru_cache(maxsize=512)
def canonical_path(text: str) -> str:
    """Normalize grouping in a retained path for the RDFLib serializer."""
    path = prepareQuery(f"SELECT ?s ?o WHERE {{ ?s {text} ?o }}").algebra
    return str(
        next(
            n.triples[0][1] for n in walk(path) if isinstance(n, CompValue) and n.name == "BGP"
        ).n3()
    )


def _optional_scope(node: Any) -> tuple[set[Variable], set[Variable]]:
    if not isinstance(node, CompValue):
        return set(), set()
    if node.name == "BGP":
        variables = {v for triple in node.triples for v in triple if isinstance(v, Variable)}
        return variables, set()
    if node.name in {"Join", "LeftJoin", "Union"}:
        left, left_optional = _optional_scope(node.p1)
        right, right_optional = _optional_scope(node.p2)
        if node.name == "Join" and ((left_optional & right) or (right_optional & left)):
            raise QueryValidationError(
                "optional_rebinding",
                "A dependent field can rebind a missing optional parent. Nest it inside that OPTIONAL.",
            )
        if node.name == "LeftJoin":
            unsafe = left_optional & right
            if unsafe and not (left & right):
                raise QueryValidationError(
                    "optional_rebinding", "Nest dependent metadata inside its parent OPTIONAL."
                )
            return left, left_optional | right_optional | (right - left)
        if node.name == "Union":
            return left & right, left_optional | right_optional | (left ^ right)
        return left | right, left_optional | right_optional
    return _optional_scope(node.get("p")) if "p" in node else (set(), set())


def validate_outputs(variables: Iterable[object], expected: Iterable[object]) -> None:
    """Require the caller's requested columns before final execution."""
    missing = sorted({str(v).lstrip("?$") for v in expected} - {str(v) for v in variables})
    if missing:
        raise QueryValidationError(
            "missing_outputs", f"Project the requested output variables: {', '.join(missing)}."
        )


def verify_query(
    sparql: str,
    requirements: Mapping[str, Requirement],
    grounding: Mapping[str, dict[str, Any]],
    catalogue: Catalogue,
    *,
    output_variables: Iterable[str] = (),
) -> PreparedQuery:
    """Expand one SELECT and check its declared semantic witnesses."""
    query = compile_query(
        sparql, catalogue.fragments, catalogue.client._scope, catalogue.known_iris
    )
    validate_outputs(query.variables, output_variables)
    query.warnings.extend(validate_retrieval(query, catalogue))
    resolved: dict[str, dict[str, Any]] = {}
    for key, requirement in requirements.items():
        if requirement.kind != "scope":
            selected = grounding.get(key, {})
            try:
                if not selected.get("evidence") or (
                    requirement.kind == "entity_filter" and not selected.get("entity")
                ):
                    selected = infer_grounding(requirement, query, catalogue, selected)
                validate_goal(requirement, selected, query, catalogue)
            except QueryValidationError as exc:
                exc.goal = key
                exc.requirement = requirement.model_dump()
                raise
            resolved[key] = selected
    query.diagnostics["grounding"] = resolved
    query.diagnostics["validation"] = [
        *(["requested output columns"] if output_variables else []),
        *(["selected goal witnesses"] if resolved else []),
        "field ownership",
        "binding scope",
        "retained paths",
        "retrieval algebra",
    ]
    return query


def infer_grounding(
    requirement: Requirement,
    query: PreparedQuery,
    catalogue: Catalogue,
    choice: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a goal from actual query witnesses and generated metadata."""
    from rdfsolve.client.catalogue import score, words

    choice = choice or {}
    nodes = list(walk(prepareQuery(query.sparql).algebra))
    triples = [t for n in nodes if isinstance(n, CompValue) and n.name == "BGP" for t in n.triples]
    used = {u["ref"] for u in query.uses}
    types: dict[Node, set[str]] = {}
    for subject, predicate, obj in triples:
        if predicate == RDF.type:
            types.setdefault(subject, set()).add(str(obj))
    candidates: list[tuple[tuple[float, float], str]] = []
    owner: str | None = None
    subject_binding: Variable | None = None
    if requirement.owner:
        try:
            owner = catalogue._type(requirement.owner)
        except ValueError:
            variable = Variable(requirement.owner.lstrip("?$"))
            bound_subjects = {s for s, _, _ in triples if isinstance(s, Variable)}
            subject_binding = variable
            if variable not in bound_subjects:
                raise QueryValidationError(
                    "goal_owner",
                    f"{requirement.clause}: owner '{requirement.owner}' is absent. The query has subject bindings {sorted(str(s) for s in bound_subjects)}. Use the intended subject or correct its retained owner.",
                ) from None
    for ref in catalogue.schema_documents:
        fragment = catalogue.fragments[ref]
        if requirement.kind == "relation" and fragment.kind == "type":
            continue
        if owner and fragment.owner and fragment.owner != owner:
            continue
        subjects = [
            s
            for s, p, o in triples
            if (fragment.kind == "type" and p == RDF.type and str(o) == fragment.iri)
            or (
                fragment.path
                and canonical_path(p.n3()) == canonical_path(path_to_sparql(fragment.path))
            )
        ]
        if subject_binding is not None and fragment.kind == "field":
            subjects = [s for s in subjects if s == subject_binding]
            if not subjects:
                continue
        if (
            fragment.owner
            and subjects
            and all(types.get(s) and fragment.owner not in types[s] for s in subjects)
        ):
            continue
        present = ref in used or bool(subjects)
        if not present or (
            requirement.kind in {"entity_filter", "text_filter"} and fragment.kind != "field"
        ):
            continue
        rank = catalogue.relevance(ref, requirement.concept)
        if (
            requirement.kind == "output"
            and fragment.kind in {"type", "field"}
            and words(fragment.label) == words(requirement.concept)
        ):
            rank += 5
        if ref == requirement.concept:
            rank = 10
        if choice.get("evidence"):
            if ref in choice["evidence"]:
                candidates.append(((10, 10), ref))
        elif rank >= 1:
            candidates.append(((score(fragment.label, requirement.concept), rank), ref))
    entities = [
        ref
        for ref, fragment in catalogue.fragments.items()
        if fragment.term
        and (
            ref == choice.get("entity")
            or (
                not choice.get("entity")
                and (
                    requirement.value.casefold()
                    in catalogue.metadata.get(ref, {}).get("lookups", set())
                    or requirement.value.casefold()
                    in {fragment.label.casefold(), fragment.term.value.casefold()}
                )
            )
        )
        and fragment.term.to_rdf() in [n for n in nodes if isinstance(n, (URIRef, Literal))]
    ]
    if requirement.kind != "entity_filter":
        entities = [""]
    accepted: list[tuple[tuple[float, float], dict[str, Any]]] = []
    failures: list[QueryValidationError] = []
    for ranking, ref in sorted(candidates, reverse=True):
        if accepted and ranking < accepted[0][0]:
            break
        for entity in entities:
            selected = {**choice, "evidence": [ref], "entity": entity}
            before = len(query.warnings)
            try:
                validate_goal(requirement, selected, query, catalogue)
                accepted.append((ranking, selected))
            except QueryValidationError as exc:
                exc.evidence = [ref]
                failures.append(exc)
            finally:
                del query.warnings[before:]
    if len(accepted) == 1:
        return accepted[0][1]
    if accepted:
        choices = [item[1] for item in accepted[:4]]
        raise QueryValidationError(
            "goal_choice", f"{requirement.clause}: choose the intended retained evidence: {choices}"
        )
    if failures and failures[0].code in {
        "goal_availability",
        "goal_optional",
        "goal_owner",
        "goal_type",
    }:
        raise failures[0]
    detail = (
        str(failures[0])
        if failures
        else "No grounded field or class explains this clause. Inspect its owner and select the relevant evidence."
    )
    raise QueryValidationError(
        "unresolved_goals", f"Ground every active clause. {requirement.clause}: {detail}"
    )


def required_nodes(value: Any) -> Iterator[Any]:
    """Traverse patterns that hold for every solution."""
    yield value
    if isinstance(value, CompValue) and value.name == "LeftJoin":
        yield from required_nodes(value.p1)
    elif isinstance(value, CompValue) and value.name == "Union":
        return
    elif isinstance(value, dict):
        for key, item in value.items():
            if not key.startswith("_"):
                yield from required_nodes(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from required_nodes(item)


def validate_goal(
    requirement: Requirement,
    grounding: Mapping[str, Any],
    expanded: PreparedQuery,
    catalogue: Catalogue,
) -> None:
    """Require RDF witnesses for selected outputs, relations and restrictions."""
    evidence = [catalogue.type_refs.get(ref, ref) for ref in grounding.get("evidence", [])]
    fragments = [catalogue.fragments[ref] for ref in evidence if ref in catalogue.fragments]
    if not fragments or len(fragments) != len(evidence):
        raise QueryValidationError(
            "goal_evidence",
            f"{requirement.clause}: select retained class, field or path references as evidence.",
        )
    if requirement.kind == "relation" and any(f.kind == "type" for f in fragments):
        raise QueryValidationError(
            "goal_evidence",
            f"{requirement.clause}: select a field or path connecting roles. A class alone does not establish the relationship.",
        )
    from rdfsolve.client.catalogue import words

    exact_types = {
        f.iri
        for f in catalogue.fragments.values()
        if f.kind == "type" and words(f.label) == words(requirement.concept)
    }
    if any(f.kind == "field" and words(f.label) == words(requirement.concept) for f in fragments):
        exact_types = set()
    if exact_types and not any(
        f.iri in exact_types
        or any(t in exact_types for t in catalogue.metadata.get(ref, {}).get("targets", []))
        for ref, f in zip(evidence, fragments, strict=True)
    ):
        raise QueryValidationError(
            "goal_concept",
            f"{requirement.clause}: select evidence for the discovered {requirement.concept} class.",
        )
    if not any(
        catalogue.relevance(ref, requirement.concept) >= 1
        for ref, f in zip(evidence, fragments, strict=True)
    ):
        raise QueryValidationError(
            "unexplained_grounding",
            f"{requirement.clause}: the retained metadata for {', '.join(f.label for f in fragments)} does not explain '{requirement.concept}'. Retrieve relevant field definitions or mapping evidence; this choice remains unresolved.",
        )
    algebra = prepareQuery(expanded.sparql).algebra
    nodes = list(required_nodes(algebra) if requirement.required else walk(algebra))
    triples = [t for n in nodes if isinstance(n, CompValue) and n.name == "BGP" for t in n.triples]
    types: dict[Node, set[str]] = {}
    for n in walk(algebra):
        if isinstance(n, CompValue) and n.name == "BGP":
            for subject, predicate, obj in n.triples:
                if predicate == RDF.type:
                    types.setdefault(subject, set()).add(str(obj))
    if (
        requirement.binding
        and grounding.get("project")
        and grounding["project"] != [requirement.binding]
    ):
        raise QueryValidationError(
            "goal_output", f"{requirement.clause}: preserve output binding {requirement.binding}."
        )
    selected_outputs = (
        [requirement.binding]
        if requirement.binding
        else (grounding.get("project") or expanded.variables)
    )
    outputs = {Variable(v.lstrip("?$")) for v in selected_outputs}
    if not outputs.issubset({Variable(v) for v in expanded.variables}):
        raise QueryValidationError(
            "goal_output",
            f"{requirement.clause}: declared output bindings are missing from SELECT.",
        )
    if (
        requirement.kind == "output"
        and exact_types
        and not any(types.get(v, set()).intersection(exact_types) for v in outputs)
    ):
        raise QueryValidationError(
            "goal_output",
            f"{requirement.clause}: project the actual typed resource identity. A field's target hint does not establish that its values are the requested resource.",
        )
    owner, subject_binding = None, None
    if requirement.owner and any(f.path for f in fragments):
        variable = Variable(requirement.owner.lstrip("?$"))
        if variable in {
            s
            for n in walk(algebra)
            if isinstance(n, CompValue) and n.name == "BGP"
            for s, _, _ in n.triples
        }:
            subject_binding = Variable(requirement.owner.lstrip("?$"))
        else:
            try:
                owner = catalogue._type(requirement.owner)
            except ValueError as exc:
                owners = sorted({catalogue.type_refs[f.owner] for f in fragments if f.owner})
                raise QueryValidationError(
                    "goal_owner",
                    f"{requirement.clause}: owner '{requirement.owner}' is unresolved. Ground this goal's owner to its subject variable or retained class {owners}. Field inserts include their owner type.",
                ) from exc
            bindings = [str(v) for v, classes in types.items() if owner in classes]
            if len(bindings) > 1:
                raise QueryValidationError(
                    "goal_owner",
                    f"{requirement.clause}: this class occurs in several roles {bindings}. Ground the owner to the intended subject variable.",
                )
    matching_fields = {
        ref
        for ref in catalogue.field_refs.values()
        if words(requirement.concept) == words(catalogue.fragments[ref].label)
        and (not owner or catalogue.fragments[ref].owner == owner)
        and (
            subject_binding is None
            or catalogue.fragments[ref].owner in types.get(subject_binding, set())
        )
    }
    if matching_fields and not matching_fields.intersection(evidence) and not exact_types:
        raise QueryValidationError(
            "goal_concept",
            f"{requirement.clause}: discover the '{requirement.concept}' field on its owner; the selected evidence describes another field.",
        )
    witnessed = []
    for ref, fragment in zip(evidence, fragments, strict=True):
        if owner and fragment.owner and fragment.owner != owner:
            raise QueryValidationError(
                "goal_owner",
                f"{requirement.clause}: the selected field belongs to another subject class.",
            )
        if fragment.kind == "type":
            matches = [(s, o) for s, p, o in triples if p == RDF.type and str(o) == fragment.iri]
            witnessed += [s for s, _ in matches]
        elif fragment.path:
            matches = [
                (s, o)
                for s, p, o in triples
                if canonical_path(p.n3()) == canonical_path(path_to_sparql(fragment.path))
            ]
            matches += [
                (Variable(u["variables"][0]), Variable(u["variables"][-1]))
                for u in expanded.uses
                if u["ref"] == ref
                and len(u["variables"]) >= 2
                and all(
                    t in triples
                    for n in walk(prepareQuery("SELECT * WHERE {" + u["pattern"] + "}").algebra)
                    if isinstance(n, CompValue) and n.name == "BGP"
                    for t in n.triples
                )
            ]
            expected_owner = fragment.owner or owner
            if expected_owner:
                if matches and not any(expected_owner in types.get(s, set()) for s, _ in matches):
                    subject = matches[0][0].n3()
                    raise QueryValidationError(
                        "goal_type",
                        f"{requirement.clause}: identify the field's subject class with {subject} a <{expected_owner}> . The selected field insert supplies this pattern.",
                    )
                matches = [(s, o) for s, o in matches if expected_owner in types.get(s, set())]
            if subject_binding is not None:
                matches = [(s, o) for s, o in matches if s == subject_binding]
            witnessed += [o for _, o in matches]
        else:
            matches = []
        if requirement.kind == "relation" and requirement.target:
            try:
                target_class = catalogue._type(requirement.target)
                matches = [(s, o) for s, o in matches if target_class in types.get(o, set())]
            except ValueError:
                matches = [
                    (s, o) for s, o in matches if o == Variable(requirement.target.lstrip("?$"))
                ]
            if not matches:
                raise QueryValidationError(
                    "goal_target",
                    f"{requirement.clause}: connect the selected owner to {requirement.target} in the same required pattern.",
                )
        if not matches:
            all_triples = [
                t
                for n in walk(algebra)
                if isinstance(n, CompValue) and n.name == "BGP"
                for t in n.triples
            ]
            available = any(
                (fragment.kind == "type" and p == RDF.type and str(o) == fragment.iri)
                or (
                    fragment.path
                    and canonical_path(p.n3()) == canonical_path(path_to_sparql(fragment.path))
                )
                for _, p, o in all_triples
            )
            if (
                available
                and subject_binding is not None
                and not any(
                    s == subject_binding
                    and fragment.path
                    and canonical_path(p.n3()) == canonical_path(path_to_sparql(fragment.path))
                    for s, p, _ in all_triples
                )
            ):
                raise QueryValidationError(
                    "goal_owner",
                    f"{requirement.clause}: {fragment.label} is not a field on {subject_binding.n3()}. Select evidence owned by that role or correct the goal's owner.",
                )
            if requirement.required and available:
                raise QueryValidationError(
                    "goal_availability",
                    f"{requirement.clause}: {fragment.label} occurs only in an optional or alternative branch, but this goal requires it in every row. Make the field required in the query. Change the goal to required=false only if the original request asks for optional metadata.",
                )
            raise QueryValidationError(
                "goal_path",
                f"{requirement.clause}: the selected schema evidence is absent from its pattern.",
            )
        if requirement.kind == "output":
            values = [s if fragment.kind == "type" else o for s, o in matches]
            if not outputs.intersection(values):
                raise QueryValidationError(
                    "goal_output",
                    f"{requirement.clause}: project the selected {fragment.label} binding.",
                )
        if requirement.kind in {"output", "relation"} and not requirement.required:
            mandatory = [
                t
                for n in required_nodes(algebra)
                if isinstance(n, CompValue) and n.name == "BGP"
                for t in n.triples
            ]
            if any(s == left and o == right for left, _, right in mandatory for s, o in matches):
                raise QueryValidationError(
                    "goal_optional",
                    f"{requirement.clause}: keep available metadata optional so missing values preserve the requested resources.",
                )
    if requirement.kind == "output" and not outputs.intersection(witnessed):
        raise QueryValidationError(
            "goal_output",
            f"{requirement.clause}: project the selected resource identity or field value.",
        )
    if requirement.kind in {"entity_filter", "text_filter"}:
        if not requirement.value:
            raise QueryValidationError(
                "goal_value", "Declare the requested entity or text value before grounding it."
            )
        if requirement.kind == "entity_filter":
            term = catalogue.fragments.get(grounding.get("entity", ""))
            if term is None or term.kind != "term" or term.term is None:
                raise QueryValidationError(
                    "goal_entity",
                    f"{requirement.clause}: retrieve and select the actual entity with rdf_find or a field value lookup.",
                )
            rdf = term.term.to_rdf()
            fixed = any(o == rdf for o in witnessed)
            fixed |= any(
                n.res and any(all(row.get(v) == rdf for row in n.res) for v in witnessed)
                for n in nodes
                if isinstance(n, CompValue) and n.name == "values"
            )
            if not fixed:
                raise QueryValidationError(
                    "goal_entity",
                    f"{requirement.clause}: constrain its relationship to the selected exact entity. A prose match cannot supply this binding.",
                )
        else:
            filters = [n for n in nodes if isinstance(n, CompValue) and n.name == "Filter"]
            if not any(
                isinstance(n, CompValue)
                and n.name.lower() == "builtin_isliteral"
                and any(v in witnessed for v in walk(n) if isinstance(v, Variable))
                for n in nodes
            ):
                raise QueryValidationError(
                    "goal_text_kind",
                    f"{requirement.clause}: add isLiteral on the text binding so URI strings cannot satisfy a wording condition.",
                )
            if not any(
                any(v in witnessed for v in walk(n.expr) if isinstance(v, Variable))
                and any(
                    isinstance(v, Literal) and str(v).casefold() == requirement.value.casefold()
                    for v in walk(n.expr)
                )
                for n in filters
            ):
                raise QueryValidationError(
                    "goal_text",
                    f"{requirement.clause}: filter the selected field with the declared text value.",
                )
