"""Compose and check retrieval queries against grounded question clauses."""

from __future__ import annotations

from functools import lru_cache
from typing import Any
from typing import Literal as Choice

from pydantic import BaseModel, ConfigDict, Field, model_validator
from rdflib import RDF, Literal, URIRef, Variable
from rdflib.paths import Path
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import CompValue

from rdfsolve.query_fragments import PreparedQuery, compile_query, walk
from rdfsolve.schema_models.exporters.paths import path_to_sparql


class QueryValidationError(ValueError):
    """A query failed an identified retrieval requirement."""

    def __init__(self, code: str, message: str):
        """Associate a stable error code with a concrete query failure."""
        self.code = code
        super().__init__(message)


class Requirement(BaseModel):
    """An atomic semantic commitment made before query construction."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    clause: str
    kind: Choice["output", "entity_filter", "text_filter", "relation", "scope"] = Field(
        description="output lists resources or field values; entity_filter restricts to a particular named member; text_filter matches literal wording; relation requires a connection; scope names the configured database."
    )
    concept: str = Field(
        description="Requested class, field or relationship concept in the question's vocabulary."
    )
    owner: str = Field(
        default="",
        description="Subject variable or class for a field/relationship, e.g. project. Omit for a resource output.",
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
    def valid_request(self):
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


def canonical(value: Any) -> Any:
    """Compare algebra while retaining scope, filters, terms and multiplicity."""
    if isinstance(value, CompValue):
        entries = []
        for key, item in value.items():
            if key.startswith("_"):
                continue
            normalized = canonical(item)
            if key in {"triples", "PV"}:
                normalized = tuple(sorted(set(normalized), key=repr))
            entries.append((key, normalized))
        return value.name, tuple(sorted(entries))
    if isinstance(value, dict):
        return tuple(sorted((canonical(k), canonical(v)) for k, v in value.items()))
    if isinstance(value, (tuple, list)):
        return tuple(canonical(v) for v in value)
    if isinstance(value, set):
        return tuple(sorted((canonical(v) for v in value), key=repr))
    if isinstance(value, (URIRef, Literal, Variable, Path)):
        return type(value).__name__, value.n3()
    return value


def validate_retrieval(query: PreparedQuery, catalogue) -> list[str]:
    """Check the supported SELECT algebra, paths, owners and term constraints."""
    algebra = prepareQuery(query.sparql).algebra
    nodes = list(walk(algebra))
    forbidden = {"Extend", "AggregateJoin", "Group", "Minus", "Reduced", "Slice"}
    for node in nodes:
        if isinstance(node, CompValue) and (
            node.name in forbidden or node.name.startswith("Aggregate_")
        ):
            raise QueryValidationError(
                "retrieval_operator",
                f"{node.name} transforms the answer. Retrieve RDF bindings and transform them in Python.",
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
    types = {}
    for subject, predicate, obj in triples:
        if predicate == RDF.type and isinstance(obj, URIRef):
            types.setdefault(subject, set()).add(str(obj))
    fields = [f for f in catalogue.fragments.values() if f.kind == "field"]
    supported = set()
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
    bound_terms = {}
    for node in nodes:
        if isinstance(node, CompValue) and node.name == "values":
            for row in node.res:
                for variable, term in row.items():
                    bound_terms.setdefault(variable, set()).add(term)
    warnings = []
    edges = []
    for subject, predicate, obj in triples:
        if predicate == RDF.type:
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
            f for f in fields if f.path.operator == "predicate" and str(predicate) == f.path.iri
        ]
        if isinstance(predicate, Path):
            candidates = [
                f
                for f in fields
                if canonical_path(predicate.n3()) == canonical_path(path_to_sparql(f.path))
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
                catalogue.metadata.get(catalogue.field_refs.get((f.owner, f.field_name)), {})
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
    # Algebra parsing provides the authoritative path structure.
    path = prepareQuery(f"SELECT ?s ?o WHERE {{ ?s {text} ?o }}").algebra
    return repr(
        canonical(
            next(
                n.triples[0][1] for n in walk(path) if isinstance(n, CompValue) and n.name == "BGP"
            )
        )
    )


def _optional_scope(node) -> tuple[set, set]:
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


def verify_query(sparql, requirements, grounding, catalogue) -> PreparedQuery:
    """Expand one SELECT and check its declared semantic witnesses."""
    query = compile_query(
        sparql, catalogue.fragments, catalogue.client._scope, catalogue.known_iris
    )
    query.warnings = validate_retrieval(query, catalogue)
    for key, requirement in requirements.items():
        if requirement.kind != "scope":
            validate_goal(requirement, grounding[key], query, catalogue)
    query.diagnostics["validation"] = [
        "selected goal witnesses",
        "field ownership",
        "binding scope",
        "retained paths",
        "retrieval algebra",
    ]
    return query


def required_nodes(value):
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


def validate_goal(requirement: Requirement, grounding, expanded: PreparedQuery, catalogue) -> None:
    """Require RDF witnesses for selected outputs, relations and restrictions."""
    evidence = [catalogue.type_refs.get(ref, ref) for ref in grounding.get("evidence", [])]
    fragments = [catalogue.fragments[ref] for ref in evidence if ref in catalogue.fragments]
    if not fragments or len(fragments) != len(evidence):
        raise QueryValidationError(
            "goal_evidence",
            f"{requirement.clause}: select retained class, field or path references as evidence.",
        )
    from rdfsolve.catalogue import words

    exact_types = {
        f.iri
        for f in catalogue.fragments.values()
        if f.kind == "type" and words(f.label) == words(requirement.concept)
    }
    if exact_types and not any(
        f.iri in exact_types
        or any(t in exact_types for t in catalogue.metadata.get(ref, {}).get("targets", []))
        for ref, f in zip(evidence, fragments, strict=True)
    ):
        raise QueryValidationError(
            "goal_concept",
            f"{requirement.clause}: select evidence for the discovered {requirement.concept} class.",
        )
    algebra = prepareQuery(expanded.sparql).algebra
    nodes = list(
        required_nodes(algebra)
        if requirement.required and requirement.kind in {"entity_filter", "text_filter", "relation"}
        else walk(algebra)
    )
    triples = [t for n in nodes if isinstance(n, CompValue) and n.name == "BGP" for t in n.triples]
    types = {}
    for n in walk(algebra):
        if isinstance(n, CompValue) and n.name == "BGP":
            for subject, predicate, obj in n.triples:
                if predicate == RDF.type:
                    types.setdefault(subject, set()).add(str(obj))
    outputs = {Variable(v.lstrip("?$")) for v in (grounding.get("project") or expanded.variables)}
    if not outputs.issubset({Variable(v) for v in expanded.variables}):
        raise QueryValidationError(
            "goal_output",
            f"{requirement.clause}: declared output bindings are missing from SELECT.",
        )
    owner, subject_binding = None, None
    if requirement.owner and any(f.owner for f in fragments):
        bound = types.get(Variable(requirement.owner.lstrip("?$")), set())
        if bound:
            subject_binding = Variable(requirement.owner.lstrip("?$"))
        else:
            owner = catalogue._type(requirement.owner)
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
                matches = [(s, o) for s, o in matches if expected_owner in types.get(s, set())]
            if subject_binding is not None:
                matches = [(s, o) for s, o in matches if s == subject_binding]
            witnessed += [o for _, o in matches]
        else:
            matches = []
        if not matches:
            raise QueryValidationError(
                "goal_path",
                f"{requirement.clause}: the selected schema evidence is absent from its pattern.",
            )
        if requirement.kind == "output" and not requirement.required:
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
            if term is None or term.kind != "term":
                raise QueryValidationError(
                    "goal_entity",
                    f"{requirement.clause}: retrieve and select the actual entity with rdf_find or a field value lookup.",
                )
            rdf = term.term.to_rdf()
            fixed = any(o == rdf for o in witnessed)
            fixed |= any(
                isinstance(n, dict)
                and any(v in witnessed and value == rdf for v, value in n.items())
                for n in nodes
            )
            if not fixed:
                raise QueryValidationError(
                    "goal_entity",
                    f"{requirement.clause}: constrain its relationship to the selected exact entity. A prose match cannot supply this binding.",
                )
        else:
            filters = [n for n in nodes if isinstance(n, CompValue) and n.name == "Filter"]
            if not any(
                set(witnessed).intersection(walk(n.expr))
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
