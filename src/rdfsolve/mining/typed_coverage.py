"""Match observed edges against typed schema profiles."""

from __future__ import annotations

from rdflib import Literal, URIRef

from rdfsolve.mining.query_builders import _context_pattern
from rdfsolve.mining.types import EXCLUDED_RECORD_TYPES
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS


def eligible_subject(graphs: list[str] | None) -> str:
    """Select untyped subjects and subjects with an included IRI type."""
    types = _context_pattern("?s a ?_recordType .", graphs)
    excluded = ", ".join(URIRef(t).n3() for t in sorted(EXCLUDED_RECORD_TYPES))
    return (
        f"(!EXISTS {{ {types} }} || EXISTS {{ {types} "
        f"FILTER(isIRI(?_recordType) && ?_recordType NOT IN ({excluded})) }})"
    )


def typed_match(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
) -> str:
    """Return an existence test for the union of observed typed profiles."""
    if not keys:
        return "false"
    values = []
    for subject, predicate, obj, datatype in sorted(set(keys), key=str):
        target = Literal(obj).n3() if obj in _SENTINEL_OBJECTS else URIRef(obj).n3()
        dt = URIRef(datatype).n3() if datatype else "UNDEF"
        values.append(f"({URIRef(subject).n3()} {URIRef(predicate).n3()} {target} {dt})")
    objects = list(dict.fromkeys((type_graphs or []) + (context_graphs or [])))
    subject_type = _context_pattern("?s a ?_coveredSubject .", type_graphs).replace(
        "?_contextGraph", "?_subjectTypeGraph"
    )
    object_type = _context_pattern("?o a ?_coveredObject .", objects).replace(
        "?_contextGraph", "?_objectTypeGraph"
    )
    any_type = _context_pattern("?o a ?_anyObjectType .", objects).replace(
        "?_contextGraph", "?_objectAnyGraph"
    )
    return f"""EXISTS {{
?s ?p ?o .
VALUES (?_coveredSubject ?_coveredPredicate ?_coveredObject ?_coveredDatatype) {{ {" ".join(values)} }}
{subject_type}
FILTER(?p = ?_coveredPredicate)
FILTER(
  (isIRI(?_coveredObject) && EXISTS {{ {object_type} }}) ||
  (?_coveredObject = "Literal" && isLiteral(?o) &&
    (!BOUND(?_coveredDatatype) || DATATYPE(?o) = ?_coveredDatatype)) ||
  (?_coveredObject = "Resource" && isIRI(?o) && !EXISTS {{ {any_type} }}) ||
  (?_coveredObject = "BlankNode" && isBlank(?o))
)
}}"""


def uncovered_filter(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
) -> str:
    """Select eligible edges absent from the observed typed profiles."""
    return (
        f"FILTER({eligible_subject(type_graphs)}) "
        f"FILTER(!{typed_match(keys, type_graphs, context_graphs)})"
    )
