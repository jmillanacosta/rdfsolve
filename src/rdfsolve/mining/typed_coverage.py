"""Match observed edges against typed schema profiles."""

from __future__ import annotations

from rdflib import Literal, URIRef

from rdfsolve.mining.query_builders import _context_pattern
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS


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
    subject_type = _context_pattern("?s a ?_coveredSubject .", objects).replace(
        "?_contextGraph", "?_subjectTypeGraph"
    )
    object_type = _context_pattern("?o a ?_coveredObject .", objects).replace(
        "?_contextGraph", "?_objectTypeGraph"
    )
    any_type = _context_pattern("?o a ?_anyObjectType .", objects).replace(
        "?_contextGraph", "?_objectAnyGraph"
    )
    match = f"""EXISTS {{
?s ?p ?o .
VALUES (?_coveredSubject ?p ?_coveredObject ?_coveredDatatype) {{ {" ".join(values)} }}
{subject_type}
FILTER(
  (isIRI(?_coveredObject) && EXISTS {{ {object_type} }}) ||
  (?_coveredObject = "Literal" && isLiteral(?o) &&
    (!BOUND(?_coveredDatatype) || DATATYPE(?o) = ?_coveredDatatype)) ||
  (?_coveredObject = "Resource" && isIRI(?o) && !EXISTS {{ {any_type} }}) ||
  (?_coveredObject = "BlankNode" && isBlank(?o))
)
}}"""
    return f"IF(EXISTS {{ {subject_type} }}, {match}, false)"


def uncovered_filter(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
) -> str:
    """Select edges absent from the observed typed profiles."""
    return f"FILTER(!{typed_match(keys, type_graphs, context_graphs)})"
