"""Match observed edges against typed schema profiles."""

from __future__ import annotations

from rdflib import Literal, URIRef

from rdfsolve.mining.query_builders import _context_pattern
from rdfsolve.schema_models._constants import _SENTINEL_OBJECTS


def typed_match(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
    predicate: str | None = None,
    restriction: str = "",
) -> str:
    """Return an existence test for the union of observed typed profiles.

    With *predicate*, the test is for edges of that one property: its group reads only that
    property. QLever evaluates the group of EXISTS on its own before the join, so a group that
    reads ?s ?p ?o reads the whole graph for each property (Bgee RO_0002162: 217 s, and 20 s
    with the constant property; the same counts). *restriction* (for example VALUES ?o
    {...}) follows the edge, so that the group reads only the edges of one census batch.
    """
    if not keys:
        return "false"
    values = []
    for subject, prop, obj, datatype in sorted(set(keys), key=str):
        target = Literal(obj).n3() if obj in _SENTINEL_OBJECTS else URIRef(obj).n3()
        dt = URIRef(datatype).n3() if datatype else "UNDEF"
        values.append(f"({URIRef(subject).n3()} {URIRef(prop).n3()} {target} {dt})")
    objects = list(dict.fromkeys((type_graphs or []) + (context_graphs or [])))
    subject_type = _context_pattern("?s a ?_coveredSubject .", objects).replace(
        "?_contextGraph", "?_subjectTypeGraph"
    )
    object_type = _context_pattern("?o a ?_objectType .", objects).replace(
        "?_contextGraph", "?_objectTypeGraph"
    )
    any_type = _context_pattern("?o a ?_anyObjectType .", objects).replace(
        "?_contextGraph", "?_objectAnyGraph"
    )
    # The triple pattern joins the outer ?s ?p ?o: QLever and Virtuoso evaluate EXISTS as a
    # join on the variables that its patterns bind, not by substitution. The property of the
    # profile is compared, not bound again: Virtuoso rejects a VALUES that binds an outer
    # variable inside EXISTS (error SP031).
    if predicate:
        constant = URIRef(predicate).n3()
        edge, same_property = f"?s {constant} ?o .", f"FILTER(?_coveredProperty = {constant})"
    else:
        edge, same_property = "?s ?p ?o .", "FILTER(?p = ?_coveredProperty)"
    head = f"""{edge} {restriction}
VALUES (?_coveredSubject ?_coveredProperty ?_coveredObject ?_coveredDatatype) {{ {" ".join(values)} }}
{same_property}
{subject_type}"""
    untyped_object = f"{edge} {restriction} {any_type}"
    match = f"""(EXISTS {{ {head}
{object_type} FILTER(?_objectType = ?_coveredObject) }} || EXISTS {{ {head}
FILTER(
  (?_coveredObject = "Literal" && isLiteral(?o) &&
    (!BOUND(?_coveredDatatype) || DATATYPE(?o) = ?_coveredDatatype)) ||
  (?_coveredObject = "Resource" && isIRI(?o) && !EXISTS {{ {untyped_object} }}) ||
  (?_coveredObject = "BlankNode" && isBlank(?o))
) }})"""
    # The tests above already require a subject type, so no IF(EXISTS ...) wraps them (Virtuoso
    # rejects that form, error SQ156). A typed object is joined with the type of the profile in
    # one EXISTS; literals, untyped IRIs and blank nodes are tested in the other. Virtuoso
    # evaluates an EXISTS inside the EXISTS on a variable of the VALUES as false (AOP-Wiki: 1 of
    # 23,729 owl:sameAs edges covered) and gives wrong counts for an OPTIONAL inside the EXISTS
    # (1 of 2 prov:used edges); RDFLib evaluates a UNION inside an EXISTS as false. The inner
    # EXISTS of the untyped case uses no variable of the VALUES; it repeats the edge and its
    # restriction, because QLever evaluates the group on its own and ?o a ?_anyObjectType alone
    # reads every type triple of the graph (Bgee: 455.7 GB for every batch of RO_0002206). The
    # object type is compared in a FILTER: when it is joined on ?_coveredObject, QLever joins
    # the VALUES with the subject and object types, which share only variables of the VALUES,
    # before the edge (HGNC has-approved-symbol: over 6.5 GB; 41,789 edges in 0.0 s with the
    # FILTER). A group of the edge and the object type is refused by Virtuoso (error SQ156).
    return match


def uncovered_filter(
    keys: list[tuple[str, str, str, str | None]],
    type_graphs: list[str] | None,
    context_graphs: list[str] | None,
) -> str:
    """Select edges absent from the observed typed profiles."""
    return f"FILTER(!{typed_match(keys, type_graphs, context_graphs)})"
