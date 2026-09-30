"""Each edge of the connectivity graph has a level of evidence: confirmed (seen in full on the
data), tested (checked on part of the data, or with a weak result) or plausible (stated or
composed, not tested on the data). A route of several edges is a composition: its parts can be
confirmed, but no instance is known to follow the whole route, so it is plausible at most. A
tested path or a link is one edge, and a route search takes the strongest edges first."""

from rdfsolve.analysis import best_route, build_connectivity
from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary

AB = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
BC = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C")
CD = SchemaPattern(subject_class="urn:C", property_uri="urn:r", object_class="urn:D")


def _schema(name, patterns, paths=()):
    navigation = None
    if paths:
        navigation = NavigationSummary(
            max_hops=2, max_paths_per_length=0, edge_count=len(patterns), walk_counts={2: 1},
            strategy="tested", paths=list(paths),
        )
    return MinedSchema(
        about=AboutMetadata.build(dataset_name=name), patterns=list(patterns), navigation=navigation
    )


def _link(source_class, target_class, found, sampled, complete):
    link = Link("join", "a", source_class, "urn:xref", "uniprot", "b", target_class)
    return LinkEvidence(link, sampled, found, {}, [], population=sampled, complete=complete)


FOLLOWED = NavigationPath(
    steps=[AB, BC], evidence="instance_tested", instance_support="matched",
    matched_sources=3, source_count=5,
)
SCHEMAS = {"a": _schema("a", [AB, BC], [FOLLOWED]), "b": _schema("b", [CD])}


def _edges(graph, kind):
    return [(u, v, d) for u, v, d in graph.edges(data=True) if d["kind"] == kind]


def test_each_edge_has_its_level():
    links = [
        _link("urn:A", "urn:D", 50, 50, True),   # every value read and found
        _link("urn:B", "urn:D", 48, 50, False),  # a sample
        _link("urn:C", "urn:D", 5, 50, True),    # every value read, a small share
        _link("urn:B", "urn:C", 0, 50, True),    # looked up, none found
    ]
    candidate = Link("join", "a", "urn:C", "urn:seeAlso", "uniprot", "b", "urn:C")
    graph = build_connectivity(SCHEMAS, links=links, candidates=[candidate])
    assert {d["evidence"] for *_, d in _edges(graph, "schema")} == {"confirmed"}
    assert {d["evidence"] for *_, d in _edges(graph, "shared_class")} == {"plausible"}
    by_source = {u[1]: d["evidence"] for u, _, d in _edges(graph, "verified_link")}
    assert by_source == {"urn:A": "confirmed", "urn:B": "tested", "urn:C": "tested"}
    (proposed,) = _edges(graph, "candidate_link")
    assert proposed[2]["evidence"] == "plausible" and proposed[2]["predicate"] == "urn:seeAlso"
    (path,) = _edges(graph, "path")
    assert path[:2] == (("a", "urn:A"), ("a", "urn:C")) and path[2]["evidence"] == "confirmed"
    assert path[2]["steps"] == ["urn:p", "urn:q"] and path[2]["matched_sources"] == 3


def test_a_composed_route_is_plausible_and_a_tested_path_is_one_edge():
    graph = build_connectivity(SCHEMAS)
    inside = best_route(graph, ("a", "urn:A"), ("a", "urn:C"))
    assert [e["kind"] for e in inside["edges"]] == ["path"], "The tested path, not two steps"
    assert inside["evidence"] == "confirmed" and not inside["composed"]
    across = best_route(graph, ("a", "urn:A"), ("b", "urn:D"))
    assert [e["kind"] for e in across["edges"]] == ["path", "shared_class", "schema"]
    assert across["evidence"] == "plausible" and across["composed"]
    assert across["weakest_segment"] == "plausible"


def test_a_route_takes_the_strongest_edges_first():
    confirmed = _link("urn:A", "urn:C", 50, 50, True)
    graph = build_connectivity(SCHEMAS, links=[confirmed])
    route = best_route(graph, ("a", "urn:A"), ("b", "urn:D"))
    assert [e["kind"] for e in route["edges"]] == ["verified_link", "schema"]
    assert route["weakest_segment"] == "confirmed" and route["evidence"] == "plausible"
    assert best_route(graph, ("b", "urn:D"), ("a", "urn:A")) is None, "Edges have a direction"
