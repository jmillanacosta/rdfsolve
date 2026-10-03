"""rdfsolve.analysis.connectivity: the connectivity graph of datasets and its evidence."""

import pytest

from rdfsolve.analysis import best_route, build_connectivity
from rdfsolve.mappings.signatures import Link, LinkEvidence, read_links, write_links
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary

UP = "http://purl.uniprot.org/uniprot/"


def schema(name, rows):
    patterns = [SchemaPattern(subject_class=c, property_uri=p, object_class=o) for c, p, o in rows]
    return MinedSchema(about=AboutMetadata.build(dataset_name=name), patterns=patterns)


SCHEMAS = {
    "genes": schema("genes", [("urn:Gene", "urn:xref", "Resource")]),
    "proteins": schema("proteins", [("urn:Protein", "urn:name", "Literal")]),
}
EVIDENCE = LinkEvidence(
    Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein"),
    sampled=50,
    found=48,
    target_forms={UP + "{id}": 48},
    examples=[("https://identifiers.org/uniprot:P04637", UP + "P04637")],
)


def test_verified_links_survive_a_file_and_become_evidence_edges(tmp_path):
    path = tmp_path / "links.tsv"
    write_links(path, [EVIDENCE])
    assert read_links(path) == [EVIDENCE], "The table keeps every field"
    graph = build_connectivity(SCHEMAS, links=read_links(path))
    (edge,) = [(a, b, d) for a, b, d in graph.edges(data=True) if d["kind"] == "verified_link"]
    assert edge[:2] == (("genes", "urn:Gene"), ("proteins", "urn:Protein"))
    assert (edge[2]["predicate"], edge[2]["share"], edge[2]["sampled"]) == ("urn:xref", 0.96, 50)
    assert edge[2]["target_forms"] == {UP + "{id}": 48}, "The rewrite that applies the link"


def test_a_link_to_a_class_that_no_schema_has_is_refused():
    other = LinkEvidence(
        Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Missing"),
        1,
        1,
        {},
        [],
    )
    with pytest.raises(ValueError, match="absent"):
        build_connectivity(SCHEMAS, links=[other])


AB = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
BC = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C")
CD = SchemaPattern(subject_class="urn:C", property_uri="urn:r", object_class="urn:D")


def _schema(name, patterns, paths=()):
    navigation = None
    if paths:
        navigation = NavigationSummary(
            max_hops=2,
            max_paths_per_length=0,
            edge_count=len(patterns),
            walk_counts={2: 1},
            strategy="tested",
            paths=list(paths),
        )
    return MinedSchema(
        about=AboutMetadata.build(dataset_name=name), patterns=list(patterns), navigation=navigation
    )


def _link(source_class, target_class, found, sampled, complete):
    link = Link("join", "a", source_class, "urn:xref", "uniprot", "b", target_class)
    return LinkEvidence(link, sampled, found, {}, [], population=sampled, complete=complete)


FOLLOWED = NavigationPath(
    steps=[AB, BC],
    evidence="instance_tested",
    instance_support="matched",
    matched_sources=3,
    source_count=5,
)
CONNECTIVITY_EVIDENCE_SCHEMAS = {"a": _schema("a", [AB, BC], [FOLLOWED]), "b": _schema("b", [CD])}


def _edges(graph, kind):
    return [(u, v, d) for u, v, d in graph.edges(data=True) if d["kind"] == kind]


def test_each_edge_has_its_level():
    links = [
        _link("urn:A", "urn:D", 50, 50, True),  # every value read and found
        _link("urn:B", "urn:D", 48, 50, False),  # a sample
        _link("urn:C", "urn:D", 5, 50, True),  # every value read, a small share
        _link("urn:B", "urn:C", 0, 50, True),  # looked up, none found
    ]
    candidate = Link("join", "a", "urn:C", "urn:seeAlso", "uniprot", "b", "urn:C")
    graph = build_connectivity(CONNECTIVITY_EVIDENCE_SCHEMAS, links=links, candidates=[candidate])
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
    graph = build_connectivity(CONNECTIVITY_EVIDENCE_SCHEMAS)
    inside = best_route(graph, ("a", "urn:A"), ("a", "urn:C"))
    assert [e["kind"] for e in inside["edges"]] == ["path"], "The tested path, not two steps"
    assert inside["evidence"] == "confirmed" and not inside["composed"]
    across = best_route(graph, ("a", "urn:A"), ("b", "urn:D"))
    assert [e["kind"] for e in across["edges"]] == ["path", "shared_class", "schema"]
    assert across["evidence"] == "plausible" and across["composed"]
    assert across["weakest_segment"] == "plausible"


def test_a_route_takes_the_strongest_edges_first():
    confirmed = _link("urn:A", "urn:C", 50, 50, True)
    graph = build_connectivity(CONNECTIVITY_EVIDENCE_SCHEMAS, links=[confirmed])
    route = best_route(graph, ("a", "urn:A"), ("b", "urn:D"))
    assert [e["kind"] for e in route["edges"]] == ["verified_link", "schema"]
    assert route["weakest_segment"] == "confirmed" and route["evidence"] == "plausible"
    assert best_route(graph, ("b", "urn:D"), ("a", "urn:A")) is None, "Edges have a direction"
