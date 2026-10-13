"""rdfsolve.mining.scan: the patterns counted from the rows of an index equal the patterns the
two-phase strategy counts with SPARQL on the same data."""

import pytest
from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, Literal, Namespace

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining import scan
from rdfsolve.mining.scan import ScanStrategy, store_from_graph

E = Namespace("urn:ex:")


def _graph() -> Graph:
    graph = Graph()
    graph.add((E.A, RDF.type, OWL.Class))
    for n in range(3):
        a = E[f"a{n}"]
        graph.add((a, RDF.type, E.A))
        graph.add((a, E.size, Literal(n, datatype=XSD.integer)))
        graph.add((a, E.weight, Literal("1.5", datatype=XSD.double)))
        graph.add((a, RDFS.label, Literal(f"a {n}", lang="en")))
        graph.add((a, E.link, E.b1))
        graph.add((a, E.link, E[f"u{n}"]))  # untyped IRI
    graph.add((E.a0, RDF.type, E.C))  # two classes
    graph.add((E.b1, RDF.type, E.B))
    graph.add((E.b1, E.owner, E.a0))
    node, typed = BNode(), BNode()
    graph.add((E.a1, E.part, node))
    graph.add((node, E.name, Literal("x")))
    graph.add((E.a2, E.part, typed))
    graph.add((typed, RDF.type, E.B))
    graph.add((E.s1, E.link, E.b1))  # untyped subject
    return graph


def _patterns(miner: SchemaMiner) -> dict:
    with miner:
        schema = miner.mine(dataset_name="fixture")
    return {
        (p.subject_class, p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
            p.distinct_objects,
            p.blank_node_predicates,
        )
        for p in schema.patterns
    }


def test_scan_counts_the_patterns_the_two_phase_strategy_counts(tmp_path):
    graph = _graph()
    sparql = _patterns(SchemaMiner.from_graph(graph, delay=0))
    store = store_from_graph(graph, tmp_path / "store")
    scan = _patterns(SchemaMiner.from_graph(graph, delay=0, strategy=ScanStrategy(store=store)))
    assert scan == sparql
    assert scan[(str(E.A), str(E.part), "BlankNode", None)] == (
        2,
        2,
        2,
        [str(RDF.type), str(E.name)],
    )
    assert scan[(str(E.A), str(E.link), "Resource", None)][:3] == (3, 3, 3)


def test_scan_finds_the_tested_paths_that_the_sparql_search_finds(tmp_path):
    from rdfsolve.mining.navigation import find_tested_paths
    from rdfsolve.mining.scan_paths import find_tested_paths_in_store

    graph = _graph()
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
        sparql = find_tested_paths(schema, miner.helper, max_hops=3)
    store = store_from_graph(graph, tmp_path / "store")
    scan = find_tested_paths_in_store(schema, store, max_hops=3)

    def routes(summary):
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
            )
            for p in summary.paths
        }

    assert routes(scan) == routes(sparql) and routes(scan)
    assert scan.matched_by_length == sparql.matched_by_length
    assert scan.complete_lengths == sparql.complete_lengths == [2, 3]


def test_tested_paths_step_only_through_relations_between_records(tmp_path):
    """Membership and annotation properties (NON_RELATION_PROPERTIES, and those the data
    declare owl:AnnotationProperty) are left out of the steps of tested paths, by both searches;
    the schema's one-hop patterns keep them."""
    from rdfsolve.mining.navigation import find_tested_paths
    from rdfsolve.mining.scan_paths import find_tested_paths_in_store

    graph = _graph()
    graph.add((E.note, RDF.type, OWL.AnnotationProperty))
    graph.add((E.b1, E.note, E.a0))
    graph.add((E.b1, RDFS.seeAlso, E.a1))
    graph.add((E.b1, RDF.type, E.A))
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
        sparql = find_tested_paths(schema, miner.helper, max_hops=3)
    store = store_from_graph(graph, tmp_path / "store")
    scan_found = find_tested_paths_in_store(schema, store, max_hops=3)
    left_out = {str(RDF.type), str(RDFS.label), str(RDFS.seeAlso), str(E.note)}
    one_hop = {p.property_uri for p in schema.patterns}
    assert {str(E.note), str(RDFS.seeAlso)} <= one_hop

    def routes(summary):
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
            )
            for p in summary.paths
        }

    assert routes(scan_found) == routes(sparql) and routes(scan_found)
    assert not any(step[1] in left_out for route in routes(scan_found) for step in route)


def test_anonymous_classes_are_named_in_manchester_syntax_and_folded_into_edges(tmp_path):
    """Two blank nodes with the same expression are one named class; its members' named class
    gets the edge the expression stands for (owner, 2026-10-06: fold by object class)."""
    graph = Graph()
    for n in range(2):
        restriction = BNode()
        graph.add((restriction, RDF.type, OWL.Restriction))
        graph.add((restriction, OWL.onProperty, E.partOf))
        graph.add((restriction, OWL.someValuesFrom, E.Heart))
        graph.add((E[f"v{n}"], RDF.type, restriction))
        graph.add((E[f"v{n}"], RDF.type, E.Vessel))
        graph.add((E[f"v{n}"], E.size, Literal(n)))
    graph.add((E.Heart, RDFS.label, Literal("heart muscle", lang="en")))
    graph.add((E.Heart, RDFS.label, Literal("coeur", lang="fr")))
    graph.add((E.partOf, RDFS.label, Literal("part of")))
    store = store_from_graph(graph, tmp_path / "store")
    with SchemaMiner.from_graph(graph, delay=0, strategy=ScanStrategy(store=store)) as miner:
        schema = miner.mine(dataset_name="fixture")
        config = miner.last_report.config
    ((iri, expression),) = config["class_expressions"].items()
    assert iri.startswith("urn:rdfsolve:class-expression:")
    assert expression["manchester_iris"] == f"<{E.partOf}> some <{E.Heart}>"
    assert expression["manchester_labels"] == "'part of' some 'heart muscle'"
    assert (expression["nodes"], expression["members"]) == (2, 2)
    assert not any(iri in (p.subject_class, p.object_class) for p in schema.patterns)
    folded = [
        (p.count, p.distinct_subjects, p.evidence_source)
        for p in schema.patterns
        if (p.subject_class, p.property_uri, p.object_class)
        == (str(E.Vessel), str(E.partOf), str(E.Heart))
    ]
    assert folded == [(2, 2, "inferred")]
    assert config["class_expression_folding"]


@pytest.mark.parametrize("sparse", [1, 10**9])
def test_node_sets_read_by_marks_or_by_search_find_the_same_paths(tmp_path, monkeypatch, sparse):
    """The node sets of the search are met and followed by searching sorted arrays when small
    and by a mark per node when large, and the starts of the paths are counted in batches as
    small as one word of paths: every way gives the same paths, counts and summary."""
    from rdfsolve.mining import scan_paths

    graph = _graph()
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path / "store")

    def search():
        summary = scan_paths.find_tested_paths_in_store(schema, store, max_hops=3)
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
            )
            for p in summary.paths
        }, summary.model_dump(exclude={"paths"})

    searched, summary = search()
    monkeypatch.setattr(scan_paths, "DENSE", 1)
    monkeypatch.setattr(scan_paths, "SPARSE", sparse)
    monkeypatch.setattr(scan_paths, "LABEL_BYTES", 1)
    assert search() == (searched, summary) and searched


def test_a_predicate_of_nul_bytes_is_left_out_and_reported(tmp_path):
    """rdfportal.oma (job 115716) failed the source: a pattern's property was a string of NUL
    bytes, which SchemaPattern refuses. A term that cannot be a class or property is left out
    of the patterns and reported (scan_invalid_terms); the source is mined."""
    from rdflib import Graph, Literal, URIRef

    graph = Graph()
    a, cls = URIRef("urn:x:a"), URIRef("urn:x:A")
    graph.add((a, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), cls))
    graph.add((a, URIRef("urn:x:name"), Literal("a")))
    graph.add((a, URIRef("\x00" * 16), Literal("damaged")))
    store = store_from_graph(graph, tmp_path / "store")
    assert "\x00" * 16 not in store.predicates and list(store.left_out_predicates) == ["\x00" * 16]
    assert all(p.property_uri != "\x00" * 16 for p in scan.count_patterns(store))
    with SchemaMiner.from_graph(graph, delay=0, strategy=ScanStrategy(store=store)) as miner:
        schema = miner.mine()
        report = miner.last_report
    assert any(p.property_uri == "urn:x:name" for p in schema.patterns)
    [term] = report.config["scan_invalid_terms"]["terms"]
    assert term["term"] == repr("\x00" * 16)


def test_row_navigation_keeps_a_bounded_number_of_paths_per_class_and_length(tmp_path):
    """onco (job 115871): row navigation kept 18.9 M matched paths in its time budget, a 4.96
    GB schema whose outputs ran out of memory. At most max_paths_per_class paths of each length
    are kept for each start class; the others are counted, by length and class, and the
    lengths are marked truncated. The matched counts are those of the whole search."""
    from collections import Counter

    from rdfsolve.mining.scan_paths import find_tested_paths_in_store

    graph = _graph()
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path / "store")
    whole = find_tested_paths_in_store(schema, store, max_hops=3)
    capped = find_tested_paths_in_store(schema, store, max_hops=3, max_paths_per_class=1)
    per_class = Counter((len(p.steps), p.steps[0].subject_class) for p in capped.paths)
    assert per_class and max(per_class.values()) == 1
    assert len(capped.paths) < len(whole.paths)
    omitted = sum(n for by_class in capped.omitted_by_class.values() for n in by_class.values())
    assert len(capped.paths) + omitted <= len(whole.paths)
    assert capped.truncated_lengths and capped.max_paths_per_length == 1
    kept = {tuple(s.property_uri for s in p.steps) for p in capped.paths}
    assert kept <= {tuple(s.property_uri for s in p.steps) for p in whole.paths}
    assert not whole.truncated_lengths and not whole.omitted_by_class


def test_paths_tested_on_samples_of_every_instance_equal_the_whole_search(tmp_path, monkeypatch):
    """With a start sample as large as every class, the hop-by-hop reading of the sampled
    starts' edges finds the same paths with the same counts as reading every edge; with a
    smaller sample, each kept path is one of the whole search's, with a sampled support."""
    from rdfsolve.mining import scan_paths
    from rdfsolve.mining.scan_paths import find_tested_paths_in_store

    graph = _graph()
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path / "store")

    def routes(summary):
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
            )
            for p in summary.paths
        }

    whole = find_tested_paths_in_store(schema, store, max_hops=3)
    sampled = find_tested_paths_in_store(schema, store, max_hops=3, start_sample=10_000)
    assert routes(sampled) == routes(whole) and routes(whole)
    assert sampled.start_sample == 10_000 and sampled.stop_reason is None
    assert sampled.complete_lengths == whole.complete_lengths
    monkeypatch.setattr(scan_paths, "DENSE", 1)
    assert routes(find_tested_paths_in_store(schema, store, max_hops=3, start_sample=10_000)) == (
        routes(whole)
    )
    one = find_tested_paths_in_store(schema, store, max_hops=3, start_sample=1)
    assert set(routes(one)) <= set(routes(whole))
    assert all(source == 1 and matched == 1 for source, matched in routes(one).values())
