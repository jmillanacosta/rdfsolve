"""rdfsolve.mining.scan: the patterns counted from the rows of an index equal the patterns the
two-phase strategy counts with SPARQL on the same data."""

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


def test_a_branch_carrying_only_the_nodes_it_reaches_finds_the_same_paths(tmp_path, monkeypatch):
    """Where pairs would grow too large, a branch carries its reached nodes and counts the
    starts of each path backwards: the same paths and counts as with pairs."""
    from rdfsolve.mining import scan_paths

    graph = _graph()
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path / "store")

    def routes():
        summary = scan_paths.find_tested_paths_in_store(schema, store, max_hops=3)
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
            )
            for p in summary.paths
        }

    with_pairs = routes()
    monkeypatch.setattr(scan_paths, "PAIR_LIMIT", 0)
    assert routes() == with_pairs and with_pairs


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
