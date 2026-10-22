"""Scan mining in a graph scope: the patterns counted from the rows of a store with named graphs
equal the patterns the SPARQL miner counts on the same dataset, in the three scopes of a run:
the default graph (endpoint_default), several data graphs (quad_occurrences, per-graph counts),
and one data graph with the others as type context (triples_in_graph).

The dataset has types in another graph than the edges that use them, a triple held by two
graphs, a blank node, an untyped IRI and triples outside every named graph (QLever keeps input
without a graph in its own default graph; the default graph of a query is the union of all).
"""

from __future__ import annotations

import pytest
from rdflib import RDF, BNode, Dataset, Literal, Namespace, URIRef

from rdfsolve.mining.edge_graph_split import split_by_edge_graph
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.scan import QLEVER_DEFAULT_GRAPH, ScanStrategy, store_from_graph

E = Namespace("urn:ex:")
P, T, R = "urn:graph:proteins", "urn:graph:taxa", "urn:graph:reactions"
DATA = [P, T, R]


def _dataset() -> Dataset:
    data = Dataset(default_union=True)
    proteins, taxa, reactions = (data.graph(URIRef(g)) for g in DATA)
    for n in (1, 2):
        protein = E[f"p{n}"]
        proteins.add((protein, RDF.type, E.Protein))
        proteins.add((protein, E.name, Literal(f"p{n}")))
    proteins.add((E.p1, E.organism, E.t1))  # typed in the taxa graph
    proteins.add((E.p2, E.organism, E.t9))  # typed nowhere
    part = BNode()
    proteins.add((E.p1, E.part, part))
    proteins.add((part, E.name, Literal("domain")))
    taxa.add((E.t1, RDF.type, E.Taxon))
    taxa.add((E.t1, E.name, Literal("t1")))
    taxa.add((E.p2, RDF.type, E.Enzyme))  # a second type of p2, in another graph
    reactions.add((E.r1, RDF.type, E.Reaction))
    reactions.add((E.r1, E.enzyme, E.p1))  # typed in the proteins graph
    reactions.add((E.r1, E.enzyme, E.p2))
    reactions.add((E.p1, E.name, Literal("p1")))  # the same triple as in the proteins graph
    data.default_graph.add((E.d1, RDF.type, E.Protein))  # outside every named graph
    data.default_graph.add((E.d1, E.organism, E.t1))
    data.default_graph.add((E.p1, E.name, Literal("p1")))
    return data


def _mine(strategy=None, **scope):
    kwargs = {"strategy": strategy} if strategy is not None else {}
    with SchemaMiner.from_graph(_dataset(), delay=0, **scope, **kwargs) as miner:
        return miner.mine(dataset_name="fixture")


def _rows(schema) -> dict:
    return {
        (p.subject_class, p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
            p.distinct_objects,
            p.count_semantics,
            p.graphs,
            p.blank_node_predicates,
        )
        for p in schema.patterns
    }


SCOPES = {
    "default": {},
    "several graphs": {"graph_uris": DATA},
    "one graph with type context": {"graph_uris": [R], "type_context_graph_uris": [P, T]},
    "one graph without type context": {"graph_uris": [R]},
    "two graphs with type context": {"graph_uris": [P, R], "type_context_graph_uris": [T]},
}


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    return store_from_graph(_dataset(), tmp_path_factory.mktemp("store") / "store")


def test_the_store_keeps_the_graph_of_each_row(store):
    assert set(store.graphs) == {P, T, R, QLEVER_DEFAULT_GRAPH}
    name = str(E.name)
    assert store.manifest["rows"][name] == 6
    assert store.manifest["triples_by_predicate"][name] == 4  # p1 name "p1" in three graphs
    assert store.rows(name).collect().height == 4
    assert store.view([P, R]).rows(name).collect().height == 3
    assert store.view([P, R]).graph_rows(name).collect().height == 4


@pytest.mark.parametrize("scope", SCOPES, ids=list(SCOPES))
def test_scan_counts_what_the_sparql_miner_counts_in_each_graph_scope(store, scope):
    sparql = _rows(_mine(**SCOPES[scope]))
    scan = _rows(_mine(ScanStrategy(store=store), **SCOPES[scope]))
    assert scan == sparql
    assert scan


def test_the_scopes_differ_as_the_miner_defines_them(store):
    rows = {scope: _rows(_mine(ScanStrategy(store=store), **SCOPES[scope])) for scope in SCOPES}
    name = (str(E.Protein), str(E.name), "Literal", "http://www.w3.org/2001/XMLSchema#string")
    # The default graph holds p1 name "p1" once; several graphs count it in each.
    assert rows["default"][name][:4] == (2, 2, 2, "endpoint_default")
    assert rows["several graphs"][name][:5] == (3, None, None, "quad_occurrences", {P: 2, R: 1})
    enzyme = (str(E.Reaction), str(E.enzyme), str(E.Protein), None)
    assert rows["one graph with type context"][enzyme][:4] == (2, 1, 2, "triples_in_graph")
    resource = (str(E.Reaction), str(E.enzyme), "Resource", None)
    assert rows["one graph without type context"][resource][:3] == (2, 1, 2)
    assert enzyme not in rows["one graph without type context"]


def test_per_graph_schemas_get_the_distinct_counts_of_their_graph(store):
    """A schema of several graphs has no distinct counts; each per-graph schema cut from it has
    those of its graph, equal to the graph mined alone with the others as type context."""
    whole = _mine(ScanStrategy(store=store), graph_uris=DATA)
    assert all(p.distinct_subjects is None for p in whole.patterns)
    for graph in DATA:
        part = split_by_edge_graph(whole, graph, f"fixture.{graph[-4:]}")
        others = [g for g in DATA if g != graph]
        alone = _rows(_mine(graph_uris=[graph], type_context_graph_uris=others))
        assert {k: v[:3] for k, v in _rows(part).items()} == {k: v[:3] for k, v in alone.items()}


@pytest.mark.parametrize("scope", ["default", "several graphs", "two graphs with type context"])
def test_structure_counts_and_paths_follow_the_graph_scope(store, scope):
    """The other scan phases read the same scope as the miner's queries: the structural census
    of each data graph, class members and overlaps, dataset statistics and tested paths."""
    from rdfsolve.mining.navigation import find_tested_paths
    from rdfsolve.mining.scan_paths import find_tested_paths_in_store
    from tests.mining.test_scan_structure import QLeverLike

    def mine(strategy=None):
        kwargs = {"strategy": strategy} if strategy is not None else {}
        miner = SchemaMiner.from_graph(_dataset(), delay=0, **SCOPES[scope], **kwargs)
        miner._helper = QLeverLike(miner._helper)
        with miner:
            schema = miner.mine(dataset_name="fixture")
            paths = find_tested_paths(schema, miner.helper, max_hops=3) if not strategy else None
        return schema, miner._rc.report.config, paths

    sparql, sparql_config, sparql_paths = mine()
    scan, scan_config, _ = mine(ScanStrategy(store=store))

    def structural(schema):
        return sorted(
            (p.model_dump(exclude={"examples"}) for p in schema.structural_patterns or []), key=str
        )

    assert structural(scan) == structural(sparql)
    drop = {"census", "census_properties", "census_patterns"}
    assert [
        {k: v for k, v in e.items() if k not in drop} for e in scan_config["structural_coverage"]
    ] == [
        {k: v for k, v in e.items() if k not in drop} for e in sparql_config["structural_coverage"]
    ]
    assert scan.about.class_entity_counts == sparql.about.class_entity_counts
    assert scan.class_extensions == sparql.class_extensions
    assert scan_config["dataset_statistics"] == sparql_config["dataset_statistics"]
    found = find_tested_paths_in_store(scan, store, max_hops=3)

    def routes(summary):
        return {
            tuple((s.subject_class, s.property_uri, s.object_class) for s in p.steps): (
                p.source_count,
                p.matched_sources,
                tuple(p.graph_uris),
                tuple(p.type_context_graph_uris),
            )
            for p in summary.paths
        }

    assert routes(found) == routes(sparql_paths)
