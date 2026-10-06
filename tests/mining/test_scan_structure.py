"""rdfsolve.mining.scan_structure: the outputs counted from the rows of a store equal what the
SPARQL miner computes on the same data.

The SPARQL side runs on an rdflib graph through a helper that answers as QLever's endpoint
does (sparql_engine "qlever", refusing QLever's ql:has-predicate), so that the miner takes the
path it takes on a QLever index: the census of each property, the recount of each structural
pattern, class extensions and dataset statistics.
"""

from __future__ import annotations

from typing import Any

import pytest
from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, Literal, Namespace

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.scan import store_from_graph
from rdfsolve.mining.scan_structure import (
    class_entity_counts,
    class_extensions,
    data_iri_findings,
    dataset_statistics,
    iri_findings,
    property_usage_evidence,
    structural_census,
)
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

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
        graph.add((a, E.link, E[f"u{n}"]))  # untyped IRI object
        for k in range(n + 1):  # 1, 2, 3 values: a histogram with several buckets
            graph.add((a, E.tag, Literal(f"t{k}")))
    graph.add((E.a0, RDFS.label, Literal("a zero", lang="nl")))
    graph.add((E.a0, RDF.type, E.C))  # two classes
    graph.add((E.a1, RDF.type, E.C))
    graph.add((E.C, RDFS.subClassOf, E.A))
    graph.add((E.b1, RDF.type, E.B))
    graph.add((E.b1, E.owner, E.a0))
    graph.add((E.b1, E.note, Literal("two\nlines")))
    node, typed = BNode(), BNode()
    graph.add((E.a1, E.part, node))
    graph.add((node, E.name, Literal("x")))
    graph.add((E.a2, E.part, typed))
    graph.add((typed, RDF.type, E.B))
    # untyped subjects: IRIs and blank nodes, with literals, IRIs and blank nodes as objects
    graph.add((E.s1, E.link, E.b1))
    graph.add((E.s1, RDFS.label, Literal("s one", lang="en")))
    graph.add((E.s1, E.size, Literal(7, datatype=XSD.integer)))
    graph.add((E.s2, E.link, E.u0))
    graph.add((E.s2, E.link, E.a1))
    axiom, inner = BNode(), BNode()
    graph.add((axiom, OWL.annotatedSource, E.a0))
    graph.add((axiom, OWL.annotatedTarget, inner))
    graph.add((inner, E.name, Literal("y")))
    # a subject whose only type is a blank node: typed, but no class covers its edges
    anonymous = BNode()
    graph.add((E.d1, RDF.type, anonymous))
    graph.add((E.d1, E.link, E.b1))
    return graph


class QLeverLike(SparqlHelper):
    """Answer from a local helper as a QLever endpoint, without ql:has-predicate."""

    def __init__(self, local: SparqlHelper) -> None:
        super().__init__(local.endpoint_url, sparql_engine="qlever")
        self.local = local
        self.dataset = None

    def select(self, query: str, purpose: str = "") -> dict[str, Any]:
        if "ql:has-predicate" in query:
            raise EndpointError("ql:has-predicate is QLever's")
        return self.local.select(query, purpose)

    def ask(self, query: str) -> bool:
        return self.local.ask(query)

    def construct(self, query: str) -> str:
        return self.local.construct(query)


@pytest.fixture(scope="module")
def mined(tmp_path_factory):
    graph = _graph()
    miner = SchemaMiner.from_graph(graph, delay=0)
    miner._helper = QLeverLike(miner._helper)
    with miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path_factory.mktemp("store") / "store")
    return miner, schema, miner._rc.report, store


def _without(entry: dict, *names: str) -> dict:
    return {k: v for k, v in entry.items() if k not in names}


def test_structural_census_and_patterns(mined, tmp_path):
    _, schema, report, store = mined
    assert report.config["structural_execution"] == "per_profile_queries"
    (sparql_entry,) = report.config["structural_coverage"]
    assert sparql_entry["census"] == "per_property"
    entry, patterns = structural_census(store, schema.patterns, buckets=3, work_dir=tmp_path / "w")
    assert _without(entry, "census") == _without(sparql_entry, "census")
    assert entry["uncovered_triples"] > entry["untyped_subject_triples"] > 0  # E.d1

    def key(p):
        return p.model_dump(exclude={"examples"})

    assert [key(p) for p in patterns] == [key(p) for p in schema.structural_patterns]
    assert {p.subject_selection for p in patterns} == {"untyped", "uncovered"}
    assert {p.subject_kind for p in patterns} == {"IRI", "BlankNode"}
    assert {p.object_kind for p in patterns} == {"IRI", "BlankNode", "Literal"}
    assert any(p.language == "en" for p in patterns)
    # each witness is an edge of its pattern: the miner's recount restricted to it finds it
    edges = {(str(s), str(p), str(o)) for s, p, o in _graph()}
    for p in patterns:
        (example,) = p.examples
        s, o = example["s"], example["o"]
        if s["type"] == "uri" and o["type"] != "bnode":
            assert (s["value"], p.property_uri, o["value"]) in edges


def test_class_entity_counts_and_extensions(mined):
    _, schema, _, store = mined
    counts, states = class_entity_counts(store, sorted(schema.about.class_entity_counts))
    assert counts == schema.about.class_entity_counts
    assert states == schema.about.class_entity_count_states
    found = class_extensions(store, sorted(schema.about.class_entity_counts))
    assert found == schema.class_extensions
    assert found.contained_in  # E.C within E.A


def test_dataset_statistics(mined):
    _, schema, report, store = mined
    statistics = dataset_statistics(store, buckets=3)
    assert statistics == report.config["dataset_statistics"]
    assert statistics["property_partitions"] == schema.about.property_partitions


def test_property_usage_evidence(mined):
    from rdfsolve.evidence.observed import collect_property_usage_evidence

    miner, schema, _, store = mined
    arguments: dict[str, Any] = dict(
        dataset_id="fixture",
        classes=sorted({p.subject_class for p in schema.patterns}),  # pipeline_stages/base.py
        class_entity_counts=schema.about.class_entity_counts,
        class_entity_count_states=schema.about.class_entity_count_states,
        graph_uris=schema.about.graph_uris,
        batch_size=2,
        collect_node_kinds=True,
        collect_datatypes=True,
        collect_histograms=True,
        shared_extensions=None,
    )
    sparql = collect_property_usage_evidence(helper=miner.helper, chunk_size=10_000, **arguments)
    scan = property_usage_evidence(store, **arguments)
    assert scan.model_dump() == sparql.model_dump()
    histograms = [r.value_count_histogram for r in scan.records if r.property_uri == str(E.tag)]
    assert {"1", "2", "3"} <= set().union(*histograms)
    assert any(r.language_counts == {"en": 3, "nl": 1} for r in scan.records)


def test_iri_findings(mined):
    _, schema, report, store = mined
    statistics = report.config["dataset_statistics"]
    assert iri_findings(schema, statistics) == report.config["iri_findings"]
    assert data_iri_findings(store) == {"property": {}, "subject": {}, "object": {}}


def test_data_iri_findings_lists_terms_with_excluded_characters(tmp_path):
    from rdflib import URIRef

    graph = Graph()
    graph.add((URIRef("http://x.org/a b"), E.p, URIRef("http://x.org/c ")))
    graph.add((E.s, E.p, URIRef("http://x.org/a b")))
    found = data_iri_findings(store_from_graph(graph, tmp_path / "store"))
    # U+00A0 is allowed in IRIs (RFC 3987); the space is not
    spaced = {"http://x.org/a b": 1}
    assert found == {"property": {}, "subject": spaced, "object": spaced}
