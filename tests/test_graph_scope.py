"""Attribute mined patterns to the graph holding the edge, across graph boundaries."""

import json
from unittest.mock import Mock

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.mining.graph_selection import discover_data_graphs, missing_graphs
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.pattern_enrichment import enrich_patterns_with_counts
from rdfsolve.mining.query_builders import (
    _build_batched_typed_count_query,
    _build_batched_typed_object_query,
    _graph_scope,
)
from rdfsolve.schema_models import SchemaPattern

GRAPHS = ["urn:g:types", "urn:g:edges"]


def _spanning_dataset() -> Dataset:
    """Type triples in one graph, the edge between them in another."""
    data = Dataset()
    data.graph(URIRef("urn:g:types")).parse(
        data="<urn:s> a <urn:C> . <urn:o> a <urn:D> .", format="turtle"
    )
    data.graph(URIRef("urn:g:edges")).parse(data="<urn:s> <urn:p> <urn:o> .", format="turtle")
    data.graph(URIRef("urn:g:engine")).parse(
        data="<urn:sys> a <urn:SystemClass> .", format="turtle"
    )
    return data


def _helper(data: Dataset) -> Mock:
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        data.query(query).serialize(format="json")
    )
    return helper


def test_graph_scope_merges_for_lookups_and_names_the_edge_graph():
    dataset, opening, closing = _graph_scope(GRAPHS)
    assert dataset == (
        "FROM <urn:g:types> FROM <urn:g:edges> "
        "FROM NAMED <urn:g:types> FROM NAMED <urn:g:edges>"
    )
    assert (opening, closing) == ("GRAPH ?_g {", "}")
    assert _graph_scope(None) == ("", "", "")
    assert _graph_scope([]) == ("", "", "")


def test_only_the_edge_is_graph_scoped():
    query = _build_batched_typed_object_query(["urn:C"], GRAPHS)
    assert "GRAPH ?_g { ?s ?p ?o . }" in query
    assert "GRAPH ?_g { ?s a ?class" not in query
    assert "FROM NAMED <urn:g:edges>" in query


def test_counts_group_by_the_edge_graph_only_when_scoped():
    assert "GROUP BY ?class ?p ?oc ?_g" in _build_batched_typed_count_query(["urn:C"], GRAPHS)
    assert "GROUP BY ?class ?p ?oc" in _build_batched_typed_count_query(["urn:C"], None)
    assert "?_g" not in _build_batched_typed_count_query(["urn:C"], None)


def test_a_shape_spanning_graphs_is_retained_and_attributed():
    data = _spanning_dataset()
    rows = data.query(_build_batched_typed_object_query(["urn:C"], GRAPHS))
    assert [tuple(str(term) for term in row) for row in rows] == [("urn:C", "urn:p", "urn:D")]
    scoped = data.query("SELECT ?p WHERE { GRAPH ?g { ?s a <urn:C> . ?s ?p ?o . ?o a <urn:D> } }")
    assert len(scoped) == 0, "a single GRAPH block loses the shape; that is the regression"


def test_counts_land_in_the_per_graph_map():
    data = _spanning_dataset()
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    helper = _helper(data)
    patterns = enrich_patterns_with_counts(
        [SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D")],
        helper,
        GRAPHS,
        miner._report,
        lambda query, purpose, chunk=None: helper.select(query)["results"]["bindings"],
        class_batch_size=5,
        class_chunk_size=None,
        unsafe_paging=False,
        delay=0,
    )
    assert patterns[0].count == 1
    assert patterns[0].graphs == {"urn:g:edges": 1}


def test_missing_graphs_are_named():
    helper = _helper(_spanning_dataset())
    assert missing_graphs(helper, GRAPHS) == []
    assert missing_graphs(helper, ["urn:g:types", "urn:g:absent"]) == ["urn:g:absent"]


def test_discovery_drops_excluded_prefixes(monkeypatch):
    monkeypatch.setattr(
        "rdfsolve.void_retrieval.discover_graph_names",
        lambda helper, **kwargs: ["urn:g:edges", "urn:g:engine"],
    )
    assert discover_data_graphs(Mock(), excluded_prefixes=("urn:g:engine",)) == ["urn:g:edges"]


def test_a_configured_graph_the_endpoint_lacks_stops_the_run():
    miner = SchemaMiner("https://example.org/sparql", graph_uris=["urn:g:absent"])
    miner._helper = _helper(_spanning_dataset())
    with pytest.raises(ValueError, match="no triples in the selected graphs"):
        miner.mine(dataset_name="test")


def test_an_endpoint_without_types_is_not_reported_complete():
    data = Dataset()
    data.graph(URIRef("urn:g:data")).parse(data="<urn:s> <urn:p> <urn:o> .", format="turtle")
    helper = _helper(data)
    helper.select_chunked.side_effect = lambda query, **kwargs: iter(
        [
            json.loads(
                data.query(
                    "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"
                ).serialize(format="json")
            )["results"]["bindings"]
        ]
    )
    miner = SchemaMiner("https://example.org/sparql")
    miner._helper = helper
    schema = miner.mine(dataset_name="no-types")
    assert schema.patterns == []
    assert miner.last_report.completion_state == "failed"
    assert "No data classes found" in miner.last_report.abort_reason


def test_pattern_counts_populate_distinct_subjects_and_objects_for_single_scope():
    data = Dataset()
    graph = data.graph(URIRef("urn:g:one"))
    graph.parse(
        data="""
        <urn:s1> a <urn:C>; <urn:p> <urn:o1>, <urn:o2> .
        <urn:s2> a <urn:C>; <urn:p> <urn:o2> .
        <urn:o1> a <urn:D> .
        <urn:o2> a <urn:D> .
        """,
        format="turtle",
    )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-21T00:00:00+00:00")
    helper = _helper(data)
    patterns = enrich_patterns_with_counts(
        [SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D")],
        helper,
        ["urn:g:one"],
        miner._report,
        lambda query, purpose, chunk=None: helper.select(query)["results"]["bindings"],
        class_batch_size=5,
        class_chunk_size=None,
        unsafe_paging=False,
        delay=0,
    )
    assert patterns[0].count == 3
    assert patterns[0].distinct_subjects == 2
    assert patterns[0].distinct_objects == 2


def test_pattern_distinct_counts_are_not_summed_across_named_graphs():
    data = Dataset()
    for graph_iri in ("urn:g:one", "urn:g:two"):
        graph = data.graph(URIRef(graph_iri))
        graph.parse(
            data="""
            <urn:s> a <urn:C>; <urn:p> <urn:o> .
            <urn:o> a <urn:D> .
            """,
            format="turtle",
        )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-21T00:00:00+00:00")
    helper = _helper(data)
    patterns = enrich_patterns_with_counts(
        [SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D")],
        helper,
        ["urn:g:one", "urn:g:two"],
        miner._report,
        lambda query, purpose, chunk=None: helper.select(query)["results"]["bindings"],
        class_batch_size=5,
        class_chunk_size=None,
        unsafe_paging=False,
        delay=0,
    )
    assert patterns[0].count == 2
    assert patterns[0].graphs == {"urn:g:one": 1, "urn:g:two": 1}
    assert patterns[0].distinct_subjects is None
    assert patterns[0].distinct_objects is None
