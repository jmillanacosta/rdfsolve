"""rdfsolve.mining.dataset_statistics: distinct subjects, objects and counts of a dataset, from an
index's metadata where it has them, under a time budget, and the entity counts of classes."""

import json
from unittest.mock import Mock

from rdflib import RDF, Dataset, Literal, Namespace, URIRef
from rdflib.namespace import VOID, XSD

from rdfsolve.mining import dataset_statistics
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.pattern_enrichment import (
    enrich_patterns_with_labels,
    query_class_entity_counts,
)
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.exporters.void import to_void_graph
from rdfsolve.schema_models.readers.void import _extract_metadata_from_void
from rdfsolve.sparql_helper import EndpointError, EndpointRateLimitError

DATA = """
<urn:a> a <urn:A> ; <urn:p> <urn:b> , <urn:c> .
<urn:b> a <urn:B> ; <urn:q> "x" .
<urn:c> <urn:p> <urn:b> ; <urn:q> "x" .
"""
GRAPHS = """
<urn:g1> { <urn:a> a <urn:A> ; <urn:p> <urn:b> . }
<urn:g2> { <urn:b> a <urn:B> ; <urn:q> "x" . }
"""


def mine(monkeypatch, data=DATA, *, engine="qlever", graphs=None, fmt="turtle"):
    with SchemaMiner.from_graph(
        Dataset().parse(data=data, format=fmt), delay=0, graph_uris=graphs
    ) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        schema = miner.mine("statistics")
        return schema, miner.last_report.config["dataset_statistics"]


def test_a_qlever_index_gives_exact_dataset_statistics(monkeypatch):
    schema, record = mine(monkeypatch)
    about = schema.about
    assert record["state"] == "counted"
    assert (about.triple_count_estimate, about.distinct_subject_count) == (7, 3)
    assert (about.distinct_object_count, about.distinct_predicate_count) == (5, 3)
    void = to_void_graph(schema)
    (dataset,) = void.subjects(VOID.properties, None)
    for prop, n in (
        (VOID.triples, 7),
        (VOID.distinctSubjects, 3),
        (VOID.distinctObjects, 5),
        (VOID.properties, 3),
    ):
        assert set(void.objects(dataset, prop)) == {Literal(n, datatype=XSD.integer)}, prop
    assert about.property_partitions == {
        str(RDF.type): {"triples": 2, "distinct_subjects": 2, "distinct_objects": 2},
        "urn:p": {"triples": 3, "distinct_subjects": 2, "distinct_objects": 2},
        "urn:q": {"triples": 2, "distinct_subjects": 2, "distinct_objects": 1},
    }
    partitions = set(void.objects(dataset, VOID.propertyPartition))
    (partition,) = {pp for pp in partitions if (pp, VOID.property, URIRef("urn:q")) in void}
    for prop, n in ((VOID.triples, 2), (VOID.distinctSubjects, 2), (VOID.distinctObjects, 1)):
        assert set(void.objects(partition, prop)) == {Literal(n, datatype=XSD.integer)}, prop
    back = _extract_metadata_from_void(void)
    assert (
        back.triple_count_estimate,
        back.distinct_subject_count,
        back.distinct_object_count,
    ) == (7, 3, 5)


def test_other_engines_and_part_of_an_index_are_not_counted(monkeypatch):
    schema, record = mine(monkeypatch, engine="virtuoso")
    assert record["state"] == "not_counted" and "QLever" in record["reason"]
    assert schema.about.triple_count_estimate is None and schema.about.distinct_object_count is None
    schema, record = mine(monkeypatch, GRAPHS, graphs=["urn:g1"], fmt="trig")
    assert record["state"] == "not_counted" and "whole index" in record["reason"]
    assert schema.about.distinct_subject_count is None and schema.about.property_partitions is None
    assert not set(to_void_graph(schema).subjects(VOID.distinctSubjects, None))


def test_a_refused_property_count_keeps_the_other_counts(monkeypatch):

    def refuse(run):
        def call(query, *args, **kwargs):
            if "<urn:q>" in query and "COUNT(DISTINCT ?o)" in query:
                raise EndpointError("HTTP 500: Tried to allocate 54 GB")
            return run(query, *args, **kwargs)

        return call

    with SchemaMiner.from_graph(Dataset().parse(data=DATA, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        monkeypatch.setattr(miner.helper, "select", refuse(miner.helper.select))
        schema = miner.mine("statistics")
        record = miner.last_report.config["dataset_statistics"]
    assert record["state"] == "counted" and "54 GB" in record["refused_partitions"]["urn:q"]
    assert schema.about.distinct_object_count == 5
    assert schema.about.property_partitions["urn:q"] == {"triples": 2, "distinct_subjects": 2}


def test_a_grouped_count_without_solutions_counts_nothing(monkeypatch):
    """RDFLib (used for overlapping graphs) answers a grouped count without solutions with one
    empty row; here the default graph, which the counts read, is empty."""
    overlapping = "<urn:g> { <urn:a> <urn:p> <urn:b> . } <urn:h> { <urn:a> <urn:p> <urn:b> . }"
    schema, record = mine(monkeypatch, overlapping, graphs=["urn:g", "urn:h"], fmt="trig")
    assert record["state"] == "counted" and record["property_partitions"] == {}
    assert schema.about.triple_count_estimate == 0


def test_properties_after_the_time_budget_keep_their_triples(monkeypatch):
    monkeypatch.setattr(dataset_statistics, "PARTITION_BUDGET_S", -1.0)
    schema, record = mine(monkeypatch)
    assert record["state"] == "counted" and schema.about.distinct_subject_count == 3
    assert schema.about.property_partitions["urn:p"] == {"triples": 3}
    assert "time budget" in record["refused_partitions"]["urn:p"]


def test_entity_counts_are_distinct_scoped_and_queryable():
    data = Dataset()
    for graph in ("urn:g", "urn:h"):
        data.graph(URIRef(graph)).parse(
            data="\n            <urn:one> a <urn:A>; <urn:p> <urn:two> .\n            <urn:two> a <urn:B>, <urn:C> .\n        ",
            format="turtle",
        )
    data.graph(URIRef("urn:excluded")).add((URIRef("urn:other"), RDF.type, URIRef("urn:A")))
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        data.query(query).serialize(format="json")
    )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    states = {}
    counts = query_class_entity_counts(
        ["urn:A", "urn:B", "urn:C", "urn:unused"],
        helper,
        ["urn:g", "urn:h"],
        miner._report,
        batch_size=2,
        states_out=states,
    )
    assert counts == {"urn:A": 1, "urn:B": 1, "urn:C": 1, "urn:unused": 0}
    assert states == {
        "urn:A": "complete",
        "urn:B": "complete",
        "urn:C": "complete",
        "urn:unused": "complete",
    }
    schema = MinedSchema(
        about=AboutMetadata(dataset_name="test", class_entity_counts=counts),
        patterns=[
            SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class=c, count=1)
            for c in ("urn:B", "urn:C")
        ],
    )
    graph = schema.to_void_graph()
    rows = list(
        graph.query(
            "\n      PREFIX void: <http://rdfs.org/ns/void#>\n      SELECT ?subjectsCount ?objectClass ?objectsCount WHERE {\n        ?cp void:class <urn:A>; void:entities ?subjectsCount; void:propertyPartition ?pp .\n        ?pp void:property <urn:p>; void:classPartition ?op .\n        ?op void:class ?objectClass; void:triples ?objectsCount .\n      }\n    "
        )
    )
    assert {(int(r.subjectsCount), str(r.objectClass), int(r.objectsCount)) for r in rows} == {
        (1, "urn:B", 1),
        (1, "urn:C", 1),
    }
    void = Namespace("http://rdfs.org/ns/void#")
    cp = graph.value(predicate=void["class"], object=URIRef("urn:A"))
    assert graph.value(cp, void.triples) is None
    assert not list(graph.subjects(RDF.type, void.Linkset))


def test_labels_stop_at_the_first_rate_limit():
    helper = Mock()
    helper.select.side_effect = EndpointRateLimitError(
        "Host cooldown exceeds wait budget: example.org"
    )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    patterns = [
        SchemaPattern(subject_class=f"urn:C{i}", property_uri="urn:p", object_class="Literal")
        for i in range(120)  # three batches of labels
    ]
    assert len(enrich_patterns_with_labels(patterns, helper, None, miner._report)) == 120
    assert helper.select.call_count == 1, "The other batches are not sent to a host that refuses"


def test_a_property_that_is_not_an_rdf_iri_is_counted(monkeypatch):
    """A property IRI with a space is counted like any other (the helper writes it with IRI())."""
    a, bad = URIRef("urn:a"), URIRef("urn:bad name")
    graph = Dataset()
    for triple in (
        (a, URIRef("urn:p"), URIRef("urn:b")),
        (a, bad, Literal("x")),
        (a, bad, Literal("y")),
    ):
        graph.add(triple)
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", "qlever")
        miner.mine("statistics")
        record = miner.last_report.config["dataset_statistics"]
    assert record["state"] == "counted" and not record["refused_partitions"]
    assert record["property_partitions"]["urn:bad name"] == {
        "triples": 2,
        "distinct_subjects": 1,
        "distinct_objects": 2,
    }
