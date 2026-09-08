"""Check entity denominators against RDF data with overlapping types and graphs."""

import json
from unittest.mock import Mock

from rdflib import Dataset, Namespace, RDF, URIRef

from rdfsolve.miner import SchemaMiner
from rdfsolve.mining.pattern_enrichment import query_class_entity_counts
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def test_entity_counts_are_distinct_scoped_and_queryable():
    data = Dataset()
    for graph in ("urn:g", "urn:h"):
        data.graph(URIRef(graph)).parse(
            data="""
            <urn:one> a <urn:A>; <urn:p> <urn:two> .
            <urn:two> a <urn:B>, <urn:C> .
        """,
            format="turtle",
        )
    data.graph(URIRef("urn:excluded")).add((URIRef("urn:other"), RDF.type, URIRef("urn:A")))
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        data.query(query).serialize(format="json")
    )
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    counts = query_class_entity_counts(
        ["urn:A", "urn:B", "urn:C", "urn:unused"],
        helper,
        ["urn:g", "urn:h"],
        miner._report,
        batch_size=2,
    )
    assert counts == {"urn:A": 1, "urn:B": 1, "urn:C": 1, "urn:unused": 0}
    schema = MinedSchema(
        about=AboutMetadata(dataset_name="test", class_entity_counts=counts),
        patterns=[
            SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class=c, count=1)
            for c in ("urn:B", "urn:C")
        ],
    )
    graph = schema.to_void_graph()
    rows = list(
        graph.query("""
      PREFIX void: <http://rdfs.org/ns/void#>
      SELECT ?subjectsCount ?objectClass ?objectsCount WHERE {
        ?cp void:class <urn:A>; void:entities ?subjectsCount; void:propertyPartition ?pp .
        ?pp void:property <urn:p>; void:classPartition ?op .
        ?op void:class ?objectClass; void:triples ?objectsCount .
      }
    """)
    )
    assert {(int(r.subjectsCount), str(r.objectClass), int(r.objectsCount)) for r in rows} == {
        (1, "urn:B", 1),
        (1, "urn:C", 1),
    }
    void = Namespace("http://rdfs.org/ns/void#")
    cp = graph.value(predicate=void["class"], object=URIRef("urn:A"))
    assert graph.value(cp, void.triples) is None
    assert not list(graph.subjects(RDF.type, void.Linkset))


def test_missing_count_is_unknown_and_marks_run_incomplete():
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    helper = Mock()
    helper.select.return_value = {"results": {"bindings": []}}
    assert query_class_entity_counts(["urn:A"], helper, None, miner._report) == {}
    assert miner._report.report.query_failures[0].classes == ["urn:A"]
