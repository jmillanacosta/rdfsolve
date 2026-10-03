import json
from unittest.mock import Mock

from rdflib import RDF, Dataset, Namespace, URIRef
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.pattern_enrichment import enrich_patterns_with_labels, query_class_entity_counts
from rdfsolve.sparql_helper import EndpointRateLimitError
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


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
    helper.select.side_effect = EndpointRateLimitError("Host cooldown exceeds wait budget: example.org")
    miner = SchemaMiner("https://example.org/sparql", counts=False)
    miner._init_report("test", "test", "2026-09-07T00:00:00+00:00")
    patterns = [
        SchemaPattern(subject_class=f"urn:C{i}", property_uri="urn:p", object_class="Literal")
        for i in range(120)  # three batches of labels
    ]
    assert len(enrich_patterns_with_labels(patterns, helper, None, miner._report)) == 120
    assert helper.select.call_count == 1, "The other batches are not sent to a host that refuses"
