"""A QLever index answers the number of distinct subjects and of distinct objects of all its
triples from its metadata (Bgee, 715,849,799 subjects and 155,731,759 objects: under 0.1 s, job
114239), and the triples of each property in one grouped query. The counts are exact; when the
scope holds the whole index, they go to the schema and to VoID (void:triples,
void:distinctSubjects, void:distinctObjects, void:properties). RDFLib stands in for QLever here."""

from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import RDF, VOID, XSD

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models.exporters.void import to_void_graph
from rdfsolve.schema_models.readers.void import _extract_metadata_from_void
from rdfsolve.sparql_helper import EndpointError

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
    with SchemaMiner.from_graph(Dataset().parse(data=data, format=fmt), delay=0, graph_uris=graphs) as miner:
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
    for prop, n in ((VOID.triples, 7), (VOID.distinctSubjects, 3), (VOID.distinctObjects, 5), (VOID.properties, 3)):
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
    assert (back.triple_count_estimate, back.distinct_subject_count, back.distinct_object_count) == (7, 3, 5)


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
