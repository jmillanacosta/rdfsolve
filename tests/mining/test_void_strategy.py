"""rdfsolve.mining.void_strategy: the schema of a source is read from the VoID its endpoint
publishes; what the VoID's counts leave without a class is found and mined lightly; the
largest patterns are re-counted on the endpoint (drift), with examples, class samples and
language tags."""

from rdflib import RDF, Dataset, Graph, Literal, Namespace, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.void_strategy import VoidStrategy, void_gaps
from rdfsolve.schema_models import UNTYPED_SUBJECT
from rdfsolve.schema_models.readers.void import scope_void_graph, void_datasets_of_graphs

E = Namespace("urn:ex:")
G1, G2 = "urn:g1", "urn:g2"
LANG = str(RDF.langString)

VOID = """
@prefix void: <http://rdfs.org/ns/void#> .
@prefix void-ext: <http://ldf.fi/void-ext#> .
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex: <urn:ex:> .

[] sd:namedGraph [ sd:name <urn:g1> ; sd:graph <urn:void:g1> ] .
<urn:void:g1> void:classPartition <urn:cp:A> ;
    void:propertyPartition [ void:property ex:link ; void:triples 3 ] .
<urn:cp:A> void:class ex:A ; void:entities 1 ;
    void:propertyPartition <urn:pp:link> , <urn:pp:name> ; void:subset <urn:ls:AB> .
<urn:pp:link> void:property ex:link ; void:triples 2 ; void:distinctObjects 2 ;
    void-ext:distinctIRIReferenceObjects 2 ; void-ext:distinctLiterals 0 .
<urn:pp:name> void:property ex:name ; void:triples 1 ;
    void-ext:datatypePartition [ void-ext:datatype rdf:langString ; void:triples 1 ] .
<urn:ls:AB> a void:Linkset ; void:subjectsTarget <urn:cp:A> ; void:linkPredicate ex:link ;
    void:objectsTarget <urn:cp:B> ; void:triples 1 .
<urn:cp:B> void:class ex:B .
"""


def _data() -> Dataset:
    data = Dataset(default_union=True)
    g1, g2 = data.graph(URIRef(G1)), data.graph(URIRef(G2))
    g1.add((E.a1, RDF.type, E.A))
    g1.add((E.a1, E.link, E.b1))
    g1.add((E.a1, E.link, E.u1))
    g1.add((E.a1, E.name, Literal("x", lang="en")))
    g1.add((E.s1, E.link, E.b1))
    g2.add((E.b1, RDF.type, E.B))
    return data


def _strategy() -> VoidStrategy:
    void = Graph().parse(data=VOID, format="turtle")
    scoped = scope_void_graph(void, void_datasets_of_graphs(void, [G1]))
    return VoidStrategy(scoped, void_graph="urn:void", issued="2026-09-28", drift_random=0)


def test_the_gaps_come_from_the_counts_of_the_void():
    gaps = {(g.kind, g.subject_class, g.triples) for g in void_gaps(_strategy().void)}
    assert gaps == {("objects", str(E.A), 1), ("subjects", None, 1)}


def test_the_schema_is_read_from_the_void_and_its_gaps_are_mined():
    strategy = _strategy()
    miner = SchemaMiner.from_graph(_data(), graph_uris=[G1], strategy=strategy, delay=0)
    try:
        schema = miner.mine(dataset_name="fixture")
    finally:
        miner.close()
    found = {
        (p.subject_class, p.property_uri, p.object_class, p.count, p.evidence_source)
        for p in schema.patterns
    }
    assert found == {
        (str(E.A), str(E.link), str(E.B), 1, "void"),
        (str(E.A), str(E.name), "Literal", 1, "void"),
        (str(E.A), str(E.link), "Resource", None, "mined"),
        (UNTYPED_SUBJECT, str(E.link), "Resource", None, "mined"),
    }, "The link to B is typed in another graph; u1 has no class; s1 has no class"
    (untyped,) = [p for p in schema.patterns if p.untyped_subject]
    assert untyped.subject_binding == "untyped", "A subject gap gives an untyped pattern"
    assert schema.about.class_entity_counts == {str(E.A): 1}
    record = miner.last_report.config["void_source"]
    assert [s["objects_by_class"] for s in record["object_samples"]] == [
        {str(E.B): 1, "Resource": 1}
    ], "A sample of the objects: b1 has class B (in another graph), u1 none"
    assert [
        (s["property"], s["triples_at_least"], s["example"]["subject"]["value"])
        for s in record["untyped_subjects"]
    ] == [(str(E.link), 1, str(E.s1))]
    assert record["languages"]["tags"] == {f"{E.A} {E.name}": {"en": 1}}
    assert {(r["object"], r["ratio"]) for r in record["drift"]["patterns"]} == {
        (str(E.B), 1.0),
        ("Literal", 1.0),
    }
    assert schema.enrichment.class_examples[str(E.A)][0].value == str(E.a1)
    assert {e.property_uri for e in schema.enrichment.examples} == {str(E.link), str(E.name)}


def test_a_purpose_whose_queries_the_endpoint_cuts_is_stopped_and_recorded(monkeypatch):
    """Drift re-counts that a gateway cuts at a fixed limit stop once the limit is shown; the
    record and a measurement gap say why, with the queries cut and not sent, and the schema
    from the VoID stands."""
    from functools import partial

    from rdfsolve.mining import void_strategy
    from rdfsolve.sparql_helper import EndpointError, QueryCut, QueryCuts

    monkeypatch.setattr(void_strategy, "QueryCuts", partial(QueryCuts, after=1))
    strategy = _strategy()
    miner = SchemaMiner.from_graph(_data(), graph_uris=[G1], strategy=strategy, delay=0)
    select = miner.helper.select
    sent = []

    def gateway(query, purpose=""):
        sent.append(purpose)
        if purpose == "void/drift":
            error = EndpointError("HTTP 524: A timeout occurred")
            error.cut = QueryCut("HTTP 524", 120.0)
            raise error
        return select(query, purpose=purpose)

    miner.helper.select = gateway
    try:
        schema = miner.mine(dataset_name="fixture")
    finally:
        miner.close()
    assert sent.count("void/drift") == 1, "Two re-counts: one cut, one not sent"
    record = miner.last_report.config["void_source"]
    stop = {
        "stopped": "endpoint cuts queries at 120 s",
        "cut_by": "HTTP 524",
        "queries_cut": 1,
        "queries_not_sent": 1,
    }
    assert record["stopped"] == {"void/drift": stop}
    assert record["drift"]["stopped"] == stop["stopped"] and record["drift"]["measured"] == 0
    gaps = [g for g in miner.last_report.measurement_gaps if g.purpose == "void/drift"]
    assert any("1 not sent" in g.message for g in gaps)
    # Three from the VoID and its object gap; one of the IRI subject without a class (s1).
    assert len(schema.patterns) == 4 and not miner.last_report.query_failures
    assert sum(p.untyped_subject for p in schema.patterns) == 1


def test_a_void_term_that_is_not_an_rdf_iri_is_left_out_and_recorded():
    import logging

    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    strategy = _strategy()
    space = " http://identifiers.org/obo.aeo/"  # lsr's namespace (L21)
    strategy.void.add(
        (URIRef("urn:void:g1"), URIRef("http://rdfs.org/ns/void#vocabulary"), URIRef(space))
    )
    miner = SchemaMiner.from_graph(_data(), graph_uris=[G1], strategy=strategy, delay=0)
    try:
        schema = miner.mine(dataset_name="fixture")
    finally:
        miner.close()
    assert miner.last_report.config["iri_findings"]["graphs"]["void"] == {
        "triples_left_out": 1,
        "terms": [{"iri": space, "triples": 1}],
    }
    assert {p.evidence_source for p in schema.patterns} == {"void", "mined"}
