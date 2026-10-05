"""rdfsolve.schema_models.iri_quality: terms of a source that are not RDF IRIs (a space in them)
are mined, counted and kept in the JSON schema, reported as data-quality findings with the
queries that find them, and left out of the RDF outputs, which then serialize."""

import json
import logging

from rdflib import RDF, Dataset, Graph, Literal, URIRef

from rdfsolve.mining.miner import SchemaMiner

A, BAD_P, BAD_C, BAD_O = "urn:A", "urn:bad name", "urn:Bad Class", "urn:o x"


def mined():
    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    data = Dataset()
    for s, p, o in (
        ("urn:a", RDF.type, URIRef(A)),
        ("urn:a", BAD_P, Literal("x")),
        ("urn:a", "urn:p", URIRef(BAD_O)),
        ("urn:b", RDF.type, URIRef(BAD_C)),
        ("urn:b", "urn:q", Literal("y")),
    ):
        data.add((URIRef(s), URIRef(p), o))
    with SchemaMiner.from_graph(data, delay=0, enrich=True) as miner:  # as the pipeline
        return miner.mine("quality"), miner.last_report


def test_terms_that_are_not_rdf_iris_are_mined_reported_and_left_out_of_rdf():
    schema, report = mined()
    rows = {(p.subject_class, p.property_uri, p.object_class): p.count for p in schema.patterns}
    assert rows[A, BAD_P, "Literal"] == 1 and rows[BAD_C, "urn:q", "Literal"] == 1
    assert BAD_P in json.dumps(schema.to_dict()), "The JSON schema keeps the term as it is"
    findings = {f["iri"]: f for f in report.config["iri_findings"]["terms"]}
    assert set(findings) == {BAD_P, BAD_C}
    assert findings[BAD_P]["roles"] == ["property"] and findings[BAD_P]["patterns"] == 1
    assert findings[BAD_C]["roles"] == ["class"]
    assert all(f["in_rdf_outputs"] is False for f in findings.values())
    queries = report.config["iri_findings"]["queries"]
    assert set(queries) == {"property", "subject", "object"}
    found = {
        str(row.term) for row in Dataset().query(queries["property"])
    }  # an empty graph: the queries are valid SPARQL
    assert found == set()
    for text in (schema.to_void_graph().serialize(format="turtle"), schema.to_shacl()):
        graph = Graph().parse(data=text, format="turtle")
        terms = {str(t) for triple in graph for t in triple}
        assert not {BAD_P, BAD_C} & terms and A in terms
    assert BAD_P not in schema.to_linkml_yaml()


def test_enrichment_rdf_leaves_out_terms_that_are_not_rdf_iris_without_rdflib_warnings(caplog):
    """The examples and labels of such a term stay in the JSON schema; the RDF outputs do not
    build the term, so rdflib logs no warning for each of its examples (Bio2RDF BioModels)."""
    schema, _ = mined()
    enrichment = schema.enrichment
    assert BAD_C in enrichment.class_examples, "The JSON schema keeps the examples of the term"
    logging.getLogger("rdflib.term").setLevel(logging.WARNING)
    with caplog.at_level(logging.WARNING, logger="rdflib.term"):
        graph = enrichment.to_rdf_graph()
    assert not [r for r in caplog.records if r.name == "rdflib.term"]
    terms = {str(t) for triple in graph for t in triple}
    assert not {BAD_P, BAD_C, BAD_O} & terms and A in terms
