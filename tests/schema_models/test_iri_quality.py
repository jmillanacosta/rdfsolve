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


def test_graph_findings_count_the_triples_that_rdf_terms_only_leaves_out():
    from rdfsolve.schema_models.iri_quality import graph_findings, rdf_terms_only

    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    graph = Graph()
    graph.add((URIRef("urn:d"), URIRef("urn:ns"), URIRef(" http://example.org/ns/")))
    graph.add((URIRef(BAD_O), URIRef(BAD_P), URIRef(" http://example.org/ns/")))
    graph.add((URIRef("urn:d"), URIRef("urn:title"), Literal("x")))
    found = graph_findings(graph)
    assert found == {" http://example.org/ns/": 2, BAD_O: 1, BAD_P: 1}
    assert len(rdf_terms_only(graph)) == 1
    assert graph_findings(graph) == {}


LSR_NS = " http://identifiers.org/obo.aeo/"  # lsr's namespace with a leading space (L21)


def metadata_with_bad_term():
    from rdfsolve.schema_models.metadata import MetadataDocument

    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    graph = Graph()
    graph.add((URIRef("urn:d"), RDF.type, URIRef("http://rdfs.org/ns/void#Dataset")))
    graph.add((URIRef("urn:d"), URIRef("http://rdfs.org/ns/void#vocabulary"), URIRef(LSR_NS)))
    return MetadataDocument(graph=graph, scope="retained VoID RDF")


def test_retained_metadata_leaves_out_and_records_terms_that_are_not_rdf_iris():
    from rdfsolve.schema_models.metadata import RetainedMetadata

    document = metadata_with_bad_term()
    retained = RetainedMetadata.from_document(document)  # raised before
    assert retained.iri_findings == {
        "triples_left_out": 1,
        "terms": [{"iri": LSR_NS, "triples": 1}],
    }
    assert len(retained.to_document().graph) == 1
    assert len(document.graph) == 2  # the evidence in memory is not changed
    again = RetainedMetadata.model_validate_json(retained.model_dump_json())
    assert again.iri_findings == retained.iri_findings
    good = RetainedMetadata.from_document(
        type(document)(graph=Graph().add((URIRef("urn:a"), RDF.type, URIRef("urn:A"))), scope="x")
    )
    assert good.iri_findings is None


def test_metadata_trig_leaves_out_quads_whose_terms_or_graph_names_are_not_rdf_iris():
    from rdfsolve.schema_models.metadata import MetadataDocument, RetainedMetadata

    logging.getLogger("rdflib.term").setLevel(logging.ERROR)
    data = Dataset()
    data.add((URIRef("urn:a"), RDF.type, URIRef("urn:A"), URIRef("urn:g")))
    data.add((URIRef("urn:a"), URIRef("urn:p"), URIRef(LSR_NS), URIRef("urn:g")))
    data.add((URIRef("urn:b"), RDF.type, URIRef("urn:A"), URIRef("urn:g h")))
    document = MetadataDocument(graph=Graph(), rdf_dataset=data, scope="x")
    retained = RetainedMetadata.from_document(document)
    assert retained.format == "trig"
    assert retained.iri_findings == {
        "triples_left_out": 2,
        "terms": [{"iri": LSR_NS, "triples": 1}, {"iri": "urn:g h", "triples": 1}],
    }
    restored = retained.to_document()
    assert {str(g) for g in restored.graph.subjects()} == {"urn:a"}
    assert "urn:g" in {str(c.identifier) for c in restored.rdf_dataset.contexts()}


def test_void_with_a_term_that_is_not_an_rdf_iri_is_read():
    from rdfsolve.schema_models.readers.void import void_graph_to_minedschema

    schema = void_graph_to_minedschema(metadata_with_bad_term().graph)  # raised before
    assert schema.source_metadata.iri_findings["terms"] == [{"iri": LSR_NS, "triples": 1}]


def test_mining_result_export_leaves_out_and_records_terms_that_are_not_rdf_iris(tmp_path):
    from rdfsolve.schema_models import MiningResult

    schema, _ = mined()
    result = MiningResult(data_schema=schema, metadata=metadata_with_bad_term())
    found = result.export(tmp_path)  # raised before
    assert found == {"metadata": {"triples_left_out": 1, "terms": [{"iri": LSR_NS, "triples": 1}]}}
    assert json.loads((tmp_path / "iri_findings.json").read_text()) == found
    assert len(Graph().parse(tmp_path / "metadata.ttl")) == 1
    assert len(Graph().parse(tmp_path / "schema.ttl"))
