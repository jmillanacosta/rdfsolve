"""Observed ontology records retain their properties and counts."""

from rdflib import Dataset, URIRef
from rdflib.namespace import OWL, RDFS, XSD

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner


def test_ontology_records_survive_mining():
    data = Dataset()
    data.graph(URIRef("urn:data")).parse(data='''
        @prefix ex: <https://example.org/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        ex:Water a owl:Class; rdfs:label "Water"; ex:formula "H2O" .
        ex:Salt a owl:Class; rdfs:label "Salt"; ex:formula "NaCl" .
        ex:Category a rdfs:Class; ex:code "C" .
        ex:formula a owl:AnnotationProperty; rdfs:label "Formula" .
        ex:item a ex:Record; ex:name "A record" .
        ex:shape a sh:NodeShape; sh:targetClass ex:Record .
    ''', format="turtle")
    data.graph(URIRef("urn:context")).parse(data='''
        <urn:outside> a <http://www.w3.org/2002/07/owl#Class>;
            <urn:context-only> "Context" .
    ''', format="turtle")
    for strategy in ("two-phase", "single-pass", "one-shot"):
        for term_probes in (False, True):
            with SchemaMiner.from_graph(
                data, graph_uris=["urn:data"], strategy=strategy, delay=0
            ) as miner:
                schema = mine_with_ontology(
                    miner, ontology_as_data=term_probes
                ).data_schema
                patterns = {
                    (p.subject_class, p.property_uri): p
                    for p in schema.patterns if p.object_class == "Literal"
                }
                expected = {
                    (str(OWL.Class), "https://example.org/formula"): 2,
                    (str(OWL.Class), str(RDFS.label)): 2,
                    (str(RDFS.Class), "https://example.org/code"): 1,
                    (str(OWL.AnnotationProperty), str(RDFS.label)): 1,
                    ("https://example.org/Record", "https://example.org/name"): 1,
                }
                for key, count in expected.items():
                    assert key in patterns, (strategy, term_probes, "missing record", key)
                    pattern = patterns[key]
                    assert (
                        pattern.count, pattern.distinct_subjects,
                        pattern.datatype, pattern.graphs, pattern.evidence_source
                    ) == (count, count, str(XSD.string), {"urn:data": count}, "mined"), (
                        strategy, term_probes, key, pattern
                    )
                assert all(
                    p.property_uri != "urn:context-only"
                    for p in schema.patterns
                ), (strategy, term_probes, "context edge leaked")
                assert any(p.subject_class == "http://www.w3.org/ns/shacl#NodeShape"
                           and p.property_uri == "http://www.w3.org/ns/shacl#targetClass"
                           for p in schema.patterns), (strategy, term_probes, "missing shape record")
                assert schema.about.class_entity_counts[str(OWL.Class)] == 2
                assert miner.last_report.completion_state == "complete"
