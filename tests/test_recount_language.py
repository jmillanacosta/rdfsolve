"""A literal with a datatype other than rdf:langString has no language tag, so the recount of its
pattern tests the datatype alone. Virtuoso answers 0 for FILTER(DATATYPE(?o) = xsd:integer) with
FILTER(LANG(?o) = "") on the SIBiLS dc:extent edges, and 771 for either filter alone."""

from rdfsolve.mining.structural_strategy import structural_queries
from rdfsolve.schema_models.structural import StructuralPattern

XSD = "http://www.w3.org/2001/XMLSchema#"
LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"


def pattern(datatype, language):
    return StructuralPattern(
        subject_properties=["urn:p"], object_properties=[], subject_kind="IRI", object_kind="Literal",
        property_uri="urn:p", datatype=datatype, language=language, graph_uri=None, type_graph_uris=[],
        object_type_graph_uris=[], covered_types=[], subject_selection="untyped", count=1,
        distinct_subjects=1, distinct_objects=1, witness_query="", recount_query="",
    )


def test_the_language_is_tested_only_for_a_language_string():
    for datatype in (XSD + "integer", XSD + "dateTime", XSD + "string"):
        witness, recount = structural_queries(pattern(datatype, ""))
        assert f"DATATYPE(?o) = <{datatype}>" in recount and "LANG(?o)" not in recount + witness
    witness, recount = structural_queries(pattern(LANG_STRING, "en"))
    assert 'FILTER(LANG(?o) = "en")' in recount and 'FILTER(LANG(?o) = "en")' in witness
