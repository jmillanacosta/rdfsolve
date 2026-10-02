"""One rule for the namespace of a term, in Python and in SPARQL (rdfsolve.ontology.terms).

The ontology code had three rules: OBO prefixes upper-cased (FBbt became FBBT), any IRI cut at
its last "_" (restriction patterns), and the IRI without its last part; now there is one."""

import pyoxigraph as ox
import pytest

from rdfsolve.ontology.terms import namespace, namespace_expression, obo_prefix

OBO = "http://purl.obolibrary.org/obo/"
CASES = {
    OBO + "MONDO_0000001": OBO + "MONDO_",
    OBO + "FBbt_00001234": OBO + "FBbt_",
    OBO + "NCBITaxon_9606": OBO + "NCBITaxon_",
    OBO + "go.owl": OBO,
    "http://purl.obolibrary.org/obo/chebi/ontology_term": "http://purl.obolibrary.org/obo/chebi/",
    "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C12345": (
        "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"
    ),
    "http://example.org/my_term": "http://example.org/",
    "urn:term:ethanol": "urn:term:",
}


@pytest.mark.parametrize(("iri", "expected"), sorted(CASES.items()))
def test_python_and_sparql_give_the_same_namespace(iri, expected):
    assert namespace(iri) == expected
    query = f"SELECT ({namespace_expression('?t')} AS ?ns) WHERE {{ BIND(<{iri}> AS ?t) }}"
    (row,) = ox.Store().query(query)
    assert row["ns"].value == expected


def test_the_obo_prefix_is_kept_as_written():
    assert obo_prefix(OBO + "FBbt_00001234") == "FBbt"
    assert obo_prefix("http://example.org/my_term") is None
