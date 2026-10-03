"""rdfsolve.ontology.terms: term namespaces, read in Python and in SPARQL, and namespace groups."""

import pyoxigraph as ox
import pytest

from rdfsolve.ontology.terms import namespace, namespace_expression, obo_prefix
from rdfsolve.ontology.terms import namespace as term_namespace

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


NCIT = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"


def test_namespaces_of_terms():
    assert term_namespace(OBO + "MONDO_0000001") == OBO + "MONDO_"
    assert term_namespace(NCIT + "C12345") == NCIT
    assert term_namespace("http://purl.bioontology.org/ontology/SNOMEDCT/123") == (
        "http://purl.bioontology.org/ontology/SNOMEDCT/"
    )
    assert term_namespace("urn:term:ethanol") == "urn:term:"
