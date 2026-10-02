"""The ontology namespace of a term, which tells the namespaces used for typing (those with many
terms without a parent) from the vocabulary of the data, and is reported for each shape group."""

from rdfsolve.ontology.terms import namespace as term_namespace

OBO = "http://purl.obolibrary.org/obo/"
NCIT = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"


def test_namespaces_of_terms():
    assert term_namespace(OBO + "MONDO_0000001") == OBO + "MONDO_"
    assert term_namespace(NCIT + "C12345") == NCIT
    assert term_namespace("http://purl.bioontology.org/ontology/SNOMEDCT/123") == (
        "http://purl.bioontology.org/ontology/SNOMEDCT/"
    )
    assert term_namespace("urn:term:ethanol") == "urn:term:"
