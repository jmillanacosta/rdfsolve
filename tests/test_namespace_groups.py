"""Ontology terms without a parent cannot be lifted to an ancestor. Before mining, those that
stand only for themselves are grouped by the ontology namespace that they come from, when the
namespace has many of them (a namespace with few is more likely the vocabulary of the data);
each group is recorded with its namespace, so that the rows of a group are not read as a class."""

from rdfsolve.config import mint
from rdfsolve.mining.ontology_as_data import choose_representatives, group_by_namespace, term_namespace

OBO = "http://purl.obolibrary.org/obo/"
NCIT = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"


def test_namespaces_of_terms():
    assert term_namespace(OBO + "MONDO_0000001") == OBO + "MONDO_"
    assert term_namespace(NCIT + "C12345") == NCIT
    assert term_namespace("http://purl.bioontology.org/ontology/SNOMEDCT/123") == (
        "http://purl.bioontology.org/ontology/SNOMEDCT/"
    )
    assert term_namespace("urn:term:ethanol") == "urn:term:"


def test_parentless_terms_are_grouped_by_namespace():
    data = "http://example.org/vocab#Compound"
    terms = [OBO + "CHEBI_1", OBO + "CHEBI_2", OBO + "MONDO_1", OBO + "MONDO_2", NCIT + "C1", NCIT + "C2", data]
    parents = {OBO + "CHEBI_1": {OBO + "CHEBI_0"}, OBO + "CHEBI_2": {OBO + "CHEBI_0"}}
    chosen = choose_representatives(terms, parents, budget=2)
    assert chosen.over_budget and chosen.classes_after == 6
    groups = group_by_namespace(chosen, parents, min_terms=2)
    mondo, ncit = mint("term-group", OBO + "MONDO_"), mint("term-group", NCIT)
    assert groups == {mondo: OBO + "MONDO_", ncit: NCIT}
    assert chosen.representative[OBO + "MONDO_2"] == mondo
    assert chosen.representative[OBO + "CHEBI_1"] == OBO + "CHEBI_0", "Lifted terms keep their ancestor"
    assert chosen.representative[data] == data, "A namespace with fewer terms is not grouped"
    assert chosen.classes_after == 4
    assert chosen.members()[ncit] == [NCIT + "C1", NCIT + "C2"]


def test_a_root_with_members_is_not_grouped():
    terms = [OBO + "CHEBI_1", OBO + "CHEBI_0"]
    chosen = choose_representatives(terms, {OBO + "CHEBI_1": {OBO + "CHEBI_0"}}, budget=1)
    assert group_by_namespace(chosen, {OBO + "CHEBI_1": {OBO + "CHEBI_0"}}) == {}
