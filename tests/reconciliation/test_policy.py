"""rdfsolve.reconciliation.policy: a policy states which source decides each namespace, the
match levels accepted, whether its cross-references are taken as exact and its links as unique,
and which entries it prefers; it is written as SHACL shapes that check mapping claims and
records, kept in a nanopublication, and read by the decision of mapping claims."""

import pyshacl
from rdflib import Graph, Literal

from rdfsolve.reconciliation.nanopubs import load, save
from rdfsolve.reconciliation.policy import Authority, Policy
from tests.mappings.test_claims import IDO, XREF, claims

OBO = "http://purl.obolibrary.org/obo/"
UP = "http://purl.uniprot.org/core/"
CHEMICALS = Policy(
    "urn:policy/chemicals",
    (
        Authority("chebi", "chebi", exact=True, unique=True, link=XREF),
        Authority("uniprot", "uniprot", preferred=((UP + "reviewed", Literal(True)),)),
    ),
)


def conforms(policy, data):
    return pyshacl.validate(
        Graph().parse(data=data, format="turtle"), shacl_graph=policy.to_graph(), advanced=True
    )[0]


def test_a_policy_is_kept_as_shapes_and_in_a_nanopublication(tmp_path):
    assert Policy.from_graph(CHEMICALS.to_graph(), CHEMICALS.iri) == CHEMICALS
    saved = load(
        save(CHEMICALS.nanopublication(attributed_to="urn:tool", created="2026-10-05"), tmp_path)
    )
    assert Policy.from_graph(saved.assertion, CHEMICALS.iri) == CHEMICALS


def test_the_shapes_check_match_levels_and_unique_links():
    xref = f"""
    [] a <http://www.w3.org/2002/07/owl#Axiom> ;
       <http://www.w3.org/2002/07/owl#annotatedSource> <{IDO}lipidmaps/LMSP03010023> ;
       <http://www.w3.org/2002/07/owl#annotatedProperty> <{XREF}> ;
       <http://www.w3.org/2002/07/owl#annotatedTarget> <{OBO}CHEBI_91146> .
    """
    assert conforms(CHEMICALS, xref), "Cross-references to ChEBI are taken as exact"
    stated = Policy(CHEMICALS.iri, (Authority("chebi", "chebi"),))
    assert not conforms(stated, xref), "Otherwise only exact matches are accepted"
    two = f'<urn:entry> <{XREF}> "CHEBI:1", "CHEBI:2", "CAS:50-00-0" .'
    assert not conforms(CHEMICALS, two), "Two ChEBI links of one record break uniqueness"
    assert conforms(CHEMICALS, f'<urn:entry> <{XREF}> "CHEBI:1", "CAS:50-00-0" .')


def test_the_decision_reads_the_policy():
    by_policy = claims().decide(namespaces=["chebi"], policy=CHEMICALS)
    by_arguments = claims().decide(
        namespaces=["chebi"], authority=["chebi", "uniprot"], exact=["chebi"]
    )
    assert by_policy.table().equals(by_arguments.table())
    assert by_policy.policy == CHEMICALS.iri, "The decision names the policy it applied"
