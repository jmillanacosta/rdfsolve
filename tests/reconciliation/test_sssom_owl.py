"""rdfsolve.mappings.sssom to_owl: identity claims are SSSOM in OWL form (owl:Axiom), each with
a record IRI that follows from its content; a claim found through another identifier is
justified by semapv:MappingChaining and names the mappings it came from (prov:wasDerivedFrom)."""

from rdflib import OWL, PROV, Namespace, URIRef

from rdfsolve.mappings.claims import Claims, claim
from rdfsolve.mappings.sssom import to_owl
from tests.mappings.test_claims import IDO, XREF, ChEBI

SSSOM = Namespace("https://w3id.org/sssom/")
SEMAPV = Namespace("https://w3id.org/semapv/vocab/")


def test_a_chained_claim_names_the_mappings_it_came_from():
    stated = Claims(
        [claim(IDO + "cas/50-00-0", IDO + "lipidmaps/LMSP03010023", "wikipathways", XREF)]
    )
    graph = to_owl(stated.ask(ChEBI(), through=True).to_sssom())
    axioms = list(graph.subjects(None, OWL.Axiom))
    records = {graph.value(a, SSSOM.record_id): a for a in axioms}
    assert len(records) == len(axioms) and None not in records, "One record IRI for each mapping"
    (chained,) = [
        a for a in axioms if graph.value(a, SSSOM.mapping_justification) == SEMAPV.MappingChaining
    ]
    sources = {
        str(graph.value(records[d], OWL.annotatedSource))
        for d in graph.objects(chained, PROV.wasDerivedFrom)
    }
    assert sources == {
        str(graph.value(chained, OWL.annotatedSource)),
        "http://www.lipidmaps.org/data/LMSDRecord.php?LMID=LMSP03010023",
    }, "The claim through the LIPID MAPS id, and ChEBI's statement about that id"
    again = to_owl(stated.ask(ChEBI(), through=True).to_sssom())
    assert {graph.value(a, SSSOM.record_id) for a in axioms} == {
        again.value(a, SSSOM.record_id) for a in again.subjects(None, OWL.Axiom)
    }, "A record IRI follows from the content"
