"""A verified link is a VoID linkset only where the source's triples name the target's own
resources; a link that needs its identifiers rewritten is not a linkset."""

from rdflib import Namespace, RDF, URIRef

from rdfsolve.config import mint
from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.mappings.void import links_to_void

VOID = Namespace("http://rdfs.org/ns/void#")
UP, IDORG = "http://purl.uniprot.org/uniprot/", "https://identifiers.org/uniprot:"
DIRECT = LinkEvidence(
    Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein"),
    3, 2, {UP + "{id}": 2}, [(UP + "P04637", UP + "P04637"), (UP + "P38398", UP + "P38398")],
    population=3, complete=True,
)
REWRITE = LinkEvidence(
    Link("join", "genes", "urn:Gene", "urn:other", "uniprot", "proteins", "urn:Protein"),
    50, 40, {UP + "{id}": 40}, [(IDORG + "P04637", UP + "P04637")], population=900,
)


def test_only_direct_links_are_linksets():
    graph = links_to_void([DIRECT, REWRITE], min_share=0.5)
    (linkset,) = graph.subjects(RDF.type, VOID.Linkset)
    assert graph.value(linkset, VOID.linkPredicate) == URIRef("urn:xref")
    assert graph.value(linkset, VOID.subjectsTarget) == URIRef(mint("dataset", "genes"))
    assert graph.value(linkset, VOID.objectsTarget) == URIRef(mint("dataset", "proteins"))
    assert graph.value(linkset, VOID.distinctObjects).toPython() == 2, "Every value was read"
