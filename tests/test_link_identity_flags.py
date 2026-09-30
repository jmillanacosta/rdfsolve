"""A link over an identity property (owl:sameAs, skos:exactMatch) is checked as a declared
identity is: a gene stated to be the same as its protein is a real join on the data, but the
statement claims more than holds. The link keeps its evidence and records the flags; a route
and an edge of the connectivity graph that use the link carry them, and a route search takes
a route without flags first (the owner, 2026-09-30)."""

from rdflib import Dataset

from rdfsolve.analysis import best_route, build_connectivity
from rdfsolve.api import Client
from rdfsolve.mappings.routes import Route, RouteEvidence
from rdfsolve.mappings.signatures import Link, read_links, verify, write_links
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

UP, GENE = "http://purl.uniprot.org/uniprot/", "https://identifiers.org/ncbigene/"
SAME, XREF = "http://www.w3.org/2002/07/owl#sameAs", "urn:xref"
SOURCE = f"""
<{GENE}7157> a <urn:Gene> ; <{SAME}> <{UP}P04637> ; <{XREF}> <{UP}P04637> .
<{GENE}672> a <urn:Gene> ; <{SAME}> <{UP}P38398> ; <{XREF}> <{UP}P38398> .
"""
TARGET = f"<{UP}P04637> a <urn:Protein> . <{UP}P38398> a <urn:Protein> ; <urn:in> <urn:t/1> . <urn:t/1> a <urn:Taxon> ."
PATTERNS = [
    SchemaPattern(subject_class="urn:Gene", property_uri=SAME, object_class="Resource"),
    SchemaPattern(subject_class="urn:Gene", property_uri=XREF, object_class="Resource"),
]
IN = SchemaPattern(subject_class="urn:Protein", property_uri="urn:in", object_class="urn:Taxon")
GENES = MinedSchema(about=AboutMetadata.build(dataset_name="genes"), patterns=PATTERNS)
PROTEINS = MinedSchema(about=AboutMetadata.build(dataset_name="proteins"), patterns=[IN])
STATED = Link("join", "genes", "urn:Gene", SAME, "uniprot", "proteins", "urn:Protein")
REFERRED = Link("join", "genes", "urn:Gene", XREF, "uniprot", "proteins", "urn:Protein")


def _verify(link):
    source = Dataset().parse(format="turtle", data=SOURCE)
    target = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, source) as s, Client(PROTEINS, target) as t:
        return verify(link, s, t, sample=None)


def test_an_identity_link_between_two_kinds_of_entity_is_flagged_and_kept(tmp_path):
    stated, referred = _verify(STATED), _verify(REFERRED)
    assert stated.found == 2 and stated.flags == {"kind:gene-protein": 2}
    assert stated.flag_checked == 2 and stated.flagged
    assert referred.found == 2 and referred.flags == {} and not referred.flagged, (
        "A cross-reference states no identity"
    )
    write_links(tmp_path / "links.tsv", [stated, referred])
    read = read_links(tmp_path / "links.tsv")
    assert [e.flags for e in read] == [{"kind:gene-protein": 2}, {}]
    assert read[0].flag_checked == 2


def test_routes_and_edges_carry_the_flags_and_a_route_without_flags_is_taken_first():
    stated, referred = _verify(STATED), _verify(REFERRED)
    schemas = {"genes": GENES, "proteins": PROTEINS}
    flagged = RouteEvidence(Route(STATED, (), (IN,)), 2, 1, True, flags=stated.flags)
    clean = RouteEvidence(Route(REFERRED, (), (IN,)), 2, 1, True)
    ends = ("genes", "urn:Gene"), ("proteins", "urn:Taxon")
    only = build_connectivity(schemas, links=[stated], routes=[flagged])
    found = best_route(only, *ends)
    assert found["flagged"] and found["edges"][0]["flags"] == {"kind:gene-protein": 2}
    assert found["evidence"] == "confirmed", "The join is on the data; the flag is about the claim"
    link_edge = best_route(only, ("genes", "urn:Gene"), ("proteins", "urn:Protein"))
    assert link_edge["flagged"] and link_edge["edges"][0]["kind"] == "verified_link"
    both = build_connectivity(schemas, links=[stated, referred], routes=[flagged, clean])
    taken = best_route(both, *ends)
    assert not taken["flagged"] and taken["edges"][0]["predicate"] == XREF
