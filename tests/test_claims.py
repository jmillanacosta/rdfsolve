"""Claims of sameness from several sources, compared, checked through a third identifier, and
decided (rdfsolve.mappings.claims), then used by a property graph (Identity.of)."""

from types import SimpleNamespace

import pyoxigraph as ox

from rdfsolve.mappings.claims import Claim, Claims
from rdfsolve.property_graph import Identity, PropertyGraph

WP = "http://vocabularies.wikipathways.org/wp#"
O = "http://purl.obolibrary.org/obo/"
IDO = "https://identifiers.org/"
DATA = f"""
@prefix wp: <{WP}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<{IDO}lipidmaps/LMSP03010023> a wp:Metabolite ; rdfs:label "C24:1DH SM" ;
    wp:bdbChEBI <{IDO}chebi/CHEBI:89488>, <{IDO}chebi/CHEBI:91146> .
<{IDO}pubchem.compound/5951> a wp:Metabolite ; rdfs:label "Serine" ;
    wp:bdbChEBI <{IDO}chebi/CHEBI:17115>, <{IDO}chebi/CHEBI:33384> .
<{IDO}lipidmaps/LMSP03010025> a wp:Metabolite ; rdfs:label "C26:1DH SM" ;
    wp:bdbChEBI <{IDO}chebi/CHEBI:1>, <{IDO}chebi/CHEBI:2> .
<{IDO}ensembl/ENSG00000104763> a wp:Protein ; rdfs:label "ASAH1" ;
    wp:bdbUniprot <{IDO}uniprot/Q13510>, <{IDO}uniprot/A0A1B0GTA6> ; wp:bdbEntrezGene <{IDO}ncbigene/427> .
<{O}CHEBI_91146> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "C24:1 sphingomyelin" .
<{O}CHEBI_17115> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "L-serine" .
<{O}CHEBI_33384> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "L-serine zwitterion" .
<http://purl.uniprot.org/uniprot/Q13510> a <http://purl.uniprot.org/core/Protein> ;
    rdfs:seeAlso <http://purl.uniprot.org/geneid/427> .
<http://purl.uniprot.org/uniprot/A0A1B0GTA6> a <http://purl.uniprot.org/core/Protein> ;
    rdfs:seeAlso <http://purl.uniprot.org/geneid/9999> .
"""
BRIDGEDB = [WP + "bdbChEBI", WP + "bdbUniprot", WP + "bdbEntrezGene"]
SEE_ALSO = "http://www.w3.org/2000/01/rdf-schema#seeAlso"
XREF = "http://www.geneontology.org/formats/oboInOwl#hasDbXref"


def rdf():
    return ox.Dataset(ox.parse(DATA.encode(), ox.RdfFormat.TURTLE))


class ChEBI:
    """A client of ChEBI that knows one cross-reference."""

    _schema = SimpleNamespace(about=SimpleNamespace(dataset_name="chebi"))

    def identify(self, identifiers):
        known = {"lipidmaps:LMSP03010023": O + "CHEBI_91146"}
        return [SimpleNamespace(identifier=i, resource=known[i], predicate=XREF) for i in identifiers if i in known]


def claims():
    found = Claims.stated(rdf(), BRIDGEDB, "wikipathways")
    found += Claims.stated(rdf(), [SEE_ALSO], "uniprot", objects="ncbigene")
    return found.ask(ChEBI(), [IDO + "lipidmaps/LMSP03010023", IDO + "lipidmaps/LMSP03010025"])


def test_sources_are_compared():
    table = claims().compare().set_index("subject")
    row = table.loc["lipidmaps:LMSP03010023"]
    assert row["status"] == "disagree"
    assert row["wikipathways"] == ["chebi:89488", "chebi:91146"] and row["chebi"] == ["chebi:91146"]
    assert table.loc["pubchem.compound:5951", "status"] == "one source"


def test_a_gene_to_protein_link_is_checked_through_ncbi_gene():
    """BridgeDb links ASAH1 (NCBI Gene 427) to two accessions; UniProt gives one of them
    another gene, so that link disagrees."""
    checked = claims().check("ncbigene", among=["wikipathways"])
    checked = checked[checked.object.str.startswith("uniprot:")].set_index("object")
    assert checked.loc["uniprot:Q13510", "status"] == "agree"
    assert checked.loc["uniprot:A0A1B0GTA6", "status"] == "disagree"


def test_the_issuer_decides_and_variants_join():
    decision = claims().decide(namespaces=["chebi"], variants=lambda iris: [
        (IDO + "chebi/CHEBI:17115", IDO + "chebi/CHEBI:33384")
    ] if IDO + "chebi/CHEBI:17115" in iris else [])
    rows = decision.table().set_index("subject")
    assert rows.loc["lipidmaps:LMSP03010023", "decided by"] == "chebi"
    assert rows.loc["lipidmaps:LMSP03010023", "overruled"] == "chebi:89488"
    assert rows.loc["pubchem.compound:5951", "outcome"] == "accepted: variants of one entity"
    assert rows.loc["lipidmaps:LMSP03010025", "outcome"].startswith("ambiguous")
    assert (IDO + "lipidmaps/LMSP03010023", O + "CHEBI_91146") in decision.pairs()
    assert not [p for p in decision.pairs() if "LMSP03010025" in p[0]]


def test_a_property_graph_follows_the_decision():
    """Every metabolite with an accepted ChEBI class is one node with it; L-serine and its
    zwitterion share the serine node (declared variants); the RDF is given back."""
    decision = claims().decide(namespaces=["chebi"], variants=lambda iris: [
        (a, b) for a in iris for b in iris if {a[-5:], b[-5:]} == {"17115", "33384"}
    ])
    chebi = SimpleNamespace(issued_kinds=lambda: {"chebi": ["http://www.w3.org/2002/07/owl#Class"]})
    pg = PropertyGraph.from_rdf(rdf(), identity=Identity.of(chebi, decision=decision), as_attributes=BRIDGEDB)
    report = pg.report()
    assert report["lossless"]["passed"]
    serine = next(n for n in pg.nodes.values() if IDO + "pubchem.compound/5951" in n.members)
    assert {O + "CHEBI_17115", O + "CHEBI_33384"} <= set(serine.members)
    assert serine.labels == [WP + "Metabolite"]
    sphingomyelin = next(n for n in pg.nodes.values() if IDO + "lipidmaps/LMSP03010023" in n.members)
    assert O + "CHEBI_91146" in sphingomyelin.members and O + "CHEBI_89488" not in sphingomyelin.members
    metabolites = [n for n in pg.nodes.values() if WP + "Metabolite" in n.labels]
    assert len(metabolites) == 3 and all(n.labels == [WP + "Metabolite"] for n in metabolites)
