"""rdfsolve.property_graph identity: nodes follow a claims decision (one node per entity, named by
its issuer), and an identifier of another kind is kept apart or unfolded into its own node."""

from types import SimpleNamespace

import pyoxigraph as ox

from rdfsolve.property_graph import Identity, PropertyGraph
from tests.mappings.test_claims import BRIDGEDB, IDO, O, RDF_TYPE, WP, claims, rdf


def test_a_property_graph_follows_the_decision():
    """Every metabolite with an accepted ChEBI class is one node with it; L-serine and its
    zwitterion share the serine node (declared variants); the RDF is given back."""
    decision = claims().decide(
        namespaces=["chebi"],
        exact=["chebi"],
        variants=lambda iris: [
            (a, b) for a in iris for b in iris if {a[-5:], b[-5:]} == {"17115", "33384"}
        ],
    )
    chebi = SimpleNamespace(issued_kinds=lambda: {"chebi": ["http://www.w3.org/2002/07/owl#Class"]})
    pg = PropertyGraph.from_rdf(
        rdf(), identity=Identity.of(chebi, decision=decision), as_attributes=BRIDGEDB
    )
    report = pg.report()
    assert report["lossless"]["passed"]
    serine = next(n for n in pg.nodes.values() if IDO + "pubchem.compound/5951" in n.members)
    assert {O + "CHEBI_17115", O + "CHEBI_33384"} <= set(serine.members)
    assert serine.labels == [WP + "Metabolite"]
    sphingomyelin = next(
        n for n in pg.nodes.values() if IDO + "lipidmaps/LMSP03010023" in n.members
    )
    assert (
        O + "CHEBI_91146" in sphingomyelin.members
        and O + "CHEBI_89488" not in sphingomyelin.members
    )
    metabolites = [n for n in pg.nodes.values() if WP + "Metabolite" in n.labels]
    assert len(metabolites) == 3 and all(n.labels == [WP + "Metabolite"] for n in metabolites)


def test_one_node_per_entity_named_by_its_issuer():
    """The protein WikiPathways draws with an Ensembl gene id and its UniProt entry are one node
    keyed by UniProt's IRI; the metabolite takes ChEBI's name and keeps WikiPathways' beside it;
    the TrEMBL entry the RDF only cites is a value, not a node."""
    decision = claims().decide(
        namespaces=["uniprot", "chebi"],
        prefer=["http://purl.uniprot.org/uniprot/Q13510"],
        exact=["uniprot", "chebi"],
    )
    issuers = [
        SimpleNamespace(issued_kinds=lambda: {"chebi": ["http://www.w3.org/2002/07/owl#Class"]}),
        SimpleNamespace(issued_kinds=lambda: {"uniprot": ["http://purl.uniprot.org/core/Protein"]}),
        SimpleNamespace(issued_kinds=lambda: {"wikipathways": [WP + "Metabolite", WP + "Protein"]}),
    ]
    data = [q for q in rdf() if "A0A1B0GTA6" not in q.subject.value]  # its record was not fetched
    pg = PropertyGraph.from_rdf(data, identity=Identity.of(*issuers, decision=decision))
    report = pg.report()
    assert report["lossless"]["passed"]
    protein = pg.nodes["http://purl.uniprot.org/uniprot/Q13510"]
    # The issuer's kind is the node type; WikiPathways' class of its drawing is kept as type.
    assert IDO + "ensembl/ENSG00000104763" in protein.members
    assert protein.labels == ["http://purl.uniprot.org/core/Protein"]
    assert WP + "Protein" in {v.lexical for v in protein.properties[RDF_TYPE]}
    assert "http://purl.uniprot.org/uniprot/A0A1B0GTA6" not in pg.nodes
    assert not [n for n in pg.nodes.values() if not n.labels], "every node has a class"
    node = pg.to_networkx().nodes[O + "CHEBI_91146"]
    assert node["label"] == "C24:1 sphingomyelin" and node["label_wikipathways"] == "C24:1DH SM"


GENE = "http://purl.obolibrary.org/obo/SO_0000704"


def test_a_gene_id_is_kept_apart_from_its_protein_and_can_be_unfolded():
    """WikiPathways draws an enzyme as a Protein with an Ensembl gene id. Once Ensembl ids are
    known to name genes, the node is the UniProt protein and the gene id is kept apart (an
    attribute, not an id); unfold makes the gene a node again, linked to the protein. Without
    the kind, the merge is reported with what would decide it. The RDF is given back."""
    decision = claims().decide(
        namespaces=["uniprot"], prefer=["http://purl.uniprot.org/uniprot/Q13510"], exact=["uniprot"]
    )
    up = SimpleNamespace(issued_kinds=lambda: {"uniprot": ["http://purl.uniprot.org/core/Protein"]})
    data = [q for q in rdf() if "A0A1B0GTA6" not in q.subject.value]
    protein = "http://purl.uniprot.org/uniprot/Q13510"
    gene = IDO + "ensembl/ENSG00000104763"

    unknown = PropertyGraph.from_rdf(data, identity=Identity.of(up, decision=decision)).report()[
        "identity"
    ]
    assert unknown["merged_with_unknown_kind"]["identifiers"] == {"ensembl": 1}
    assert "'ensembl'" in unknown["merged_with_unknown_kind"]["decide_with"]

    kinds = {"ensembl": [GENE]}
    pg = PropertyGraph.from_rdf(data, identity=Identity.of(up, decision=decision, kinds=kinds))
    assert pg.report()["lossless"]["passed"]
    node = pg.to_networkx().nodes[protein]
    assert gene not in node["ids"] and node["ensembl"] == gene
    assert node["labels"] == ["Protein"] and gene not in pg.nodes

    apart = PropertyGraph.from_rdf(
        data, identity=Identity.of(up, decision=decision, kinds=kinds, unfold=["ensembl"])
    )
    report = apart.report()
    assert report["lossless"]["passed"], report["lossless"]
    assert apart.nodes[gene].labels == [GENE] and report["identity"]["unfolded"]["nodes"] == 1
    assert [(e.source, e.target) for e in apart.edges if e.type == WP + "bdbUniprot"] == [
        (gene, protein)
    ]
    assert WP + "bdbEntrezGene" in apart.nodes[gene].properties, "the gene keeps its NCBI Gene id"
    assert "ensembl" not in apart.to_networkx().nodes[protein]
    self_named = [q for q in data] + [
        ox.Quad(ox.NamedNode(gene), ox.NamedNode(WP + "bdbEnsembl"), ox.NamedNode(gene))
    ]
    named = PropertyGraph.from_rdf(
        self_named, identity=Identity.of(up, decision=decision, kinds=kinds, unfold=["ensembl"])
    )
    assert [e.type for e in named.edges if e.source == gene] == [WP + "bdbUniprot"], (
        "a self statement is no edge"
    )
    assert named.report()["lossless"]["passed"]
