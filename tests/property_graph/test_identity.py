"""rdfsolve.property_graph identity: nodes follow a claims decision (one node per entity, named by
its issuer), and an identifier of another kind is kept apart or unfolded into its own node."""

import csv
import json
from datetime import date
from types import SimpleNamespace

import networkx as nx
import pyoxigraph as ox
import pytest

from rdfsolve.property_graph import Conversion, Fold, Identity, PropertyGraph, suggest_folds
from rdfsolve.schema_models import MinedSchema, SchemaPattern
from tests.mappings.test_claims import BRIDGEDB, IDO, WP, O, claims, rdf
from tests.property_graph.data import (
    DATA,
    IDS,
    MAPPINGS,
    PREFIXES,
    RDF_TYPE,
    WPV,
    XSD,
    E,
    _pathway,
    graph,
)


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


def test_identity_merges_one_identifier_and_decides_each_mapping():
    """The WikiPathways, UniProt and ChEBI case: IRIs of one identifier become one node with its
    IRIs; a link between two kinds stays an edge; a same-kind one-to-one link to an identifier
    without data becomes an attribute; a link whose kind is unknown stays an edge and is
    reported as undecided. The RDF is given back unchanged."""
    from rdfsolve.property_graph import Identity

    data = ox.Dataset(ox.parse(IDS.encode(), ox.RdfFormat.TURTLE))
    protein = "http://purl.uniprot.org/core/Protein"
    kinds = {
        "uniprot": [protein],
        "pr": [protein],
        "chebi": ["http://www.w3.org/2002/07/owl#Class"],
    }
    pg = PropertyGraph.from_rdf(data, identity=Identity(kinds=kinds, mappings=MAPPINGS))
    report = pg.report()
    assert report["lossless"]["passed"], report["lossless"]
    p53 = pg.nodes["http://purl.uniprot.org/uniprot/P04637"]
    assert p53.members == [
        "http://purl.uniprot.org/uniprot/P04637",
        "https://identifiers.org/uniprot/P04637",
    ]
    assert "https://identifiers.org/uniprot/P04637" not in pg.nodes
    water = pg.nodes["http://purl.obolibrary.org/obo/CHEBI_15377"]
    assert len(water.members) == 2 and report["identity"]["statements_within_merged_nodes"] == 1
    assert "http://purl.obolibrary.org/obo/PR_P04637" not in pg.nodes, "Same kind, one to one"
    attribute = pg.to_networkx().nodes["http://purl.uniprot.org/uniprot/P04637"]
    assert attribute["exactMatch"] == "http://purl.obolibrary.org/obo/PR_P04637"
    assert attribute["ids"] == p53.members
    gene_links = [e for e in pg.edges if e.source.endswith("ENSG00000141510")]
    assert {e.target for e in gene_links} == {"http://purl.uniprot.org/uniprot/P04637"}, (
        "A gene and its protein stay linked; nothing is merged across kinds"
    )
    gene = next(n for n in pg.nodes.values() if n.id.endswith("ENSG00000141510"))
    assert "https://identifiers.org/uniprot/A0A087X1C5" in [
        v.lexical for v in gene.properties[WPV + "bdbUniprot"]
    ], "A protein the RDF only cites is a value of the gene, not a node"
    assert report["cited"]["resources"] >= 1
    rows = {r["decision"]: r for r in report["identity"]["mappings"]}
    undecided = rows["edge: undecided, kind unknown for ensembl"]
    assert undecided["links"] == 2 and "ensembl" in undecided["decide_with"]
    decided = PropertyGraph.from_rdf(
        data,
        identity=Identity(
            kinds={**kinds, "ensembl": ["http://example.org/Gene"]}, mappings=MAPPINGS
        ),
    ).report()
    assert {r["decision"] for r in decided["identity"]["mappings"]} >= {
        "edge: a relation between two kinds"
    }
    assert not [r for r in decided["identity"]["mappings"] if "ensembl" in r["decision"]]
    assert decided["lossless"]["passed"]


def test_without_identity_nothing_is_merged():
    data = ox.Dataset(ox.parse(IDS.encode(), ox.RdfFormat.TURTLE))
    pg = PropertyGraph.from_rdf(data)
    assert not any(n.members for n in pg.nodes.values()) and pg.report()["lossless"]["passed"]
    assert "https://identifiers.org/uniprot/P04637" not in pg.nodes, "only cited: a value"


def test_exact_identity_merges_two_identifiers_of_one_kind():
    """A WikiPathways metabolite (a CAS number) and its ChEBI class, linked one to one, are one
    node once CAS numbers are known to name the same kind as ChEBI ids; without exact they stay
    linked by an edge. Nothing is merged across predicates that cannot state identity."""
    from rdfsolve.property_graph import Identity

    data = ox.Dataset(ox.parse(IDS.encode(), ox.RdfFormat.TURTLE))
    chemical = ["http://www.w3.org/2002/07/owl#Class"]
    kinds = {"chebi": chemical, "cas": chemical}
    linked = PropertyGraph.from_rdf(data, identity=Identity(kinds=kinds, mappings=MAPPINGS))
    rows = {r["decision"] for r in linked.report()["identity"]["mappings"]}
    assert "edge: same kind, one to one; merged with Identity(exact=True)" in rows
    one = PropertyGraph.from_rdf(
        data, identity=Identity(kinds=kinds, mappings=MAPPINGS, exact=True)
    )
    report = one.report()
    assert report["lossless"]["passed"] and report["identity"]["merged_exact"] == 1
    water = next(
        n for n in one.nodes.values() if "https://identifiers.org/cas/7732-18-5" in n.members
    )
    assert set(water.members) == {
        "http://purl.obolibrary.org/obo/CHEBI_15377",
        "https://identifiers.org/chebi/CHEBI:15377",
        "https://identifiers.org/cas/7732-18-5",
    }
    # One kind of entity, one node type: the data's own class; the issuer's class is its type.
    metabolite, owl_class = (
        "http://vocabularies.wikipathways.org/wp#Metabolite",
        "http://www.w3.org/2002/07/owl#Class",
    )
    assert set(water.labels) == {metabolite}
    assert [
        v.lexical for v in water.properties["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
    ] == [owl_class]
    assert report["identity"]["labels"]["classes_moved_to_type"] == 1
    every = PropertyGraph.from_rdf(
        data, identity=Identity(kinds=kinds, mappings=MAPPINGS, exact=True, labels="all")
    )
    assert {metabolite, owl_class} <= set(
        next(
            n for n in every.nodes.values() if "https://identifiers.org/cas/7732-18-5" in n.members
        ).labels
    )
    stated = PropertyGraph.from_rdf(
        data, identity=Identity(kinds=kinds, mappings=MAPPINGS, exact=True, labels=[owl_class])
    )
    chosen = next(
        n for n in stated.nodes.values() if "https://identifiers.org/cas/7732-18-5" in n.members
    )
    assert set(chosen.labels) == {owl_class} and stated.report()["lossless"]["passed"]
    rows = report["identity"]["mappings"]
    assert not [r for r in rows if r["example"][0] == r["example"][1]], (
        "A self statement is no mapping"
    )
    assert "https://identifiers.org/chebi/CHEBI:15422" not in water.members, (
        "Two ChEBI ids are two entities"
    )
    default = PropertyGraph.from_rdf(data, identity=Identity(kinds=kinds)).report()["identity"]
    assert {r["predicate"] for r in default["mappings"]} == {
        "http://www.w3.org/2004/02/skos/core#exactMatch"
    }
