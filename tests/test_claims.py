"""Claims of sameness from several sources, compared, checked through a third identifier, and
decided (rdfsolve.mappings.claims), then used by a property graph (Identity.of)."""

from types import SimpleNamespace

import pyoxigraph as ox

from rdfsolve.mappings.claims import Claims, claim, property_of, source_of
from rdfsolve.property_graph import Identity, PropertyGraph

WP = "http://vocabularies.wikipathways.org/wp#"
O = "http://purl.obolibrary.org/obo/"
IDO = "https://identifiers.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
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

    def issued_kinds(self):
        return {"chebi": ["http://www.w3.org/2002/07/owl#Class"]}

    def identify(self, identifiers):
        assert not [i for i in identifiers if i.startswith("chebi:")], "Not asked about its own ids"
        found = [
            SimpleNamespace(
                identifier=i, resource=O + "CHEBI_91146", predicate=XREF, value=i, kind="literal"
            )
            for i in identifiers
            if i == "lipidmaps:LMSP03010023"
        ]
        # A bare number is no cross-reference.
        found += [
            SimpleNamespace(
                identifier=i,
                resource=O + "CHEBI_1",
                predicate=XREF,
                value="03010025",
                kind="literal",
            )
            for i in identifiers
            if i == "lipidmaps:LMSP03010025"
        ]
        return found


def claims():
    found = Claims.stated(rdf(), BRIDGEDB, "wikipathways")
    found += Claims.stated(rdf(), [SEE_ALSO], "uniprot", objects="ncbigene")
    return found.ask(
        ChEBI(),
        [IDO + "lipidmaps/LMSP03010023", IDO + "lipidmaps/LMSP03010025", IDO + "chebi/CHEBI:15377"],
    )


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
    decision = claims().decide(
        namespaces=["chebi"],
        variants=lambda iris: (
            [(IDO + "chebi/CHEBI:17115", IDO + "chebi/CHEBI:33384")]
            if IDO + "chebi/CHEBI:17115" in iris
            else []
        ),
    )
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
    decision = claims().decide(
        namespaces=["chebi"],
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


def test_the_issuer_preferred_entry_is_taken_and_ask_finds_its_own_questions():
    """BridgeDb links ASAH1 to its Swiss-Prot entry and to a TrEMBL one; the entry UniProt
    reviewed is taken. Asked without identifiers, ChEBI is asked about the subjects of the
    claims into ChEBI."""
    stated = Claims.stated(rdf(), BRIDGEDB, "wikipathways")
    asked = stated.ask(ChEBI())
    assert ("lipidmaps:LMSP03010023", "chebi") in {
        (r.subject, r.object.split(":")[0])
        for r in asked.table().itertuples()
        if r.source == "chebi"
    }
    decision = asked.decide(
        namespaces=["uniprot", "chebi"], prefer=["http://purl.uniprot.org/uniprot/Q13510"]
    )
    rows = decision.table().set_index("subject")
    assert (
        rows.loc["ensembl:ENSG00000104763", "outcome"] == "accepted: preferred by the issuer, 1 not"
    )
    assert rows.loc["ensembl:ENSG00000104763", "not preferred"] == "uniprot:A0A1B0GTA6"
    assert decision.targets("uniprot") == [IDO + "uniprot/Q13510"]
    assert O + "CHEBI_91146" in decision.targets("chebi")
    # through: a node with no ChEBI id gets ChEBI's class for another of its identifiers
    other = Claims(
        [claim(IDO + "cas/50-00-0", IDO + "lipidmaps/LMSP03010023", "wikipathways", XREF)]
    )
    rows = other.ask(ChEBI(), through=True).table()
    assert ("cas:50-00-0", "chebi:91146", "chebi") in set(
        zip(rows.subject, rows.object, rows.source, strict=True)
    )


def test_one_node_per_entity_named_by_its_issuer():
    """The protein WikiPathways draws with an Ensembl gene id and its UniProt entry are one node
    keyed by UniProt's IRI; the metabolite takes ChEBI's name and keeps WikiPathways' beside it;
    the TrEMBL entry the RDF only cites is a value, not a node."""
    decision = claims().decide(
        namespaces=["uniprot", "chebi"], prefer=["http://purl.uniprot.org/uniprot/Q13510"]
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


def test_prefer_takes_client_results():
    """A Results of the issuer's preferred entries (UniProt's reviewed ones) names them."""
    reviewed = SimpleNamespace(
        records=[SimpleNamespace(uri="http://purl.uniprot.org/uniprot/Q13510")]
    )
    decision = claims().decide(namespaces=["uniprot"], prefer=reviewed)
    assert decision.targets("uniprot") == [IDO + "uniprot/Q13510"]
    assert claims().decide(namespaces=["uniprot"], prefer=[reviewed]).targets("uniprot") == [
        IDO + "uniprot/Q13510"
    ]


GENE = "http://purl.obolibrary.org/obo/SO_0000704"


def test_a_gene_id_is_kept_apart_from_its_protein_and_can_be_unfolded():
    """WikiPathways draws an enzyme as a Protein with an Ensembl gene id. Once Ensembl ids are
    known to name genes, the node is the UniProt protein and the gene id is kept apart (an
    attribute, not an id); unfold makes the gene a node again, linked to the protein. Without
    the kind, the merge is reported with what would decide it. The RDF is given back."""
    decision = claims().decide(
        namespaces=["uniprot"], prefer=["http://purl.uniprot.org/uniprot/Q13510"]
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


def test_claims_of_a_client_read_the_cross_references_from_the_records():
    """BridgeDb links are claims (one namespace each, cited); a link to the source's own
    identifiers (part of a pathway) and a link to a described resource are not."""
    extra = f"""@prefix wp: <{WP}> .
    <{IDO}lipidmaps/LMSP03010023> <http://purl.org/dc/terms/isPartOf> <{IDO}wikipathways/WP4726> .
    <urn:catalysis> wp:source <{IDO}ensembl/ENSG00000104763> ."""
    pathway = [q for q in rdf() if "purl.uniprot.org" not in q.subject.value]  # WikiPathways only
    data = ox.Dataset([*pathway, *ox.parse(extra.encode(), ox.RdfFormat.TURTLE)])
    wp = SimpleNamespace(
        _schema=SimpleNamespace(about=SimpleNamespace(dataset_name="wikipathways")),
        issued_kinds=lambda: {"wikipathways": [WP + "DataNode"]},
        to_oxigraph=lambda *results: data,
    )
    found = Claims.of(wp)
    assert {property_of(c) for c in found.claims} == set(BRIDGEDB)
    assert all(source_of(c) == "wikipathways" for c in found.claims)
    cites = Claims.of(wp, citing=["ncbigene"])
    assert len(cites) == len(found)
    given = []
    wp.to_oxigraph = lambda *results: (given.append(results), data)[1]
    assert len(Claims.of(client=wp, records=["genes"])) == len(found) and given[-1] == ("genes",)
    drawn = ox.Dataset(
        [
            *data,
            ox.Quad(
                ox.NamedNode(IDO + "chebi/CHEBI:15377"),
                ox.NamedNode(WP + "bdbChEBI"),
                ox.NamedNode(IDO + "chebi/CHEBI:15377"),
            ),
        ]
    )
    wp.to_oxigraph = lambda *results: drawn
    named = Claims.of(wp).decide(namespaces=["chebi"])
    assert IDO + "chebi/CHEBI:15377" in named.targets("chebi"), (
        "a node drawn with its ChEBI id gets its ChEBI record"
    )


def test_cross_references_of_a_schema():
    """A LIPID MAPS id that fails the pattern still counts as LIPID MAPS; a property whose
    values the source describes itself (publications) is no cross-reference."""
    from rdfsolve.client.api import Client

    def example(prop, value):
        return SimpleNamespace(property_uri=prop, value=SimpleNamespace(value=value))

    refs = "http://purl.org/dc/terms/references"
    schema = SimpleNamespace(
        enrichment=SimpleNamespace(
            examples=[
                example(WP + "bdbLipidMaps", IDO + "lipidmaps/LMPR0106010002"),
                example(WP + "bdbLipidMaps", IDO + "lipidmaps/LMSP02"),  # fails the pattern
                example(refs, IDO + "pubmed/22628558"),
                example(WP + "bdbChEBI", IDO + "chebi/CHEBI:15377"),
                example("http://purl.org/dc/terms/isPartOf", IDO + "wikipathways/WP4726"),
            ]
        ),
        patterns=[
            SimpleNamespace(property_uri=WP + "bdbLipidMaps", object_class="Resource"),
            SimpleNamespace(property_uri=WP + "bdbChEBI", object_class="Resource"),
            SimpleNamespace(property_uri=refs, object_class=WP + "PublicationReference"),
            SimpleNamespace(
                property_uri="http://purl.org/dc/terms/isPartOf", object_class="Resource"
            ),
        ],
    )
    wp = SimpleNamespace(_schema=schema, issued_kinds=lambda: {"wikipathways": [WP + "DataNode"]})
    assert Client.cross_references(wp) == [WP + "bdbChEBI", WP + "bdbLipidMaps"]


def test_claims_and_the_resolution_are_sssom(tmp_path):
    """The claims and the resolution are SSSOM mapping sets that sssom-py reads back: stated
    BridgeDb links as cross-references with their source, accepted mappings as exact matches
    with the rule that decided them, overruled ones as negative mappings."""
    from sssom.parsers import parse_sssom_table

    from rdfsolve.mappings.sssom import write_sssom_tsv

    found = claims()
    decision = found.decide(
        namespaces=["chebi"],
        variants=lambda iris: [
            (a, b) for a in iris for b in iris if {a[-5:], b[-5:]} == {"17115", "33384"}
        ],
    )
    stated = found.to_sssom().df
    row = stated[
        (stated.subject_id == "lipidmaps:LMSP03010023") & (stated.object_id == "chebi:89488")
    ].iloc[0]
    assert (
        row.predicate_id == "oboinowl:hasDbXref"
        and row.mapping_justification == "semapv:UnspecifiedMatching"
    )
    assert "wikipathways" in row.mapping_provider and WP + "bdbChEBI" in row.other

    path = tmp_path / "resolution.sssom.tsv"
    write_sssom_tsv(decision.to_sssom(), path)
    table = parse_sssom_table(path).df.fillna("")
    exact = table[(table.predicate_modifier != "Not") & (table.confidence != 0.5)]
    negative = table[table.predicate_modifier == "Not"]
    assert ("lipidmaps:LMSP03010023", "chebi:91146") in set(zip(exact.subject_id, exact.object_id))
    assert list(zip(negative.subject_id, negative.object_id)) == [
        ("lipidmaps:LMSP03010023", "chebi:89488")
    ]
    assert "chebi" in negative.iloc[0].curation_rule_text
    assert set(table.mapping_justification) == {"semapv:MappingReview"}
    ambiguous = table[table.subject_id == "lipidmaps:LMSP03010025"]
    assert set(ambiguous.object_id) == {"chebi:1", "chebi:2"} and set(ambiguous.confidence) == {0.5}
    assert (
        set(ambiguous.mapping_cardinality) == {"1:n"}
        and "ambiguous" in ambiguous.iloc[0].curation_rule_text
    )


def test_an_identifier_that_fails_its_pattern_is_listed_not_written():
    """lipidmaps/LMSP02 fails the LIPID MAPS pattern: no SSSOM record, but listed as invalid."""
    turtle = f"""@prefix wp: <{WP}> .
    <{IDO}cas/7732-18-5> wp:bdbLipidMaps <{IDO}lipidmaps/LMSP02>, <{IDO}lipidmaps/LMPR0106010002> ."""
    found = Claims.stated(
        ox.Dataset(ox.parse(turtle.encode(), ox.RdfFormat.TURTLE)),
        [WP + "bdbLipidMaps"],
        "wikipathways",
    )
    assert [str(c.object_id) for c in found.claims] == ["lipidmaps:LMPR0106010002"]
    assert found.invalid == [(IDO + "cas/7732-18-5", IDO + "lipidmaps/LMSP02", "wikipathways")]
