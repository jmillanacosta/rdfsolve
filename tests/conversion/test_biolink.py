"""rdfsolve.conversion Biolink: a Biolink graph is written as KGX, process and association nodes
imply direct edges, ancestors include mixins, kind conflicts are found and decided by recorded
queries, and views show qualifiers."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Query, Rule, rules_of
from rdfsolve.property_graph import Fold, Identity, PropertyGraph
from tests.conversion.data import BIOLINK_YAML, BL, DATA, WP, Local, biolink_client, pairs, store


def test_a_biolink_graph_is_written_as_kgx_and_its_terms_drawn(tmp_path):
    """KGX nodes and edges: Biolink CURIEs, categories, names, provenance; an association node is
    one edge with its qualifier. The model draws a class with its parents and a slot as an edge."""
    import csv

    from rdfsolve.conversion import Biolink, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<https://identifiers.org/ncbigene/3156> a <{BL}Gene> ; <{BL}name> "HMGCR" .
    <http://identifiers.org/ncbigene/3156> a <{BL}Gene> .
    <https://identifiers.org/ncbigene/3156>
        <{BL}catalyzes> <urn:r1> .
    <urn:r1> a <{BL}MolecularActivity> ; <{BL}has_input> <https://source-test.invalid/data/Complex/a2d92> .
    <https://source-test.invalid/data/Complex/a2d92> a <{BL}MacromolecularComplex> .
    <urn:i1> a <{BL}Association> ; <{BL}subject> <https://identifiers.org/ncbigene/3156> ;
        <{BL}predicate> <{BL}regulates> ; <{BL}object> <urn:r1> ; <{BL}object_direction_qualifier> "decreased" ."""
    graph = PropertyGraph.from_rdf(
        ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)), identity=Identity()
    )
    nodes, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test", local_prefix="src")
    node_rows = {r["id"]: r for r in csv.DictReader(nodes.open(), delimiter="\t")}
    assert node_rows["NCBIGene:3156"] == {
        "id": "NCBIGene:3156",
        "category": "biolink:Gene",
        "name": "HMGCR",
        "xref": "",
    }
    assert "urn:i1" not in node_rows, "an association is an edge"
    edge_rows = list(csv.DictReader(edges.open(), delimiter="\t"))
    assert {
        (r["subject"], r["predicate"], r["object"], r["object_direction_qualifier"])
        for r in edge_rows
    } == {
        ("NCBIGene:3156", "biolink:catalyzes", "urn:r1", ""),
        ("NCBIGene:3156", "biolink:regulates", "urn:r1", "decreased"),
        ("urn:r1", "biolink:has_input", "src_complex:a2d92", ""),
    }
    assert all(r["primary_knowledge_source"] == "infores:test" for r in edge_rows)
    import json

    local = next(r["object"] for r in edge_rows if r["predicate"] == "biolink:has_input")
    prefix, _, name = local.partition(":")
    assert "/" not in name and json.loads((tmp_path / "kgx" / "prefixes.json").read_text())[
        prefix
    ] + name == ("https://source-test.invalid/data/Complex/a2d92"), (
        "an IRI no registry knows: a CURIE of a named namespace, expanded by prefixes.json"
    )
    drawn = biolink.diagram("biolink:Gene", "biolink:has_input")
    assert "**gene**" in drawn and '-->|"is a"|' in drawn and "ids: NCBIGene, ENSEMBL" in drawn
    assert "has input (is a has participant)" in drawn


def test_reaction_nodes_imply_derivation_edges_with_their_catalysts(tmp_path):
    """Biolink's rule: input derives_into output, with the reaction's catalysts as
    catalyst_qualifier; derived edges keep the round trip and are written to KGX."""
    import csv

    from rdfsolve.conversion import Biolink, derive_associations, derive_conversions, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<urn:r1> a <{BL}MolecularActivity> ; <{BL}has_input> <https://identifiers.org/chebi/CHEBI:1> ;
        <{BL}has_output> <https://identifiers.org/chebi/CHEBI:2>, <https://identifiers.org/chebi/CHEBI:3> .
    <https://identifiers.org/ncbigene/3156> a <{BL}Gene> ; <{BL}catalyzes> <urn:r1> .
    <https://identifiers.org/uniprot/P04035> a <{BL}Protein> ; <{BL}catalyzes> <urn:r1> .
    <https://identifiers.org/chebi/CHEBI:1> a <{BL}ChemicalEntity> . <https://identifiers.org/chebi/CHEBI:2> a <{BL}ChemicalEntity> .
    <https://identifiers.org/chebi/CHEBI:3> a <{BL}ChemicalEntity> ."""
    mapped = ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))
    with biolink_client() as client:
        assert derive_conversions(client, mapped, biolink) == {
            "chemical entity to chemical derivation association": 2,
            "with catalyst": 2,
        }
        assert [s["name"] for s in client.session_metadata()["steps"]] == ["Conversions"]
    graph = PropertyGraph.from_rdf(mapped)
    assert derive_associations(graph, biolink) == 2
    assert (
        graph.report()["lossless"]["passed"] and graph.report()["edge_types"]["derives_into"] == 2
    )
    # every catalyst of a reaction stays on its derived edges, also in networkx
    network = graph.to_networkx(edge_types=["derives_into"])
    assert [sorted(d["catalyst_qualifier"]) for *_, d in network.edges(data=True)] == [
        ["https://identifiers.org/ncbigene/3156", "https://identifiers.org/uniprot/P04035"]
    ] * 2
    _, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test")
    rows = [
        r
        for r in csv.DictReader(edges.open(), delimiter="\t")
        if r["predicate"] == "biolink:derives_into"
    ]
    assert {(r["subject"], r["object"], r["catalyst_qualifier"]) for r in rows} == {
        ("chebi:1", "chebi:2", "NCBIGene:3156|UniProtKB:P04035"),
        ("chebi:1", "chebi:3", "NCBIGene:3156|UniProtKB:P04035"),
    }


def test_biolink_ancestors_with_mixins_join_genes_and_proteins(tmp_path):
    """Along is_a only, a gene and a protein meet at named thing; with their mixins, both are a
    gene or gene product (and a macromolecular machine mixin, catalyst_qualifier's range)."""
    from rdfsolve.conversion import Biolink

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    assert biolink.ancestors("protein") == ["named thing"]
    assert biolink.ancestors("protein", mixins=True) == [
        "named thing",
        "gene product mixin",
        "gene or gene product",
        "macromolecular machine mixin",
    ]
    shared = set(biolink.ancestors("gene", mixins=True)) & set(
        biolink.ancestors("protein", mixins=True)
    )
    assert shared == {"named thing", "gene or gene product", "macromolecular machine mixin"}


def test_a_process_of_unknown_kind_gives_only_related_to(tmp_path):
    """A process that is no molecular activity (the source does not say what it is) links its
    input to its output with related_to, Biolink's root predicate, also when both are the same
    chemical; its catalyst stays attached. No derives_into is claimed."""
    from rdfsolve.conversion import Biolink, derive_associations, derive_conversions

    (tmp_path / "b.yaml").write_text(
        BIOLINK_YAML.replace(
            "  molecular activity: {is_a: named thing, id_prefixes: [RHEA]}",
            "  biological process or activity: {is_a: named thing}\n"
            "  molecular activity: {is_a: biological process or activity, id_prefixes: [RHEA]}",
        )
    )
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<urn:p1> a <{BL}BiologicalProcessOrActivity> ;
        <{BL}has_input> <https://identifiers.org/chebi/CHEBI:16113> ;
        <{BL}has_output> <https://identifiers.org/chebi/CHEBI:16113> .
    <urn:complex> <{BL}catalyzes> <urn:p1> .
    <https://identifiers.org/chebi/CHEBI:16113> a <{BL}ChemicalEntity> ."""
    mapped = ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))
    with biolink_client() as client:
        assert derive_conversions(client, mapped, biolink) == {"association": 1, "with catalyst": 1}
    graph = PropertyGraph.from_rdf(mapped)
    derive_associations(graph, biolink)
    network = graph.to_networkx(edge_types=["related_to"])
    assert [(u, v, d["catalyst_qualifier"]) for u, v, d in network.edges(data=True)] == [
        (
            "https://identifiers.org/chebi/CHEBI:16113",
            "https://identifiers.org/chebi/CHEBI:16113",
            ["urn:complex"],
        )
    ]


def test_an_association_gives_one_direct_edge_and_one_kgx_row(tmp_path):
    """An association node (regulates, decreased) gives one edge from its subject to its object
    with the qualifier attached; KGX still writes it as one row."""
    import csv

    from rdfsolve.conversion import Biolink, derive_associations, to_kgx

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<https://identifiers.org/ncbigene/335> a <{BL}Gene> .
    <https://identifiers.org/ncbigene/19> a <{BL}Gene> .
    <urn:i1> a <{BL}Association> ; <{BL}subject> <https://identifiers.org/ncbigene/335> ;
        <{BL}predicate> <{BL}regulates> ; <{BL}object> <https://identifiers.org/ncbigene/19> ;
        <{BL}object_direction_qualifier> "increased" ."""
    graph = PropertyGraph.from_rdf(
        ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE)), identity=Identity()
    )
    assert derive_associations(graph, biolink) == 1
    assert graph.report()["lossless"]["passed"]
    network = graph.to_networkx(edge_types=["regulates"])
    assert [(u, v, d["object_direction_qualifier"]) for u, v, d in network.edges(data=True)] == [
        (
            "https://identifiers.org/ncbigene/335",
            "https://identifiers.org/ncbigene/19",
            "increased",
        )
    ]
    _, edges = to_kgx(graph, biolink, tmp_path / "kgx", "infores:test")
    rows = list(csv.DictReader(edges.open(), delimiter="\t"))
    assert [(r["subject"], r["predicate"], r["object"]) for r in rows] == [
        ("NCBIGene:335", "biolink:regulates", "NCBIGene:19")
    ]


def test_kind_conflicts_are_found_and_decided_by_queries_in_the_session(tmp_path, caplog):
    """A node drawn as a protein in one place and as a chemical in another (one node through
    same), and a node with gene and protein: both found, logged, and each decision taken out by a
    CONSTRUCT recorded in the client's session."""
    from rdfsolve.conversion import Biolink, keep_kinds, kind_conflicts

    (tmp_path / "b.yaml").write_text(
        BIOLINK_YAML.replace(
            "  association:", "  chemical entity: {is_a: named thing}\n  association:"
        )
    )
    biolink = Biolink.read(tmp_path / "b.yaml")
    mapped = ox.Dataset(
        ox.parse(
            f"""<urn:a> a <{BL}Protein> . <urn:b> a <{BL}ChemicalEntity> .
        <urn:d> a <{BL}Gene>, <{BL}Protein> . <urn:e> a <{BL}Gene> .""".encode(),
            ox.RdfFormat.TURTLE,
        )
    )
    with biolink_client() as client:
        conflicts = kind_conflicts(
            client=client, statements=mapped, biolink=biolink, same=[("urn:a", "urn:b")]
        )
        assert [(row.nodes, row.kinds) for row in conflicts.itertuples()] == [
            (("urn:a", "urn:b"), ("chemical entity", "protein")),
            (("urn:d",), ("gene", "protein")),
        ]
        assert "urn:a urn:b" in caplog.text
        kept = {"urn:a": "protein", "urn:b": "protein", "urn:d": "gene"}
        assert keep_kinds(client=client, statements=mapped, biolink=biolink, kept=kept) == 2
        assert {(q.subject.value, q.object.value) for q in mapped} == {
            ("urn:a", BL + "Protein"),
            ("urn:d", BL + "Gene"),
            ("urn:e", BL + "Gene"),
        }
        session = client.session_metadata()
        assert [s["name"] for s in session["steps"]] == ["Kind conflicts", "Keep kinds"]
        assert all(q["query"].lstrip().startswith("CONSTRUCT") for q in session["queries"])


def test_a_view_shows_qualifiers_and_keeps_the_nodes_an_edge_names(tmp_path):
    """Direct edges without reaction and association nodes: a regulation shows its direction,
    an enzyme left without edges stays (an edge's catalyst_qualifier names it), others go."""
    from rdfsolve.conversion import Biolink, derive_associations, derive_conversions

    (tmp_path / "b.yaml").write_text(BIOLINK_YAML)
    biolink = Biolink.read(tmp_path / "b.yaml")
    data = f"""<urn:r1> a <{BL}MolecularActivity> ; <{BL}has_input> <urn:c1> ; <{BL}has_output> <urn:c2> .
    <urn:g1> a <{BL}Gene> ; <{BL}catalyzes> <urn:r1> . <urn:alone> a <{BL}Gene> .
    <urn:c1> a <{BL}ChemicalEntity> . <urn:c2> a <{BL}ChemicalEntity> .
    <urn:g2> a <{BL}Gene> . <urn:g3> a <{BL}Gene> .
    <urn:i1> a <{BL}Association> ; <{BL}subject> <urn:g2> ; <{BL}predicate> <{BL}regulates> ;
        <{BL}object> <urn:g3> ; <{BL}object_direction_qualifier> "increased" ."""
    mapped = ox.Dataset(ox.parse(data.encode(), ox.RdfFormat.TURTLE))
    with biolink_client() as client:
        derive_conversions(client, mapped, biolink)
    graph = PropertyGraph.from_rdf(mapped, identity=Identity())
    derive_associations(graph, biolink)
    network = graph.to_networkx(
        without=[
            "MolecularActivity",
            "Association",
            "ChemicalEntityToChemicalDerivationAssociation",
        ],
        qualifiers=["object_direction_qualifier"],
        keep="catalyst_qualifier",
    )
    assert sorted(d["type"] for *_, d in network.edges(data=True)) == [
        "derives_into",
        "regulates (increased)",
    ]
    assert sorted(network.nodes) == ["urn:c1", "urn:c2", "urn:g1", "urn:g2", "urn:g3"]
