"""The readers of mined patterns know the subjects without a type (subject_binding "untyped").

Two samples: a STRING-like graph where no subject has a type, and a mixed graph where typed
and untyped subjects share properties and one record is typed rdfs:Resource explicitly (a typed
pattern of that class, which must stay apart from the untyped ones). Each reader is checked: the
client views and queries, the exporters (Pydantic, LinkML, NetworkX, RDF-config), conversions,
property graphs, the MCP view, the mappings (signatures, routes, SSSOM), connectivity, VoID
comparison, ontology usage, selections and the term release.
"""

from __future__ import annotations

from functools import cache

import pytest
from rdflib import RDF, RDFS, Dataset, Graph, URIRef

from rdfsolve.client.api import Client
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models import UNTYPED_SUBJECT, UNTYPED_SUBJECTS_LABEL, MinedSchema
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm
from tests.mining.test_untyped_subjects import FUNCTIONAL, MIXED, string_like

RESOURCE = str(RDFS.Resource)
E = "https://example.org/"
# The mixed sample, with one record typed rdfs:Resource by the data itself.
MIXED_R = MIXED + f'\ne:r a <{RESOURCE}> ; e:name "r" .\n'


def string_graph() -> Graph:
    """The STRING-like sample in one default graph."""
    graph = Graph()
    for triple in string_like().triples((None, None, None)):
        graph.add(triple)
    return graph


def mixed_graph() -> Graph:
    """The mixed sample: typed (e:A, e:B, rdfs:Resource) and untyped subjects."""
    return Graph().parse(data=MIXED_R, format="turtle")


@cache
def mined(name: str) -> MinedSchema:
    """Mine a sample through the local endpoint, with one example per pattern."""
    graph = string_graph() if name == "string" else mixed_graph()
    with SchemaMiner.from_graph(graph, delay=0, enrich=True, examples_per_pattern=1) as miner:
        return miner.mine(dataset_name=name)


def client(name: str) -> Client:
    """A client of a sample on its local RDF."""
    graph = string_graph() if name == "string" else mixed_graph()
    data = Dataset()
    for triple in graph:
        data.add(triple)
    return Client(mined(name), data)


def no_resource_class(text: str) -> None:
    """Assert that a query or an export never states the class rdfs:Resource."""
    assert f"a <{RESOURCE}>" not in text and "a rdfs:Resource" not in text


# -- the samples ------------------------------------------------------------------------------


def test_the_samples_have_the_untyped_and_typed_patterns_the_readers_need():
    string, mixed = mined("string"), mined("mixed")
    assert string.patterns and all(p.untyped_subject for p in string.patterns)
    assert string.get_classes() == []
    typed_resource = [
        p for p in mixed.patterns if p.subject_class == RESOURCE and not p.untyped_subject
    ]
    untyped = {p.property_uri for p in mixed.patterns if p.untyped_subject}
    assert typed_resource and untyped == {E + "name", E + "link", E + "part"}
    assert all(e.untyped_subject for e in string.enrichment.examples)


# -- examples ---------------------------------------------------------------------------------


def test_an_untyped_example_keeps_its_binding_and_states_no_type():
    """The binding is written only when untyped; the RDF of an example states no type."""
    subject = RdfTerm(kind="uri", value="http://x.org/s")
    value = RdfTerm(kind="literal", value="v")
    typed = PatternExample(
        subject_class=E + "A", property_uri=E + "p", subject=subject, value=value
    )
    untyped = PatternExample(
        subject_class=UNTYPED_SUBJECT,
        property_uri=E + "p",
        subject=subject,
        value=value,
        subject_binding="untyped",
    )
    assert "subject_binding" not in typed.model_dump(mode="json")
    assert untyped.model_dump(mode="json")["subject_binding"] == "untyped"
    assert PatternExample.model_validate(untyped.model_dump(mode="json")).untyped_subject
    with pytest.raises(ValueError, match="untyped example requires"):
        PatternExample(
            subject_class=E + "A",
            property_uri=E + "p",
            subject=subject,
            value=value,
            subject_binding="untyped",
        )
    graph = mined("string").enrichment.to_rdf_graph()
    assert not list(graph.triples((None, RDF.type, None)))
    assert not list(graph.triples((RDFS.Resource, None, None)))
    assert list(graph.triples((None, URIRef(str(FUNCTIONAL)), None)))


# -- client -----------------------------------------------------------------------------------


def test_the_client_views_untyped_subjects_apart_from_the_classes_and_binds_them_by_property():
    with client("string") as found:
        assert found.models == {}
        model = found.untyped_model
        assert model is not None and found.model(UNTYPED_SUBJECTS_LABEL) is model
        assert list(found.types()["Class"]) == [UNTYPED_SUBJECTS_LABEL]
        assert found.type_name(model) == UNTYPED_SUBJECTS_LABEL
        records = found.sample(model, limit=4)
        query = next(q for q in found.queries if "ORDER BY ?s" in q)
        no_resource_class(query)
        assert "FILTER NOT EXISTS" in query and str(FUNCTIONAL) in query
        assert [r.uri for r in records] == [
            f"http://string-db.org/network/9606.ENSP{i}" for i in range(1, 5)
        ]
        assert not records[0].rdf_type
        assert found.find("P1").records[0].uri.endswith("ENSP2")
        no_resource_class(found.queries[-2])


def test_mixed_data_keeps_the_typed_rdfs_resource_class_apart_from_the_untyped_view():
    with client("mixed") as found:
        typed = found.model(RESOURCE)
        untyped = found.untyped_model
        assert typed is not untyped and typed.rdf_class_iri == RESOURCE
        typed_props = {
            i.json_schema_extra["rdf_property_iri"]
            for i in typed.model_fields.values()
            if isinstance(i.json_schema_extra, dict) and "rdf_property_iri" in i.json_schema_extra
        }
        assert typed_props == {E + "name"}
        assert {
            i.json_schema_extra["rdf_property_iri"]
            for i in untyped.model_fields.values()
            if isinstance(i.json_schema_extra, dict) and "rdf_property_iri" in i.json_schema_extra
        } == {E + "name", E + "link", E + "part"}
        # Only the subjects without any type: e:a, e:b and the typed e:r are left out.
        assert [r.uri for r in found.sample(untyped, limit=10)] == [E + "u", E + "v"]
        assert [r.uri for r in found.sample(typed, limit=10)] == [E + "r"]
        # A link to a class from untyped subjects is drawn from "untyped subjects".
        from rdfsolve.client.diagram import link_diagram

        assert "untyped subjects" in link_diagram(found, "B", ["^link"], fenced=False)
        authored = found.create(UNTYPED_SUBJECTS_LABEL, uri=E + "new")
        assert authored.rdf_type == []


# -- exporters --------------------------------------------------------------------------------


def test_linkml_networkx_and_rdf_config_name_the_untyped_subjects_and_claim_no_class():
    from rdfsolve.schema_models.exporters.linkml import to_linkml
    from rdfsolve.schema_models.exporters.networkx import to_networkx
    from rdfsolve.schema_models.exporters.rdfconfig import to_rdfconfig

    string = mined("string")
    linkml = to_linkml(string)
    assert set(linkml.classes) == {"UntypedSubject"}
    mixin = linkml.classes["UntypedSubject"]
    assert mixin.mixin and not mixin.class_uri
    assert set(mixin.slots) == {linkml.slots[s].name for s in linkml.slots}
    mixed = to_linkml(mined("mixed"))
    assert RESOURCE in {c.class_uri for c in mixed.classes.values()}  # the typed e:r
    assert "UntypedSubject" in mixed.classes
    graph = to_networkx(mined("mixed"))
    assert graph.nodes[UNTYPED_SUBJECTS_LABEL]["untyped"] is True
    assert graph.has_edge(UNTYPED_SUBJECTS_LABEL, E + "B")
    model = to_rdfconfig(string)["model"]
    assert "UntypedSubject" in model and RESOURCE not in model
    no_resource_class(model)


# -- conversions ------------------------------------------------------------------------------


def test_a_conversion_query_is_checked_against_the_untyped_patterns_and_rules_need_a_class():
    from pathlib import Path

    from rdfsolve.conversion import Query, Rule
    from rdfsolve.schema_models.exporters.shacl import untyped_shape_iri

    query = Query(
        Path("string-to-biolink.rq"),
        f"""CONSTRUCT {{ ?x <https://w3id.org/biolink/vocab/interacts_with> ?y }}
WHERE {{ ?x <{FUNCTIONAL}> ?y . ?y a <{RESOURCE}> . }}""",
        {},
    )
    rows = {row["pattern"]: row for row in query.check(mined("string"))}
    row = rows[f"?x <{FUNCTIONAL}> ?y"]
    assert row["statements"] == 6 and row["subject"] == UNTYPED_SUBJECTS_LABEL
    assert row["shape"] == untyped_shape_iri("string", str(FUNCTIONAL))
    assert rows[f"?y a <{RESOURCE}>"]["found"] is False
    with client("string") as found, pytest.raises(ValueError, match="without a type"):
        Rule.between(found, UNTYPED_SUBJECTS_LABEL, "a", value="biolink:Protein")


# -- property graph ---------------------------------------------------------------------------


def test_property_graphs_fold_and_count_classes_only():
    from rdfsolve.property_graph import PropertyGraph, _schema_labels, suggest_folds

    assert suggest_folds(mined("string")) == []
    assert all(f.cls != RESOURCE for f in suggest_folds(mined("mixed")))
    assert RESOURCE not in _schema_labels(mined("string"))
    built = PropertyGraph.from_rdf(mixed_graph(), schema=mined("mixed"))
    keys = {key for key in built.patterns if key[0] == RESOURCE}
    assert keys == {(RESOURCE, E + "name", "Literal")}  # the typed e:r only
    assert built.report()["node_types"]["(no class)"] >= 2  # e:u and e:v


# -- MCP view ---------------------------------------------------------------------------------


def test_the_mcp_view_lists_untyped_subjects_as_their_own_group():
    from rdfsolve.mcp.tools import Toolbox
    from rdfsolve.mcp.view import SchemaView

    view = SchemaView(mined("string"))
    assert "Subjects without a type" in view.overview()
    card = view.untyped_card()
    assert str(FUNCTIONAL).rsplit("/", 1)[-1] in card and "Not a class" in card
    assert view.match_classes(RESOURCE) == []
    mixed = SchemaView(mined("mixed"))
    resource_card = mixed.card(RESOURCE)
    assert "part" not in resource_card and "link" not in resource_card
    b_card = mixed.card(E + "B")
    assert f"({UNTYPED_SUBJECTS_LABEL})" in b_card
    assert all(
        step[0] != RESOURCE and step[2] != RESOURCE
        for groups in (mixed.routes(E + "A", E + "B", 3),)
        for group in groups
        for route in group
        for step in route
    )
    assert f"({UNTYPED_SUBJECTS_LABEL})" in mixed.search(["link"])
    with client("string") as found:
        tools = Toolbox(found, probe_timeout=None)
        assert "Not a class" in tools.schema([UNTYPED_SUBJECTS_LABEL])["text"]
        text = tools.find("P1", in_class=UNTYPED_SUBJECTS_LABEL)["text"]
        assert f"({UNTYPED_SUBJECTS_LABEL})" in text and RESOURCE not in text


# -- mappings ---------------------------------------------------------------------------------

UP = "http://purl.uniprot.org/uniprot/"
GENE_DATA = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}Q9Y6K9> .
"""
# A STRING-like target: two proteins without a type, and one typed protein.
PROTEIN_DATA = f"""
<{UP}P38398> <urn:name> "BRCA1" .
<{UP}Q9Y6K9> <urn:name> "NEMO" .
<{UP}P04637> a <urn:Protein> ; <urn:name> "p53" .
"""


def mapping_schemas() -> tuple[MinedSchema, MinedSchema]:
    """The schemas of the gene source and of the mostly untyped protein target."""

    def mine(data: str, name: str) -> MinedSchema:
        graph = Graph().parse(data=data, format="turtle")
        with SchemaMiner.from_graph(graph, delay=0, enrich=True, examples_per_pattern=2) as m:
            return m.mine(dataset_name=name)

    return mine(GENE_DATA, "genes"), mine(PROTEIN_DATA, "proteins")


def test_links_to_untyped_subjects_are_inferred_verified_and_kept_out_of_class_mappings():
    from rdfsolve.mappings.signatures import (
        UNTYPED,
        infer_links,
        members,
        read_links,
        signatures,
        verify,
        write_links,
    )
    from rdfsolve.mappings.sssom import links_to_sssom

    genes, proteins = mapping_schemas()
    assert UNTYPED in signatures(proteins).subjects["uniprot"]
    links = [
        link for link in infer_links({"genes": genes, "proteins": proteins}) if link.kind == "join"
    ]
    untyped = next(link for link in links if link.target_class == UNTYPED)
    no_resource_class(members("?t", UNTYPED))
    source = Client(genes, Dataset().parse(data=GENE_DATA, format="turtle"))
    target = Client(proteins, Dataset().parse(data=PROTEIN_DATA, format="turtle"))
    with source, target:
        evidence = verify(untyped, source, target, sample=None)
        # The typed P04637 is not an untyped subject: two of the three values are found.
        assert (evidence.sampled, evidence.found) == (3, 2)
        for query in target.queries:
            no_resource_class(query)
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "links.tsv"
        write_links(path, [evidence])
        assert read_links(path)[0].link == untyped
    mapping = links_to_sssom(
        [evidence], {"genes": genes, "proteins": proteins}, "https://example.org/set"
    )
    assert mapping.df.empty


def test_routes_never_pass_through_untyped_subjects():
    from rdfsolve.mappings.routes import Route, _source_pattern, propose_segments
    from rdfsolve.mappings.signatures import UNTYPED, Link

    mixed = mined("mixed")
    link = Link("join", "x", UNTYPED, E + "link", "uniprot", "y", target_class=E + "B")
    befores, afters = propose_segments(link, mixed, mixed)
    assert befores == [()]
    assert all(not step.untyped_subject for path in afters for step in path)
    typed = Link("join", "x", E + "B", E + "name", "uniprot", "y", target_class=E + "A")
    befores, _ = propose_segments(typed, mixed, mixed)
    assert all(not step.untyped_subject for path in befores for step in path)
    pattern = _source_pattern(Route(link))
    no_resource_class(pattern)
    assert "FILTER NOT EXISTS" in pattern


# -- analyses ---------------------------------------------------------------------------------


def test_connectivity_void_comparison_and_ontology_usage_do_not_count_rdfs_resource():
    from rdfsolve.analysis.connectivity import build_connectivity
    from rdfsolve.analysis.void_comparison import compare_void_with_mined
    from rdfsolve.ontology.usage import observed_terms_from_patterns

    assert len(build_connectivity({"s1": mined("string")})) == 0  # the class graph
    graph = build_connectivity(
        {"s1": mined("string"), "s2": mined("string")}, untyped_subjects=True
    )
    node = ("s1", UNTYPED_SUBJECTS_LABEL)
    assert graph.nodes[node]["untyped"] is True
    assert not [e for e in graph.edges(data=True) if e[2]["kind"] == "shared_class"]
    comparison = compare_void_with_mined([], mined("mixed").patterns)
    assert comparison.classes.mined_only == 3  # e:A, e:B and the typed rdfs:Resource
    string = compare_void_with_mined([], mined("string").patterns)
    assert string.classes.mined_only == 0 and string.patterns.mined_only == 6
    assert RESOURCE not in observed_terms_from_patterns(mined("string").patterns).classes
    dumped = [p.model_dump(mode="json") for p in mined("string").patterns]
    assert RESOURCE not in observed_terms_from_patterns(dumped).classes


def test_a_selection_and_the_term_release_keep_the_binding():
    from rdfsolve.client.terms import TermRelease, regroup, to_patterns
    from rdfsolve.mining.term_release import term_rows
    from rdfsolve.schema_models.selection import SchemaSelection

    mixed = mined("mixed")
    selection = SchemaSelection(source=mixed, fields=[(RESOURCE, E + "name")])
    assert [p.untyped_subject for p in selection.patterns] == [False]
    with pytest.raises(ValueError):
        SchemaSelection(source=mixed, fields=[(RESOURCE, E + "part")])
    import polars as pl

    terms = term_rows(mixed.patterns)
    assert set(terms["subject_binding"]) == {"type", "untyped"}
    release = TermRelease(
        terms,
        pl.DataFrame(
            {"class": [], "overlaps": []},
            schema={"class": pl.String, "overlaps": pl.List(pl.String)},
        ),
        {},
    )
    back = to_patterns(regroup(release, {RESOURCE: E + "A"}))
    untyped = [p for p in back if p.untyped_subject]
    assert {p.property_uri for p in untyped} == {E + "name", E + "link", E + "part"}
    assert all(p.subject_class == UNTYPED_SUBJECT for p in untyped)
    # The typed rdfs:Resource row is grouped; the untyped rows are not.
    assert any(p.subject_class == E + "A" and p.property_uri == E + "name" for p in back)
    old = release.terms.drop("subject_binding")
    assert all(
        not p.untyped_subject
        for p in to_patterns(regroup(TermRelease(old, release.classes, {}), {}))
    )
