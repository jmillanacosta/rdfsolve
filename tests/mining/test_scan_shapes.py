"""rdfsolve.mining.scan_shapes: the observed shapes validate the data they were read from, and
a change beyond what was observed is a violation."""

from __future__ import annotations

import pytest
from rdflib import OWL, RDF, RDFS, XSD, BNode, Dataset, Graph, Literal, Namespace, URIRef

from rdfsolve.mining.scan import store_from_graph
from rdfsolve.mining.scan_shapes import class_property_profiles, observed_shapes

pyshacl = pytest.importorskip("pyshacl")

E = Namespace("urn:ex:")
SH = Namespace("http://www.w3.org/ns/shacl#")
VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")


def _graph() -> Graph:
    graph = Graph()
    graph.add((E.Sub, RDFS.subClassOf, E.A))
    for n in range(3):
        a = E[f"a{n}"]
        graph.add((a, RDF.type, E.A))
        graph.add((a, E.size, Literal(n, datatype=XSD.integer)))
        graph.add((a, RDFS.label, Literal(f"a {n}", lang="en")))
        for k in range(n + 1):  # 1 to 3 values
            graph.add((a, E.tag, Literal(f"t{k}")))
        graph.add((a, E.link, E.b1))
    graph.add((E.a0, E.size, Literal("2.5", datatype=XSD.decimal)))  # two datatypes
    graph.add((E.a1, RDFS.label, Literal("a een", lang="nl")))
    graph.add((E.a2, RDFS.label, Literal("plain")))  # a label without a tag
    graph.add((E.a0, RDF.type, E.C))  # two classes
    graph.add((E.s1, RDF.type, E.Sub))  # an instance of A through rdfs:subClassOf
    graph.add((E.s1, E.link, E.u1))  # an IRI without a type
    graph.add((E.s1, E.size, Literal("many", datatype=XSD.integer)))  # ill-typed
    graph.add((E.b1, RDF.type, E.B))
    graph.add((E.b1, RDF.type, OWL.Thing))
    node, typed = BNode(), BNode()
    graph.add((E.a1, E.part, node))  # a blank node without a type
    graph.add((E.a2, E.part, typed))
    graph.add((typed, RDF.type, E.B))  # a typed blank node
    graph.add((node, E.name, Literal("x")))
    graph.add((E.u2, E.link, E.a1))  # an untyped subject: no shape targets it
    return graph


def _shapes(store, **kwargs) -> Graph:
    return observed_shapes(class_property_profiles(store), dataset_name="fixture", **kwargs)


def _validate(data: Graph, shapes: Graph) -> tuple[bool, set[str]]:
    conforms, report, _ = pyshacl.validate(data, shacl_graph=shapes)
    components = {
        str(o).rsplit("#", 1)[-1] for o in report.objects(None, SH.sourceConstraintComponent)
    }
    return conforms, components


@pytest.fixture
def store(tmp_path):
    return store_from_graph(_graph(), tmp_path / "store")


def test_profiles_count_the_instances_and_values(store):
    profiles = {c.class_iri: c for c in class_property_profiles(store)}
    a = profiles[str(E.A)]
    assert a.instances == 4  # a0, a1, a2 and s1 (rdf:type E.Sub, E.Sub rdfs:subClassOf E.A)
    props = {p.property_uri: p for p in a.properties}
    tag = props[str(E.tag)]
    assert (tag.min_count, tag.max_count) == (0, 3)
    assert tag.values_per_instance == {0: 1, 1: 1, 2: 1, 3: 1}
    size = props[str(E.size)]
    assert size.datatypes == {str(XSD.integer): 4, str(XSD.decimal): 1}
    assert size.ill_typed == {str(XSD.integer): 1}
    label = props[str(RDFS.label)]
    assert label.languages == {"en": 3, "nl": 1}
    assert label.datatypes == {str(RDF.langString): 4, str(XSD.string): 1}
    link = props[str(E.link)]
    assert link.object_classes == {str(E.B): 3, str(OWL.Thing): 3}
    assert link.unclassed == {"IRI": 1}
    assert link.distinct_objects_by_kind == {"IRI": 2}
    part = props[str(E.part)]
    assert part.object_classes == {str(E.B): 1} and part.unclassed == {"BlankNode": 1}


@pytest.mark.parametrize("closed", [False, True])
def test_shapes_validate_their_data(store, closed):
    shapes = _shapes(store, closed=closed)
    assert not list(shapes.subjects(SH.deactivated, None))
    assert _validate(_graph(), shapes) == (True, set())


def test_counts_are_annotations_on_partitions(store):
    shapes = _shapes(store)
    shape = next(shapes.subjects(SH.targetClass, E.A))
    partition = shapes.value(shape, URIRef("http://purl.org/dc/terms/source"))
    assert shapes.value(partition, VOID.entities).toPython() == 4
    tag = next(
        p
        for p in shapes.objects(partition, VOID.propertyPartition)
        if shapes.value(p, VOID.property) == E.tag
    )
    assert shapes.value(tag, VOID.triples).toPython() == 6
    assert shapes.value(tag, VOID.distinctSubjects).toPython() == 3
    assert shapes.value(tag, VOID_EXT.distinctLiterals).toPython() == 3


def test_a_change_beyond_the_observations_is_a_violation(store):
    shapes = _shapes(store, closed=True)
    removed = _graph()
    removed.remove((E.a0, E.link, E.b1))  # every instance of A has one link
    assert _validate(removed, shapes) == (False, {"MinCountConstraintComponent"})
    more = _graph()
    more.add((E.a2, E.tag, Literal("t9")))  # a fourth value
    assert _validate(more, shapes) == (False, {"MaxCountConstraintComponent"})
    other = _graph()
    other.remove((E.a1, E.tag, Literal("t1")))
    other.add((E.a1, E.tag, Literal(1, datatype=XSD.integer)))  # a datatype not observed
    assert _validate(other, shapes) == (False, {"DatatypeConstraintComponent"})
    blank = _graph()
    blank.remove((E.a0, E.link, E.b1))
    blank.add((E.a0, E.link, BNode()))  # a kind not observed
    assert not _validate(blank, shapes)[0]
    tagged = _graph()
    tagged.add((E.a1, RDFS.label, Literal("un", lang="fr")))  # a language not observed
    assert not _validate(tagged, shapes)[0]
    extra = _graph()
    extra.add((E.a1, E.unseen, Literal("x")))  # a property not observed, with closed shapes
    assert _validate(extra, shapes) == (False, {"ClosedConstraintComponent"})


def test_ill_typed_literals_are_accepted_as_literals(store):
    shapes = _shapes(store)
    size = next(s for s in shapes.subjects(SH.path, E.size))
    assert "Ill-typed literals" in str(shapes.value(size, SH.description))
    kinds = {shapes.value(m, SH.nodeKind) for m in shapes.transitive_objects(size, None)}
    assert SH.Literal in kinds


def test_shapes_of_a_graph_scope_validate_that_graph(tmp_path):
    data = Dataset(default_union=True)
    first, second = data.graph(URIRef("urn:g:1")), data.graph(URIRef("urn:g:2"))
    for triple in _graph():
        first.add(triple)
    second.add((E.a0, E.tag, Literal("only in g2")))
    second.add((E.a9, RDF.type, E.A))
    store = store_from_graph(data, tmp_path / "store")
    view = store.view(["urn:g:1"])
    shapes = observed_shapes(
        class_property_profiles(view), dataset_name="fixture", graph_uris=["urn:g:1"]
    )
    alone = Graph()
    for triple in first:
        alone.add(triple)
    assert _validate(alone, shapes) == (True, set())
    merged = Graph()
    for triple in data.triples((None, None, None)):
        merged.add(triple)
    assert not _validate(merged, shapes)[0]  # a9 has no tag, size or link: below the minima


def test_the_census_gives_back_the_source_datatypes(tmp_path):
    """QLever reports every integer as xsd:int; the census of the files restores the source's."""
    index, source = Graph(), Graph()
    for n, datatype in ((1, XSD.nonNegativeInteger), (2, XSD.integer)):
        index.add((E[f"a{n}"], RDF.type, E.A))
        index.add((E[f"a{n}"], E.size, Literal(str(n), datatype=XSD.int)))
        index.add((E[f"a{n}"], E.rank, Literal(str(n), datatype=XSD.int)))
        source.add((E[f"a{n}"], RDF.type, E.A))
        source.add((E[f"a{n}"], E.size, Literal(str(n), datatype=XSD.nonNegativeInteger)))
        source.add((E[f"a{n}"], E.rank, Literal(str(n), datatype=datatype)))
    census = {
        str(E.size): {str(XSD.nonNegativeInteger): 2},
        str(E.rank): {str(XSD.nonNegativeInteger): 1, str(XSD.integer): 1},
    }
    store = store_from_graph(index, tmp_path / "store")
    (profile,) = class_property_profiles(store, census=census)
    props = {p.property_uri: p for p in profile.properties}
    assert props[str(E.size)].datatypes == {str(XSD.nonNegativeInteger): 2}
    assert props[str(E.rank)].datatype_options == {
        str(XSD.int): [str(XSD.integer), str(XSD.nonNegativeInteger)]
    }
    shapes = observed_shapes([profile], dataset_name="fixture")
    assert _validate(source, shapes) == (True, set())
    assert not _validate(index, shapes)[0]  # the folded datatypes are not the source's


def test_shapes_use_registered_terms(store):
    from rdfsolve.vocab import unregistered_terms

    source = {str(t) for triple in _graph() for t in triple if isinstance(t, URIRef)}
    assert unregistered_terms(_shapes(store, closed=True), source) == set()


def test_void_gives_the_totals_of_class_property_partitions():
    from rdfsolve.evidence.observed import collect_property_usage_evidence
    from rdfsolve.mining.miner import SchemaMiner
    from rdfsolve.schema_models.exporters.void import add_property_usage, to_void_graph

    with SchemaMiner.from_graph(_graph(), delay=0) as miner:
        schema = miner.mine(dataset_name="fixture")
        usage = collect_property_usage_evidence(
            dataset_id="fixture",
            classes=sorted({p.subject_class for p in schema.patterns}),
            class_entity_counts=schema.about.class_entity_counts,
            helper=miner.helper,
            graph_uris=None,
        )
    graph = to_void_graph(schema)

    def partition(cls, prop):
        found = [
            p
            for c in graph.subjects(VOID["class"], cls)
            for p in graph.objects(c, VOID.propertyPartition)
            if graph.value(p, VOID.property) == prop
        ]
        assert len(found) == 1
        return found[0]

    tag = partition(E.A, E.tag)  # literals only: exact from the patterns
    assert graph.value(tag, VOID.triples).toPython() == 6
    assert graph.value(tag, VOID_EXT.distinctLiterals).toPython() == 3
    link = partition(E.A, E.link)  # objects with classes: no total from the patterns
    assert graph.value(link, VOID.triples) is None
    part = partition(E.A, E.part)
    assert graph.value(part, VOID_EXT.distinctBlankNodeObjects).toPython() == 2
    assert add_property_usage(graph, schema, usage) == len(usage.records)
    assert graph.value(link, VOID.triples).toPython() == 3  # a0, a1, a2 -> b1
    assert graph.value(link, VOID.distinctObjects).toPython() == 1
    assert graph.value(tag, VOID.triples).toPython() == 6
