"""rdfsolve.mining.scan_enrichment: labels, enrichment, restrictions, lists, ontology axioms and
metadata computed from the rows of a store equal what the SPARQL miner's functions return on the
same rdflib graph. Deterministic outputs are compared exactly; examples, which the miner takes in
endpoint order, by a witness rule (each scan example is a row of the miner's example query).
"""

from __future__ import annotations

import pytest
from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.compare import isomorphic

from rdfsolve.metadata import query_metadata_document
from rdfsolve.mining.collections import discover_collections
from rdfsolve.mining.enrichment import example_query, query_enrichment
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.ontology_extraction import OntologyMiner
from rdfsolve.mining.restrictions import mine_restriction_patterns
from rdfsolve.mining.scan import store_from_graph
from rdfsolve.mining.scan_enrichment import (
    collections,
    enrichment,
    label_map,
    metadata_document,
    ontology_structure,
    pattern_labels,
    restriction_patterns,
    term_patterns,
)

E = Namespace("urn:ex:")
OBO = Namespace("http://purl.obolibrary.org/obo/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
OIO = Namespace("http://www.geneontology.org/formats/oboInOwl#")
VOID = Namespace("http://rdfs.org/ns/void#")
DCT = Namespace("http://purl.org/dc/terms/")


def _restriction(graph: Graph, prop, form, filler) -> BNode:
    node = BNode()
    graph.add((node, RDF.type, OWL.Restriction))
    graph.add((node, OWL.onProperty, prop))
    graph.add((node, form, filler))
    return node


def _graph() -> Graph:
    g = Graph()
    for cls in (E.Gene, E.Protein, E.Molecule, E.X, E.Y, E.Recipe, OBO.GO_0001, OBO.GO_0002):
        g.add((cls, RDF.type, OWL.Class))
    # labels and definitions in several languages and predicates
    g.add((E.Gene, RDFS.label, Literal("gene", lang="en")))
    g.add((E.Gene, RDFS.label, Literal("Gen", lang="de")))
    g.add((E.Gene, RDFS.label, Literal("a gene")))
    g.add((E.Gene, SKOS.prefLabel, Literal("Gene")))
    g.add((E.Gene, OBO.IAO_0000115, Literal("A unit of heredity.", lang="en")))
    g.add((E.Gene, RDFS.comment, Literal("two\nlines\tand a tab")))
    g.add((E.Gene, OIO.hasExactSynonym, Literal("locus")))
    g.add((E.Protein, DCT.title, Literal("Protein")))
    g.add((E.Protein, SKOS.altLabel, Literal("polypeptide", lang="fr")))
    g.add((E.encodes, RDFS.label, Literal("encodes", lang="en-GB")))
    g.add((E.encodes, RDFS.label, Literal("codiert", lang="de")))
    g.add((OBO.BFO_0000050, RDFS.label, Literal("part of", lang="en")))
    g.add((OBO.BFO_0000050, RDFS.label, Literal("partie de", lang="fr")))
    # axioms
    g.add((E.Protein, RDFS.subClassOf, E.Molecule))
    g.add((E.Molecule, OWL.deprecated, Literal(True)))
    g.add((E.Gene, OWL.disjointWith, E.Protein))
    g.add((E.X, OWL.disjointWith, E.Y))  # touches no schema term
    g.add((E.Gene, OWL.equivalentClass, E.Gene2))
    g.add((E.P1, OWL.equivalentClass, E.P2))  # touches no schema term
    g.add((E.encodes, RDFS.domain, E.Gene))
    g.add((E.encodes, RDFS.range, E.Protein))
    g.add((E.encodes, RDFS.subPropertyOf, E.related))
    g.add((E.related, RDFS.subPropertyOf, E.top))
    g.add((E.encodes, OWL.inverseOf, E.encodedBy))
    g.add((E.other, OWL.inverseOf, E.other2))  # touches no schema term
    g.add((E.encodes, RDF.type, OWL.TransitiveProperty))
    # instances
    for n in range(3):
        gene = E[f"g{n}"]
        g.add((gene, RDF.type, E.Gene))
        g.add((gene, E.encodes, E[f"p{n % 2}"]))
        g.add((gene, E.name, Literal(f"gene {n}")))
        g.add((gene, E.link, E[f"u{n}"]))
    g.add((E.p0, RDF.type, E.Protein))
    g.add((E.p1, RDF.type, E.Protein))
    g.add((E.g0, E.score, Literal("1.5", datatype=XSD.double)))
    g.add((E.g1, E.size, Literal(3, datatype=XSD.integer)))
    g.add((E.g2, E.name, Literal("gène", lang="fr")))
    part = BNode()
    g.add((E.g0, E.part, part))
    g.add((part, E.note, Literal("x")))
    # restrictions: SubClassOf some, only, value, a class expression filler, EquivalentTo
    g.add((E.Gene, RDFS.subClassOf, _restriction(g, E.encodes, OWL.someValuesFrom, E.Protein)))
    g.add((E.Protein, RDFS.subClassOf, _restriction(g, E.partOf, OWL.allValuesFrom, E.Complex)))
    g.add((E.Gene, RDFS.subClassOf, _restriction(g, E.taxon, OWL.hasValue, E.human)))
    for go in (OBO.GO_0001, OBO.GO_0003):
        g.add(
            (go, RDFS.subClassOf, _restriction(g, OBO.BFO_0000050, OWL.someValuesFrom, OBO.GO_0002))
        )
    union = BNode()
    Collection(g, (members := BNode()), [OBO.GO_0001, OBO.GO_0002])
    g.add((union, OWL.unionOf, members))
    g.add((union, RDF.type, OWL.Class))
    g.add(
        (OBO.GO_0005, RDFS.subClassOf, _restriction(g, OBO.BFO_0000050, OWL.someValuesFrom, union))
    )
    expression, cells = BNode(), BNode()
    g.add((expression, RDF.type, OWL.Class))
    inner = _restriction(g, OBO.BFO_0000050, OWL.someValuesFrom, OBO.GO_0001)
    Collection(g, cells, [OBO.GO_0002, inner])
    g.add((expression, OWL.intersectionOf, cells))
    g.add((OBO.GO_0004, OWL.equivalentClass, expression))
    # data lists: literals of several kinds, an empty list, a broken list
    steps = BNode()
    Collection(g, steps, [Literal("a"), Literal("b", lang="en"), Literal(3), E.p0])
    g.add((E.r1, RDF.type, E.Recipe))
    g.add((E.r1, E.steps, steps))
    g.add((E.r2, RDF.type, E.Recipe))
    g.add((E.r2, E.steps, RDF.nil))
    broken = BNode()
    g.add((broken, RDF.first, Literal("x")))
    g.add((E.r3, RDF.type, E.Recipe))
    g.add((E.r3, E.steps, broken))
    # dataset metadata with two blank-node levels and a partition
    g.add((E.ds, RDF.type, VOID.Dataset))
    g.add((E.ds, DCT.title, Literal("Fixture")))
    g.add((E.ds, VOID.triples, Literal(10, datatype=XSD.integer)))
    publisher, address, partition = BNode(), BNode(), BNode()
    g.add((E.ds, DCT.publisher, publisher))
    g.add((publisher, E.name, Literal("pub")))
    g.add((publisher, E.address, address))
    g.add((address, E.city, Literal("Maastricht")))
    g.add((E.ds, VOID.classPartition, partition))
    g.add((partition, VOID["class"], E.Gene))
    return g


@pytest.fixture(scope="module")
def setup(tmp_path_factory):
    graph = _graph()
    miner = SchemaMiner.from_graph(graph, delay=0, examples_per_pattern=2)
    with miner:
        schema = miner.mine(dataset_name="fixture")
    store = store_from_graph(graph, tmp_path_factory.mktemp("store") / "store")
    helper = SchemaMiner.from_graph(graph, delay=0)._helper
    return graph, schema, store, helper


def _dump(items) -> set[str]:
    return {item.model_dump_json() for item in items}


def _norm(items) -> set[str]:
    return {item if isinstance(item, str) else item.model_dump_json() for item in items}


def test_pattern_labels(setup):
    _, schema, store, _ = setup
    bare = [
        p.model_copy(update=dict.fromkeys(("subject_label", "property_label", "object_label")))
        for p in schema.patterns
    ]
    labelled = pattern_labels(store, bare)
    fields = ("subject_label", "property_label", "object_label")
    assert [[getattr(p, f) for f in fields] for p in labelled] == [
        [getattr(p, f) for f in fields] for p in schema.patterns
    ]
    labels = label_map(store, [str(E.Gene), str(E.Protein), str(E.encodes)])
    assert labels == {
        str(E.Gene): "a gene",
        str(E.Protein): "Protein",
        str(E.encodes): "codiert",
    }  # en-GB ranks with de (the miner's rule)


def test_enrichment(setup):
    _, schema, store, helper = setup
    extra = [str(OBO.BFO_0000050)]
    sparql = query_enrichment(schema, helper, examples_per_pattern=2, annotation_iris=extra)
    scan = enrichment(store, schema, examples_per_pattern=2, annotation_iris=extra)
    assert _dump(scan.labels) == _dump(sparql.labels)
    assert _dump(scan.definitions) == _dump(sparql.definitions)
    assert any("\t" in d.text.value for d in scan.definitions)  # rdflib store: tab kept
    # the texts read by the miner's own query through *select*: the same
    exact = enrichment(
        store, schema, examples_per_pattern=0, annotation_iris=extra, select=helper.select
    )
    assert _dump(exact.labels) == _dump(sparql.labels)
    assert _dump(exact.definitions) == _dump(sparql.definitions)
    # class examples: as many as the miner's, each a member of its class, the smallest first
    assert scan.class_examples.keys() == sparql.class_examples.keys()
    for cls, examples in scan.class_examples.items():
        assert len(examples) == len(sparql.class_examples[cls])
        members = {t.value for t in sparql.class_examples[cls]}
        if all(t.kind == "uri" for t in examples) and len(members) < 2:
            assert {t.value for t in examples} == members

    # pattern examples: per (class, property) as many as the miner's, each a row of the miner's
    # example query for one pattern of that class and property (witness rule)
    def by_pair(examples):
        out = {}
        for e in examples:
            out.setdefault((e.subject_class, e.property_uri), []).append(e)
        return out

    scan_pairs, sparql_pairs = by_pair(scan.examples), by_pair(sparql.examples)
    assert {k: len(v) for k, v in scan_pairs.items()} == {
        k: len(v) for k, v in sparql_pairs.items()
    }
    witnesses: dict[tuple, set] = {}
    for pattern in schema.patterns:
        answer = helper.select(example_query(pattern, None, 20, with_dataset=False))
        for row in answer["results"]["bindings"]:
            subject, value = row["subject"], row["value"]
            witnesses.setdefault((pattern.subject_class, pattern.property_uri), set()).add(
                (
                    subject["value"] if subject["type"] == "uri" else "_",
                    value["value"] if value["type"] != "bnode" else "_",
                )
            )
    for key, examples in scan_pairs.items():
        for e in examples:
            subject = e.subject.value if e.subject.kind == "uri" else "_"
            value = e.value.value if e.value.kind != "bnode" else "_"
            assert (subject, value) in witnesses[key], (key, e)
    again = enrichment(store, schema, examples_per_pattern=2, annotation_iris=extra)
    assert again.model_dump() == scan.model_dump()  # deterministic


def test_restriction_patterns(setup):
    graph, _, store, helper = setup
    sparql = mine_restriction_patterns(helper)
    scan = restriction_patterns(store)
    assert sparql.state == scan.state == "complete"
    examples = {"example_subject", "example_filler"}
    assert [p.model_dump(exclude=examples) for p in scan.patterns] == [
        p.model_dump(exclude=examples) for p in sparql.patterns
    ]
    assert {p.filler_namespace for p in scan.patterns} >= {"(class expression)", str(OBO) + "GO_"}
    assert {p.axiom for p in scan.patterns} == {"SubClassOf", "EquivalentTo"}
    forms = (OWL.someValuesFrom, OWL.allValuesFrom, OWL.hasValue)
    fillers = {o for form in forms for o in graph.objects(None, form)}
    for p in scan.patterns:  # each example is a term of its group, the filler a filler
        assert p.example_subject.startswith(p.subject_namespace)
        assert (URIRef(p.example_subject), None, None) in graph
        if p.example_filler is not None:
            assert URIRef(p.example_filler) in fillers
            assert p.example_filler.startswith(p.filler_namespace)


def test_collections(setup):
    graph, _, store, _ = setup
    expected = discover_collections(graph)
    found = collections(store)
    assert [p.model_dump() for p in found] == [p.model_dump() for p in expected]
    steps = next(p for p in found if p.property_uri == str(E.steps))
    assert (steps.list_count, steps.invalid_count, steps.min_length, steps.max_length) == (
        2,
        1,
        0,
        4,
    )


def test_ontology_structure_full_scope(setup):
    _, _, store, helper = setup
    sparql = OntologyMiner(helper, None).mine()
    scan = ontology_structure(store, None, None)
    for field in sparql.model_fields:
        if field != "annotations":
            assert _norm(getattr(scan, field)) == _norm(getattr(sparql, field)), field


def test_ontology_structure_schema_scope(setup):
    _, schema, store, helper = setup
    classes, properties = schema.get_classes(), schema.get_properties()
    sparql = OntologyMiner(
        helper, None, class_iris=classes, property_iris=properties, batch_size=2
    ).mine()
    scan = ontology_structure(store, classes, properties)
    exact = (
        "subclass_relations",
        "subproperty_relations",
        "domain_assertions",
        "range_assertions",
        "deprecated_terms",
        "property_characteristics",
    )
    for field in exact:
        assert _norm(getattr(scan, field)) == _norm(getattr(sparql, field)), field
    # the pairs that touch a selected term: the scan keeps only those (L50); the miner's
    # BIND-in-UNION queries may return every pair of the dataset
    pairs = {
        "equivalent_classes": ({(str(E.Gene), str(E.Gene2))}, ("class1", "class2")),
        "disjoint_classes": ({(str(E.Gene), str(E.Protein))}, ("class1", "class2")),
        "inverse_properties": ({(str(E.encodes), str(E.encodedBy))}, ("property1", "property2")),
    }
    for field, (expected, names) in pairs.items():
        found = {tuple(getattr(r, n) for n in names) for r in getattr(scan, field)}
        mined = {tuple(getattr(r, n) for n in names) for r in getattr(sparql, field)}
        assert found == expected, field
        assert mined >= expected, field
    outside = {str(E.X), str(E.Y), str(E.P1), str(E.P2)}
    assert not outside & set(scan.classes)
    assert set(scan.classes) == set(sparql.classes) - outside


def test_metadata_document(setup):
    _, _, store, helper = setup
    sparql = query_metadata_document(helper)
    scan = metadata_document(store)
    assert len(scan.graph) == len(sparql.graph) == 8  # the partition is not followed
    assert isomorphic(scan.graph, sparql.graph)


def test_metadata_census_restores_datatypes(tmp_path):
    graph = Graph()
    graph.add((E.ds, RDF.type, VOID.Dataset))
    graph.add((E.ds, VOID.triples, Literal("10", datatype=XSD.int)))  # as QLever reports it
    store = store_from_graph(graph, tmp_path / "store")
    census = {str(VOID.triples): {str(XSD.integer): 1}}
    (value,) = metadata_document(store, census).graph.objects(E.ds, VOID.triples)
    assert value.datatype == XSD.integer
    (kept,) = metadata_document(store).graph.objects(E.ds, VOID.triples)
    assert kept.datatype == XSD.int


def _terms_graph() -> Graph:
    g = Graph()
    for term in (OBO.UBERON_1, OBO.UBERON_2, OBO.UBERON_3):
        g.add((term, RDF.type, OWL.Class))
    g.add((OBO.UBERON_2, RDF.type, RDFS.Class))  # a term with two declarations
    g.add((E.note, RDF.type, OWL.AnnotationProperty))
    for n in range(3):
        sample = E[f"s{n}"]
        g.add((sample, RDF.type, E.Specimen))
        g.add((sample, E.tissue, OBO[f"UBERON_{n + 1}"]))
        g.add((sample, E.weight, Literal(n)))
    g.add((E.s0, E.tissue, E.nonterm))
    g.add((E.s1, RDF.type, E.Batch))  # two classes
    g.add((E.s2, E.tissue, OBO.UBERON_1))
    g.add((E.cls, RDF.type, OWL.Class))  # a class as subject is not a record
    g.add((E.cls, RDF.type, E.Specimen))
    g.add((E.cls, E.tissue, OBO.UBERON_1))
    # terms as subjects
    g.add((OBO.UBERON_1, RDFS.label, Literal("one")))  # ontology structure: left out
    g.add((OBO.UBERON_1, E.note, Literal("annotation")))  # declared annotation: left out
    g.add((OBO.UBERON_1, E.partOfOrgan, OBO.UBERON_2))
    g.add((OBO.UBERON_1, E.partOfOrgan, OBO.UBERON_3))
    g.add((OBO.UBERON_1, E.size, Literal(2)))
    g.add((OBO.UBERON_1, E.size, Literal("2.5", datatype=XSD.decimal)))
    g.add((OBO.UBERON_1, E.page, E.untyped))
    g.add((OBO.UBERON_1, E.ref, E.doc))
    g.add((E.doc, RDF.type, E.Doc))
    g.add((E.doc, RDF.type, E.Paper))
    g.add((OBO.UBERON_1, E.part, BNode()))  # blank-node values: left out
    g.add((OBO.UBERON_2, E.size, Literal(7)))
    return g


@pytest.mark.parametrize("with_typed", [True, False])
def test_term_patterns(tmp_path, with_typed):
    from rdfsolve.mining.ontology_as_data import probe_term_patterns

    graph = _terms_graph()
    miner = SchemaMiner.from_graph(graph, delay=0)
    with miner:
        schema = miner.mine(dataset_name="terms")
    typed = schema.patterns if with_typed else None
    expected = probe_term_patterns(miner._helper, None, miner._collect_bindings, 1000, typed=typed)
    found = term_patterns(store_from_graph(graph, tmp_path / "store"), typed)
    assert len(expected) > 10
    assert _dump(found) == _dump(expected)
    objects = {p.object_class for p in found if p.subject_binding == "term"}
    assert {"Resource", "Literal", str(E.Doc), str(E.Paper), str(OBO.UBERON_2)} <= objects
