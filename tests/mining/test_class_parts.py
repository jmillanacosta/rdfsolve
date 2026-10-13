"""rdfsolve.mining.class_parts and rdfsolve.schema_models.class_parts: the members of a class
counted by how they are identified, published as SHACL node shapes without targets and as VoID
subsets of the class partition."""

import pyshacl
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF

from rdfsolve.mining.class_parts import class_parts
from rdfsolve.mining.scan import store_from_graph
from rdfsolve.schema_models.class_parts import add_void_parts, part_shapes

SH = Namespace("http://www.w3.org/ns/shacl#")
VOID = Namespace("http://rdfs.org/ns/void#")
DATA = """
@prefix ex: <http://example.org/> .
<https://identifiers.org/ncbigene/7157> a ex:Node .
<https://identifiers.org/ncbigene/672> a ex:Node .
<https://identifiers.org/uniprot/P04637> a ex:Node .
<https://identifiers.org/uniprot/043174> a ex:Node .
<http://data.example.org/node/1> a ex:Node .
<http://data.example.org/node/2> a ex:Node .
<http://purl.obolibrary.org/obo/MI_MI:0914> a ex:Node .
_:b1 a ex:Node .
"""


def parts_of(tmp_path):
    store = store_from_graph(Graph().parse(data=DATA, format="turtle"), tmp_path / "store")
    return class_parts(store.members())


def test_members_counted_by_how_they_are_identified(tmp_path):
    parts = {
        (p.kind, p.prefix or (p.namespaces[0] if p.namespaces else None)): p.entities
        for p in parts_of(tmp_path).by_class["http://example.org/Node"]
    }
    assert parts == {
        ("identifier", "ncbigene"): 2,
        ("identifier", "uniprot"): 1,
        ("invalid_identifier", "uniprot"): 1,
        ("namespace", "http://data.example.org/node/"): 2,
        ("identifier", "mi"): 1,
        ("blank_node", None): 1,
    }, "A local identifier that is not valid for its prefix is a part of its own"


def test_part_shapes_accept_exactly_the_counted_members(tmp_path):
    found = parts_of(tmp_path).by_class["http://example.org/Node"]
    shapes = part_shapes("http://example.org/shapes/Node", "Node", found)
    graph = Graph()
    for shape in shapes:
        shape.to_rdf(graph)
    data = Graph().parse(data=DATA, format="turtle")

    def members_of(shape):
        """Return the members of the class that conform to one part shape."""
        conforming = set()
        for node in data.subjects(RDF.type, URIRef("http://example.org/Node")):
            focus = Graph()
            focus.add((node, RDF.type, URIRef("http://example.org/Node")))
            probe = Graph()
            for triple in graph:
                probe.add(triple)
            probe.add((URIRef(shape.uri), SH.targetNode, node))
            if pyshacl.validate(focus, shacl_graph=probe)[0]:
                conforming.add(str(node))
        return conforming

    by_name = {shape.name: members_of(shape) for shape in shapes}
    assert by_name["Node identified in uniprot"] == {"https://identifiers.org/uniprot/P04637"}
    assert by_name[
        "Node with an IRI of uniprot that is no valid uniprot identifier"
    ] == {"https://identifiers.org/uniprot/043174"}
    assert by_name["Node identified in mi"] == {"http://purl.obolibrary.org/obo/MI_MI:0914"}, (
        "A local identifier written with its prefix is read"
    )
    iris = [m for shape in shapes if shape.node_kind != "BlankNode" for m in by_name[shape.name]]
    assert sorted(iris) == sorted({*iris}) and len(iris) == 7, (
        "Each member with an IRI conforms to exactly one part"
    )


def test_void_subsets_state_the_entities_of_each_part(tmp_path):
    found = parts_of(tmp_path).by_class["http://example.org/Node"]
    graph = Graph()
    partition = URIRef("http://example.org/partition/class-node")
    add_void_parts(graph, partition, "http://example.org/shapes/Node", found)
    subsets = list(graph.objects(partition, VOID.subset))
    assert len(subsets) == len(found)
    assert sorted(int(graph.value(s, VOID.entities)) for s in subsets) == sorted(
        p.entities for p in found
    )
    assert (
        None,
        VOID.uriSpace,
        Literal("https://identifiers.org/ncbigene/"),
    ) in graph


def test_a_prefix_that_is_no_prefix_name_is_not_declared():
    from rdfsolve.schema_models.exporters.shacl import _PREFIX_NAME

    assert _PREFIX_NAME.match("ncbigene") and _PREFIX_NAME.match("kegg.compound")
    assert not _PREFIX_NAME.match("_3dmet"), "A SPARQL prefix name starts with a letter"
    assert not _PREFIX_NAME.match("3dmet") and not _PREFIX_NAME.match("obo.")
