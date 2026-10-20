"""rdfsolve.plan between two versions of a SHACL profile: kinds found by class and namespace,
fields carried by property and label, removed fields reported, and the records written again."""

import logging

import pytest
import rdflib

PREFIXES = """@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix schema: <https://schema.org/> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix form: <https://example.org/form#> .
@prefix draft: <https://example.org/draft/> .
@prefix released: <https://example.org/released/> .
"""
SHAPES = """
form:Tool a sh:NodeShape ; sh:targetClass schema:SoftwareApplication ; rdfs:label "Tool" ;
    sh:property [ sh:path schema:name ; sh:name "Name" ; sh:maxCount 1 ] ,
        [ sh:path schema:softwareVersion ; sh:name "Version" ; sh:maxCount 1 ] ,
        [ sh:path schema:maintainer ; sh:name "Contact" ; sh:class schema:Person ] ,
        [ sh:path schema:identifier ; sh:name "Identifier" ; sh:class schema:PropertyValue ;
          sh:nodeKind sh:BlankNode ] .
form:Method a sh:NodeShape ; sh:targetClass draft:LabProtocol ; rdfs:label "Method" ;
    sh:property [ sh:path schema:name ; sh:name "Name" ] .
form:Person a sh:NodeShape ; sh:targetClass schema:Person ; rdfs:label "Person" ;
    sh:property [ sh:path schema:name ; sh:name "Name" ] , [ sh:path schema:email ; sh:name "Email" ] .
form:Identifier a sh:NodeShape ; sh:targetClass schema:PropertyValue ; rdfs:label "Identifier" ;
    sh:property [ sh:path schema:propertyID ; sh:name "Kind" ] ,
        [ sh:path schema:value ; sh:name "Value" ] .
form:Catalog a sh:NodeShape ; sh:targetClass schema:DataCatalog ; rdfs:label "Catalogue" ;
    sh:property [ sh:path void:classPartition ; sh:name "Kinds" ; sh:node form:Partition ;
        sh:nodeKind sh:BlankNode ] .
form:Partition a sh:NodeShape ;
    sh:property [ sh:path void:class ; sh:name "Class" ] , [ sh:path schema:name ; sh:name "Name" ] .
"""
V1_PROFILE = PREFIXES + SHAPES
# The next version: a class and a property renamed, a field removed, a field added, and each
# kind's namespace declared.
V2_PROFILE = (
    PREFIXES
    + SHAPES.replace("draft:LabProtocol", "released:LabProtocol")
    .replace("schema:softwareVersion", "schema:version")
    .replace(' , [ sh:path schema:email ; sh:name "Email" ]', "")
    .replace(
        'sh:property [ sh:path schema:name ; sh:name "Name" ; sh:maxCount 1 ] ,',
        'sh:pattern "^https://example\\\\.org/tools/" ;\n'
        '    sh:property [ sh:path schema:name ; sh:name "Name" ; sh:maxCount 1 ] ,\n'
        '        [ sh:path schema:applicationCategory ; sh:name "Category" ] ,',
    )
    .replace(
        'rdfs:label "Method" ;',
        'rdfs:label "Method" ; sh:pattern "^https://example\\\\.org/methods/" ;',
    )
    .replace(
        'rdfs:label "Person" ;',
        'rdfs:label "Person" ; sh:pattern "^https://example\\\\.org/people/" ;',
    )
    + """
<https://example.org/profile> sh:declare
    [ sh:prefix "ex_tools" ; sh:namespace "https://example.org/tools/"^^xsd:anyURI ] ,
    [ sh:prefix "ex_methods" ; sh:namespace "https://example.org/methods/"^^xsd:anyURI ] ,
    [ sh:prefix "ex_people" ; sh:namespace "https://example.org/people/"^^xsd:anyURI ] .
"""
)
V1_DATA = (
    PREFIXES
    + """
<https://example.org/tools/one> a schema:SoftwareApplication ; schema:name "Tool one" ;
    schema:softwareVersion "1.0" ; schema:maintainer <https://example.org/people/editor> ;
    schema:identifier [ a schema:PropertyValue ; schema:propertyID "doi" ;
        schema:value "10.1234/one" ] .
<https://example.org/tools/two> a schema:SoftwareApplication ; schema:name "Tool two" ;
    schema:maintainer <https://example.org/people/reviewer> .
<https://example.org/methods/assay> a draft:LabProtocol ; schema:name "An assay" .
<https://example.org/people/editor> a schema:Person ; schema:name "Test Editor" ;
    schema:email "editor@example.org" .
<https://example.org/people/reviewer> a schema:Person ; schema:name "Test Reviewer" ;
    schema:email "reviewer@example.org" .
<https://example.org/platform/> a schema:DataCatalog ;
    void:classPartition [ void:class schema:SoftwareApplication ; schema:name "Tools" ] .
"""
)
V2_DATA = (
    V1_DATA.replace("a draft:LabProtocol", "a released:LabProtocol")
    .replace("schema:softwareVersion", "schema:version")
    .replace(' ;\n    schema:email "editor@example.org"', "")
    .replace(' ;\n    schema:email "reviewer@example.org"', "")
)


@pytest.fixture(scope="module")
def client():
    """A client of the first version's records, named by the first version's profile."""
    from rdfsolve import SchemaMiner
    from rdfsolve.client.api import Client

    logging.getLogger("rdfsolve").setLevel(logging.WARNING)
    graph = rdflib.Graph().parse(data=V1_DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        schema = miner.mine(dataset_name="v1")
    schema.shape_profile = V1_PROFILE
    return Client(schema, graph, graph_uris=[])


@pytest.fixture(scope="module")
def target():
    """The second version's profile as a target."""
    from rdfsolve.plan import Target

    return Target(rdflib.Graph().parse(data=V2_PROFILE, format="turtle"))


@pytest.fixture(scope="module")
def plan(client, target):
    """A plan of every record of the first version into the second version's profile."""
    from rdfsolve.client.api import Results
    from rdfsolve.plan import Plan

    graph = client._local_graph()
    records = []
    for kind in client.kinds:
        members = []
        for node in graph.subjects(rdflib.RDF.type, rdflib.URIRef(kind.iri)):
            members.append(f"_:{node}" if isinstance(node, rdflib.BNode) else str(node))
        records += list(client.from_table(kind.iri, sorted(members)).records)
    return Plan(Results(client, records), into=target)


def test_every_kind_is_decided_by_its_class_or_its_records_namespace(plan):
    assert not plan.open
    assert plan.LabProtocol.target.iri == "https://example.org/released/LabProtocol"
    assert plan.LabProtocol.chosen.why.startswith("ids: ex_methods 1")
    assert plan.PropertyValue.chosen.why == "the same class"
    assert plan.SoftwareApplication.target.name == "Tool"


def test_a_link_of_a_property_the_target_has_is_proposed_that_property(client, target):
    from rdfsolve.plan import Plan

    tools = client.from_table("SoftwareApplication", ["https://example.org/tools/one"])
    plan = Plan(tools, into=target, contents=client.kinds.SoftwareApplication.maintainer)
    assert plan.maintainer.status == "proposed"
    assert plan.maintainer.target.iri == "https://schema.org/maintainer"
    assert plan.maintainer.chosen.why == "the same property"
