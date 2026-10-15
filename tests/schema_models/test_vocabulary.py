"""rdfsolve.schema_models vocabularies: a schema is built from vocabularies and their profiles,
with equivalences and hierarchies."""

import json

import pytest
from rdflib import DCTERMS, FOAF, RDFS, SH, XSD, Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection

from rdfsolve.api import Client, RDFList
from rdfsolve.schema_models import MinedSchema

S = Namespace("https://schema.org/")
VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix ex: <urn:ex:> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> . @prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix dcterms: <http://purl.org/dc/terms/> .
schema:Thing a rdfs:Class; rdfs:label "Thing" . schema:Person a rdfs:Class; rdfs:label "Person"; rdfs:subClassOf schema:Thing; rdfs:comment "A person." .
schema:Organization a rdfs:Class; rdfs:label "Organization"; rdfs:subClassOf schema:Thing .
schema:ResearchOrganization a rdfs:Class; rdfs:label "ResearchOrganization"; rdfs:subClassOf schema:Organization .
schema:Text a schema:DataType, rdfs:Class . schema:URL rdfs:subClassOf schema:Text . schema:Date a schema:DataType, rdfs:Class . schema:DateTime a schema:DataType, rdfs:Class .
schema:name rdfs:label "name"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:Text .
schema:sameAs rdfs:label "sameAs"; schema:domainIncludes schema:Thing; schema:rangeIncludes schema:URL .
schema:birthDate rdfs:label "birthDate"; schema:domainIncludes schema:Person; schema:rangeIncludes schema:Date, schema:DateTime .
schema:affiliation rdfs:label "affiliation"; schema:domainIncludes schema:Person; schema:rangeIncludes schema:Organization .
schema:knowsAbout schema:domainIncludes schema:Person; schema:rangeIncludes schema:Thing .
schema:familyName schema:domainIncludes schema:Person; schema:rangeIncludes schema:Text .
schema:Number a schema:DataType, rdfs:Class . schema:Integer rdfs:subClassOf schema:Number . schema:Boolean a schema:DataType .
schema:value schema:domainIncludes schema:Person; schema:rangeIncludes schema:Boolean, schema:Number, schema:Text .
schema:numberOfChildren schema:domainIncludes schema:Person; schema:rangeIncludes schema:Integer .
schema:colleague schema:domainIncludes schema:Person; schema:rangeIncludes schema:Person, rdf:List .
ex:rank rdfs:label "rank"; rdfs:domain schema:Person; rdfs:range xsd:integer .
foaf:Person a rdfs:Class . foaf:name rdfs:domain <http://www.w3.org/2002/07/owl#Thing>; rdfs:range rdfs:Literal .
dcterms:date a rdf:Property; rdfs:range rdfs:Literal; rdfs:label "Birth date" . dcterms:source a rdf:Property; rdfs:label "Source" .
foaf:family_name a owl:DatatypeProperty; rdfs:label "family_name" . foaf:givenName a owl:DatatypeProperty; rdfs:label "Given name" . foaf:givenname a owl:DatatypeProperty; rdfs:comment "Old" .
dcterms:creator a rdf:Property; <http://purl.org/dc/dcam/rangeIncludes> dcterms:Agent . ex:mentioned a rdf:Property .
"""


def test_vocabulary_declarations_become_models_and_rdf(caplog, recwarn, tmp_path):
    head, tail = VOCABULARY.split("schema:familyName")
    (tmp_path / "base.ttl").write_text(head)
    prefixes = "\n".join(line for line in head.splitlines() if line.startswith("@prefix"))
    alias = "@prefix dct: <http://purl.org/dc/terms/> . <urn:ex:page> dct:title 'Page' ."
    parts = [alias, tmp_path / "base.ttl", f"{prefixes}\nschema:familyName{tail}"]
    classes = [S.Person, S.Organization, S.ResearchOrganization]
    schema = MinedSchema.from_vocabulary(VOCABULARY, classes=classes)
    from_parts = MinedSchema.from_vocabulary(parts, classes=classes)
    assert from_parts.patterns == schema.patterns, "Files and text"
    assert from_parts.prefixes["dcterms"] == str(DCTERMS) and "dct" not in from_parts.prefixes, (
        "Owner"
    )
    assert schema.prefixes["foaf"] == str(FOAF) and "owl" not in schema.prefixes, (
        "Declared prefixes in use"
    )
    parents = {S.ResearchOrganization: S.Organization}
    assert schema.class_hierarchy == {str(c): [str(p)] for c, p in parents.items()}, (
        "schema:Thing is the top class: a range of it is any resource, not a class to join"
    )
    assert {p.evidence_source for p in schema.patterns} == {"vocabulary"}, "Declared, not observed"
    person_rows = {p.property_uri for p in schema.patterns if p.subject_class == str(S.Person)}
    assert str(S.name) in person_rows, "Properties of ancestor classes are inherited"

    client = Client(schema, contract=True)  # Records are written without a data source.
    assert Client(schema, contract=True).model("Person") is client.model("Person"), "Built once"
    with pytest.raises(ValueError, match="No data source"):
        client.select("SELECT * WHERE { ?s ?p ?o }")
    org = client.create("ResearchOrganization", uri="urn:org", name="Institute")
    ada = client.create(
        "Person",
        uri="urn:ada",
        name="Ada",
        birthDate="1815-12",
        rank=1,
        affiliation=org,
        knowsAbout=org,
        sameAs=URIRef("https://www.wikidata.org/wiki/Q7259"),
        value="0000-0002",
    )
    graph, ada_iri = ada.to_graph() + org.to_graph(), URIRef("urn:ada")
    assert graph.value(ada_iri, S.birthDate) == Literal("1815-12", datatype=XSD.gYearMonth)
    assert graph.value(ada_iri, S.affiliation) == URIRef("urn:org"), (
        "A subclass fills its parent's range"
    )
    assert graph.value(ada_iri, S.knowsAbout) == URIRef("urn:org"), (
        "Also when the range is not requested"
    )
    assert not [r for r in caplog.records if r.levelname in {"ERROR", "WARNING"}], "Clean coercion"
    assert not [w for w in recwarn if "rdflib" in w.filename], "Values are not parsed to be tried"
    assert graph.value(URIRef("urn:org"), S.name) == Literal("Institute", datatype=XSD.string)
    assert graph.value(ada_iri, URIRef("urn:ex:rank")) == Literal(1, datatype=XSD.integer)
    person = client.model("Person")
    assert "A person." in (person.__doc__ or "") and "name" in person.model_fields, (
        "Documented; plain names"
    )
    research = client.model("ResearchOrganization")
    assert "name" in research.model_fields and "name" not in research.__annotations__, "Inherited"
    assert "mentioned" not in client.model("Person").model_fields, "Mentioned, not described"
    assert "number_of_children" in person.model_fields, "Python names: snake_case of the local name"
    assert {client.field_name(person, n) for n in ("numberOfChildren", "numberofchildren")} == {
        "number_of_children"
    }
    year = Literal("1815-12-10", datatype=XSD.date)
    tagged = client.create(
        "Person",
        uri="urn:en",
        name="Ada",
        foaf_name="Ada",
        foaf_givenName="A",
        familyName="L",
        numberOfChildren=3,
        dcterms_date=year,
        language="en",
    )
    assert {(p, o) for _, p, o in tagged.to_graph()} >= {
        (S.name, Literal("Ada", lang="en")),
        (FOAF.name, Literal("Ada", lang="en")),  # owl:Thing domain; a prefixed field
        (FOAF.givenName, Literal("A", lang="en")),  # A CURIE keeps its case: not foaf:givenname
        (S.familyName, Literal("L", lang="en")),  # The local name, in the vocabulary of the class
        (S.numberOfChildren, Literal(3)),  # Integer, not its parent Number
        (DCTERMS.date, year),  # No domain: any class. rdfs:Literal: any literal
    }
    cited = client.create(
        "Person", uri="urn:c", dcterms_source=URIRef("urn:s"), dcterms_creator="urn:ada"
    )
    assert set(cited.to_graph().objects()) >= {URIRef("urn:s"), URIRef("urn:ada")}, (
        "No range; dcam ranges"
    )
    with pytest.raises(ValueError, match="Ambiguous"):
        client.create("Person", uri="urn:c", dcterms_source="https://example.org/"), "IRI or text?"
    page = "https://example.org/"
    as_link = client.create(
        "Person", uri="urn:k", dcterms_source=page, kinds={"dcterms_source": "iri"}
    )
    as_text = client.create(
        "Person", uri="urn:k", dcterms_source=page, kinds={"dcterms_source": "text"}
    )
    guessed = client.create("Person", uri="urn:k", dcterms_source=page, iri_strings=True)
    assert URIRef(page) in set(as_link.to_graph().objects()), "A reading chosen once per field"
    assert Literal(page) in set(as_text.to_graph().objects())
    assert URIRef(page) in set(guessed.to_graph().objects()), "An http(s) string is a link"
    import pandas as pd

    from rdfsolve.client.authoring import RecordError

    rows = pd.DataFrame(
        {
            "iri": ["urn:p1", "urn:p2", "urn:p3"],
            "children": ["x", "2", "y"],
            "source": [page, page, page],
        }
    )
    with pytest.raises(RecordError) as raised:
        client.from_table(
            "Person", rows, id_column="iri", numberOfChildren="children", dcterms_source="source"
        )
    found = {(p["row"], p["field"]) for p in raised.value.problems}
    assert found == {
        (0, "numberOfChildren"),
        (0, "dcterms_source"),
        (1, "dcterms_source"),
        (2, "numberOfChildren"),
        (2, "dcterms_source"),
    }, "Every problem of every row, not the first one"
    made = client.from_table(
        "Person",
        rows.iloc[[1]],
        id_column="iri",
        numberOfChildren="children",
        dcterms_source="source",
        kinds={"dcterms_source": "iri"},
    )
    assert len(made) == 1
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert not set(shapes.triples((None, SH.datatype, RDFS.Literal))), "Any literal: a node kind"
    team = client.create("Person", uri="urn:t", colleague=RDFList(items=[ada, "urn:b"])).to_graph()
    assert list(Collection(team, team.value(URIRef("urn:t"), S.colleague))) == [
        ada_iri,
        URIRef("urn:b"),
    ]
    linked = client.create("Person", uri="urn:l", sameAs=org).to_graph()
    assert (URIRef("urn:org"), S.name, None) in linked, (
        "A record fills an IRI field with its statements"
    )
    with pytest.raises(
        ValueError, match=r"^affiliation: a Person record cannot be the value\. Use"
    ):
        client.create("Person", uri="urn:w", affiliation=client.create("Person", uri="urn:p"))
    with pytest.raises(ValueError, match=r"name\[1\]: missing value"):
        client.create("Person", uri="urn:m", name=["Ada", None])
    with pytest.raises(ValueError, match="urn:ex:Unknown"):
        MinedSchema.from_vocabulary(VOCABULARY, classes=["urn:ex:Unknown"])


VOCABULARY_PROFILE_S, VOCABULARY_PROFILE_FOAF = "https://schema.org/", "http://xmlns.com/foaf/0.1/"
VOCABULARY_PROFILE_VOCABULARY = """
@prefix schema: <https://schema.org/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix foaf: <http://xmlns.com/foaf/0.1/> .
schema:Thing a rdfs:Class . schema:Text a schema:DataType, rdfs:Class .
schema:Person a rdfs:Class; rdfs:subClassOf schema:Thing; owl:equivalentClass foaf:Person .
schema:name schema:domainIncludes schema:Thing; schema:rangeIncludes schema:Text .
foaf:Person a rdfs:Class . foaf:Document a rdfs:Class .
foaf:familyName a owl:DatatypeProperty; rdfs:domain foaf:Person; rdfs:range rdfs:Literal .
"""


def test_an_equivalent_class_brings_its_properties_without_a_new_axiom():
    schema = MinedSchema.from_vocabulary(
        VOCABULARY_PROFILE_VOCABULARY,
        [VOCABULARY_PROFILE_S + "Person", VOCABULARY_PROFILE_FOAF + "Person"],
    )
    rows = {(p.subject_class, p.property_uri) for p in schema.patterns}
    assert (VOCABULARY_PROFILE_S + "Person", VOCABULARY_PROFILE_FOAF + "familyName") in rows, (
        "schema.org states the equivalence"
    )
    assert (VOCABULARY_PROFILE_FOAF + "Person", VOCABULARY_PROFILE_S + "name") in rows, (
        "Equivalence goes both ways"
    )
    assert VOCABULARY_PROFILE_S + "Person" not in schema.class_hierarchy.get(
        VOCABULARY_PROFILE_FOAF + "Person", []
    ), "Not a subclass"
    client = Client(schema)
    family = client.field_name(
        VOCABULARY_PROFILE_S + "Person", VOCABULARY_PROFILE_FOAF + "familyName"
    )
    record = client.create(
        VOCABULARY_PROFILE_S + "Person", uri="urn:me", name="Me", **{family: "Me"}
    )
    assert {str(p) for p in record.to_graph().predicates()} >= {
        VOCABULARY_PROFILE_S + "name",
        VOCABULARY_PROFILE_FOAF + "familyName",
    }


CODE = (
    VOCABULARY_PROFILE_VOCABULARY
    + """
schema:SoftwareSourceCode a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:SoftwareApplication a rdfs:Class; rdfs:subClassOf schema:Thing .
schema:softwareVersion schema:domainIncludes schema:SoftwareApplication; schema:rangeIncludes schema:Text .
"""
)
PROFILE = """
@prefix schema: <https://schema.org/> . @prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix site: <urn:site:> .
site:SourceCode a sh:NodeShape ; sh:targetClass schema:SoftwareSourceCode ;
    sh:property [ sh:path schema:softwareVersion ; sh:datatype xsd:string ] .
"""


def test_a_profile_adds_rows_that_the_vocabulary_does_not_declare():
    import pytest

    classes = [VOCABULARY_PROFILE_S + "SoftwareSourceCode"]
    schema = MinedSchema.from_vocabulary(CODE, classes, profile=PROFILE)
    row = next(
        p for p in schema.patterns if p.property_uri == VOCABULARY_PROFILE_S + "softwareVersion"
    )
    assert (row.subject_class, row.datatype, row.evidence_source) == (
        VOCABULARY_PROFILE_S + "SoftwareSourceCode",
        "http://www.w3.org/2001/XMLSchema#string",
        "shacl",
    ), "The row says that it comes from the profile, not from schema.org"
    client = Client(schema)
    record = client.create(
        VOCABULARY_PROFILE_S + "SoftwareSourceCode", uri="urn:code", softwareVersion="1.0"
    )
    assert (None, None, None) in record.to_graph()
    with pytest.raises(ValueError, match="not requested"):
        MinedSchema.from_vocabulary(CODE, [VOCABULARY_PROFILE_S + "Person"], profile=PROFILE)


LISTS = """
@prefix schema: <https://schema.org/> . @prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
[] a sh:NodeShape ; sh:targetClass schema:SoftwareSourceCode ; sh:property [
    sh:path schema:contributor ; sh:nodeKind sh:BlankNode ;
    sh:property [ sh:path ( [ sh:zeroOrMorePath rdf:rest ] rdf:first ) ; sh:class schema:Person ] ] .
"""


def test_a_profile_can_declare_ordered_lists():
    from rdfsolve.api import RDFList

    schema = MinedSchema.from_vocabulary(
        CODE,
        [VOCABULARY_PROFILE_S + "SoftwareSourceCode", VOCABULARY_PROFILE_S + "Person"],
        profile=LISTS,
    )
    client = Client(schema)
    people = [
        client.create(VOCABULARY_PROFILE_S + "Person", uri=f"urn:p{i}", name=f"P{i}")
        for i in (1, 2)
    ]
    code = client.create(
        VOCABULARY_PROFILE_S + "SoftwareSourceCode",
        uri="urn:code",
        contributor=RDFList(items=people),
    )
    graph = code.to_graph()
    head = next(o for _, p, o in graph if str(p) == VOCABULARY_PROFILE_S + "contributor")
    assert [str(m) for m in graph.items(head)] == ["urn:p1", "urn:p2"], "In their order"


RULES = """
@prefix schema: <https://schema.org/> . @prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix site: <urn:site:> .
@prefix dash: <http://datashapes.org/dash#> .
site:main a sh:PropertyGroup ; sh:order 1 .
site:SourceCode a sh:NodeShape ; sh:targetClass schema:SoftwareSourceCode ;
    sh:property [ sh:path schema:softwareVersion ; sh:datatype xsd:string ; sh:minCount 1 ;
        sh:maxCount 1 ; sh:order 3 ; sh:group site:main ; sh:name "Version" ;
        sh:message "A version is required" ; dash:singleLine true ] ;
    sh:property [ sh:path schema:applicationCategory ; sh:hasValue "research" ] .
"""


def test_a_profile_keeps_its_rules_and_form_annotations_through_json_and_shacl(tmp_path):
    """minCount, hasValue, order, group, messages and DASH annotations of a profile survive
    from_vocabulary -> to_dict -> from_json -> to_shacl, merged with the generated shapes."""
    from rdflib import Namespace

    sh = Namespace("http://www.w3.org/ns/shacl#")
    dash = Namespace("http://datashapes.org/dash#")
    schema = MinedSchema.from_vocabulary(
        CODE, [VOCABULARY_PROFILE_S + "SoftwareSourceCode"], profile=RULES
    )
    path = tmp_path / "s_schema.json"
    path.write_text(json.dumps(schema.to_dict()), encoding="utf-8")
    shapes = Graph().parse(data=MinedSchema.from_json(path).to_shacl(), format="turtle")
    # The profile's shape states a message: it covers that shape's constraints only, so the shape
    # stays apart from the generated one, which takes the annotations.
    on_path = set(shapes.subjects(sh.path, URIRef(VOCABULARY_PROFILE_S + "softwareVersion")))
    (version,) = {s for s in on_path if shapes.value(s, sh.message) is not None}
    (described,) = on_path - {version}
    found = {p: shapes.value(version, p) for p in (sh.minCount, sh.maxCount)}
    assert {str(p).rsplit("#")[-1]: str(v) for p, v in found.items()} == {
        "minCount": "1",
        "maxCount": "1",
    }
    assert str(shapes.value(version, sh.message)) == "A version is required"
    assert str(shapes.value(described, sh.order)) == "3"
    assert str(shapes.value(described, sh.group)) == "urn:site:main"
    assert shapes.value(described, dash.singleLine) is not None
    assert shapes.value(described, sh.message) is None, "The generated shape keeps no message"
    assert len(list(shapes.objects(described, sh.name))) == 1, "The profile's name replaces"
    owners = {o for s in on_path for o in shapes.subjects(sh.property, s)}
    assert len(owners) == 1 and shapes.value(next(iter(owners)), sh.targetClass) is not None
    category = next(
        s for s in shapes.subjects(sh.path, URIRef(VOCABULARY_PROFILE_S + "applicationCategory"))
    )
    assert str(shapes.value(category, sh.hasValue)) == "research"
    assert (URIRef("urn:site:main"), sh.order, None) in shapes, "The property group is kept"
    targets = list(
        shapes.subjects(sh.targetClass, URIRef(VOCABULARY_PROFILE_S + "SoftwareSourceCode"))
    )
    assert len(targets) == 1, "One node shape for the class, not the profile's beside it"


def test_schema_thing_is_a_top_class_not_a_class_check():
    """A range of schema:Thing allows any IRI or blank node: no sh:class schema:Thing check, which
    untyped or external links would fail. Other vocabularies give their top class."""
    schema = MinedSchema.from_vocabulary(VOCABULARY, classes=[S.Person])
    knows = {p.object_class for p in schema.patterns if p.property_uri == str(S.knowsAbout)}
    assert knows == {"Resource", "BlankNode"}
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    assert (
        not list(shapes.subject_objects(SH["class"])) or (None, SH["class"], S.Thing) not in shapes
    )
    kept = MinedSchema.from_vocabulary(
        VOCABULARY, classes=[S.Person], top_classes=["http://www.w3.org/2002/07/owl#Thing"]
    )
    assert {p.object_class for p in kept.patterns if p.property_uri == str(S.knowsAbout)} == {
        str(S.Thing)
    }, "A vocabulary's top classes are a parameter"


def test_the_same_inputs_give_the_same_schema():
    """No random identifier and no times of the run, unless asked: a build can compare the file."""
    first = MinedSchema.from_vocabulary(VOCABULARY, classes=[S.Person])
    second = MinedSchema.from_vocabulary(VOCABULARY, classes=[S.Person])
    assert json.dumps(first.to_dict(), sort_keys=True) == json.dumps(
        second.to_dict(), sort_keys=True
    )
    assert first.about.schema_id and not first.about.generated_at
    timed = MinedSchema.from_vocabulary(VOCABULARY, classes=[S.Person], timestamps=True)
    assert timed.about.generated_at and timed.about.schema_id == first.about.schema_id
