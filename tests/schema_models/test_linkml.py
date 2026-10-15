"""rdfsolve.schema_models.exporters.linkml: a LinkML schema of a mined schema that keeps the
profile's rules, names terms from their CURIEs, maps datatypes to LinkML types, and passes the
LinkML linter and generators."""

from __future__ import annotations

from rdflib import RDF, XSD

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

S = "https://schema.org/"
DCT = "http://purl.org/dc/terms/"
PROFILE = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix dash: <http://datashapes.org/dash#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix schema: <https://schema.org/> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ex: <https://example.org/shapes/> .
ex:main rdfs:label "Main" .
ex:Tool a sh:NodeShape ; sh:targetClass schema:SoftwareApplication ;
    sh:property [ sh:path schema:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:order 1 ;
                  sh:group ex:main ; sh:name "Name" ; dash:singleLine true ] ,
        [ sh:path schema:url ; sh:minCount 1 ; sh:severity sh:Warning ;
          sh:message "Add a website." ; sh:nodeKind sh:IRI ] ,
        [ sh:path schema:creativeWorkStatus ; sh:maxCount 1 ; sh:in ( "TRL 1" "TRL 2" ) ] ,
        [ sh:path dct:conformsTo ; sh:hasValue <https://bioschemas.org/profiles/ComputationalTool/1.0-RELEASE> ] ,
        [ sh:path schema:citation ; sh:pattern "^https://doi.org/" ] ,
        [ sh:path schema:provider ; sh:class schema:Organization ] ,
        [ sh:path schema:about ; sh:or ( [ sh:class schema:DefinedTerm ] [ sh:class schema:Question ] ) ] .
"""


def _schema() -> MinedSchema:
    tool = S + "SoftwareApplication"
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="platform"),
        prefixes={"schema": S, "dct": DCT},
        patterns=[
            SchemaPattern(
                subject_class=tool,
                property_uri=S + "name",
                object_class="Literal",
                datatype=str(RDF.langString),
            ),
            SchemaPattern(
                subject_class=tool,
                property_uri=S + "dateCreated",
                object_class="Literal",
                datatype=str(XSD.date),
            ),
            SchemaPattern(
                subject_class=tool,
                property_uri=S + "version",
                object_class="Literal",
                datatype=str(XSD.gYear),
            ),
            SchemaPattern(subject_class=tool, property_uri=S + "about", object_class="Resource"),
        ],
        shape_profile=PROFILE,
    )


def test_the_profile_rules_become_linkml():
    from rdfsolve.schema_models.exporters.linkml import to_linkml

    linkml = to_linkml(_schema())
    tool = linkml.classes["SchemaSoftwareApplication"]
    from linkml_runtime.utils.schemaview import SchemaView

    assert SchemaView(linkml).expand_curie(tool.class_uri) == S + "SoftwareApplication"
    assert ":" not in tool.class_uri.split(":", 1)[1], "LinkML splits a CURIE at its colon"
    usage = tool.slot_usage
    name = usage["schema_name"]
    assert name.required and name.multivalued is False and name.rank == 1 and name.title == "Name"
    assert name.slot_group == "ex_main_group" and linkml.slots["ex_main_group"].is_grouping_slot
    assert "dash_singleLine" in name.annotations
    url = usage["schema_url"]
    assert url.recommended and not url.required and url.range == "uriorcurie"
    assert url.annotations["sh_message"].value == "Add a website."
    status = linkml.enums[usage["schema_creative_work_status"].range]
    assert set(status.permissible_values) == {"TRL 1", "TRL 2"}
    conforms = linkml.enums[usage["dct_conforms_to"].range]
    (value,) = conforms.permissible_values.values()
    assert (
        SchemaView(linkml).expand_curie(value.meaning)
        == "https://bioschemas.org/profiles/ComputationalTool/1.0-RELEASE"
    )
    assert usage["schema_citation"].pattern == "^https://doi.org/"
    assert usage["schema_provider"].range == "SchemaOrganization", "a link keeps its class"
    assert {a.range for a in usage["schema_about"].any_of} == {
        "SchemaDefinedTerm",
        "SchemaQuestion",
    }
    assert usage["schema_date_created"].range == "date"
    assert usage["schema_name"].range != "string", "a language-tagged text keeps its type"
    assert "schema" not in linkml.prefixes, "linkml:types gives schema: another namespace"


def test_names_do_not_depend_on_the_other_terms():
    from rdfsolve.schema_models.exporters.linkml import to_linkml

    small = _schema()
    small.patterns = small.patterns[:1]
    assert "schema_name" in to_linkml(small).slots and "schema_name" in to_linkml(_schema()).slots


def test_the_schema_passes_the_linter_and_the_generators(tmp_path):
    from linkml.generators.jsonschemagen import JsonSchemaGenerator
    from linkml.generators.owlgen import OwlSchemaGenerator
    from linkml.generators.pydanticgen import PydanticGenerator
    from linkml.generators.pythongen import PythonGenerator
    from linkml.generators.shaclgen import ShaclGenerator
    from linkml.linter.linter import Linter

    path = tmp_path / "platform.yaml"
    path.write_text(_schema().to_linkml_yaml())
    errors = [
        p.message
        for p in Linter({"extends": "recommended"}).lint(str(path), validate_schema=True)
        if str(p.level) == "error"
    ]
    assert errors == []
    for generator in (
        PythonGenerator,
        PydanticGenerator,
        ShaclGenerator,
        JsonSchemaGenerator,
        OwlSchemaGenerator,
    ):
        assert generator(str(path)).serialize(), generator.__name__
