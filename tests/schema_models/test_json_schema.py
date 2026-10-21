"""rdfsolve.schema_models JSON Schema reader: required and recommended properties, single values,
alternatives and definitions become SHACL that pyshacl applies to data, and unread keywords raise."""

import pytest
from pyshacl import validate
from rdflib import Graph

from rdfsolve.schema_models.readers.json_schema import json_schema_shape
from rdfsolve.schema_models.readers.vocabulary import Vocabulary

EX = "https://example.org/"
SCHEMA = {
    "type": "object",
    "required": ["name"],
    "recommended": ["author"],
    "properties": {
        "name": {"type": "string"},
        "url": {"type": "string", "format": "uri"},
        "published": {"anyOf": [{"type": "string", "format": "date"}]},
        "author": {"anyOf": [{"$ref": "#/definitions/person"}, {"type": "array", "items": {"$ref": "#/definitions/person"}}]},
    },
    "definitions": {"person": {"@type": "Person", "type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
}


RANGES = f"""@prefix schema: <https://schema.org/> . <{EX}name> schema:rangeIncludes schema:Text, schema:URL .
<{EX}published> schema:rangeIncludes schema:Date ."""


def results(data: str, vocabulary: Vocabulary | None = None) -> set[tuple[str, str]]:
    shapes = Graph()
    json_schema_shape(SCHEMA, shape=EX + "Shape", target_class=EX + "Item", context={"@vocab": EX},
                      levels={"required": "Violation", "recommended": "Warning"}, vocabulary=vocabulary).to_rdf(shapes)
    _, report, _ = validate(Graph().parse(data=f"@prefix ex: <{EX}> .\n" + data, format="turtle"), shacl_graph=shapes)
    query = "SELECT ?path ?severity WHERE { ?r a sh:ValidationResult ; sh:resultPath ?path ; sh:resultSeverity ?severity }"
    found = report.query(query, initNs={"sh": "http://www.w3.org/ns/shacl#"})
    return {(str(path).removeprefix(EX), str(severity).rsplit("#", 1)[-1]) for path, severity in found}


def test_json_schema_shape():
    fitting = 'ex:i a ex:Item ; ex:name "I" ; ex:url ex:page ; ex:author ex:a, ex:b . ex:a a ex:Person ; ex:name "A" . ex:b a ex:Person ; ex:name "B" .'
    assert results(fitting) == set()
    misfit = 'ex:i a ex:Item ; ex:name "I", "J" ; ex:url "text" ; ex:published "2020" ; ex:author ex:a . ex:a ex:name "A" .'
    assert results(misfit) == {("name", "Warning"), ("url", "Warning"), ("published", "Warning"), ("author", "Warning")}
    assert results('ex:i a ex:Item .') == {("name", "Violation"), ("author", "Warning")}
    linked = 'ex:i a ex:Item ; ex:name ex:page ; ex:published "2020"^^<http://www.w3.org/2001/XMLSchema#gYear> .'
    assert results(linked) == {("name", "Warning"), ("published", "Warning"), ("author", "Warning")}
    assert results(linked, Vocabulary(Graph().parse(data=RANGES, format="turtle"))) == {("author", "Warning")}
    with pytest.raises(NotImplementedError):
        json_schema_shape({"type": "object", "properties": {"n": {"type": "string", "maxLength": 3}}},
                          shape=EX + "Shape", target_class=EX + "Item", context={"@vocab": EX})
