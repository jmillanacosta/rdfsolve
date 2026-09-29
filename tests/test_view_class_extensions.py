"""The schema view shows classes with the same members as one class, named by a readable name
before an ontology code, with the nearest classes that hold all its instances (Bgee: orth:Gene
for orth:Gene, orth:SequenceUnit, orth:GeneTreeNode, SO_0000704 and CDAO_0000140)."""

from rdfsolve.mcp.view import SchemaView
from rdfsolve.schema_models.class_extensions import ClassExtensions
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern

ORTH, OBO = "http://purl.org/net/orth#", "http://purl.obolibrary.org/obo/"
GENE, CODE, PART = ORTH + "Gene", OBO + "SO_0000704", "urn:x:Anatomy"
WHOLE = OBO + "BFO_0000004"


def row(subject, prop, value, count):
    return SchemaPattern(subject_class=subject, property_uri=prop, object_class=value, count=count)


def view():
    schema = MinedSchema(patterns=[
        row(GENE, "urn:x:name", "Literal", 5), row(CODE, "urn:x:name", "Literal", 5),
        row(GENE, "urn:x:in", PART, 7), row(CODE, "urn:x:in", PART, 7),
        row(PART, "urn:x:name", "Literal", 3), row(WHOLE, "urn:x:name", "Literal", 4),
    ])
    schema.about.class_entity_counts = {GENE: 5, CODE: 5, PART: 3, WHOLE: 4}
    schema.class_extensions = ClassExtensions(
        members={GENE: 5, CODE: 5, PART: 3, WHOLE: 4},
        same_members=[sorted([GENE, CODE])],
        contained_in={PART: [WHOLE]},
    )
    return SchemaView(schema)


def test_classes_with_the_same_members_are_shown_once():
    text = view().overview()
    assert "orth:Gene 5 (same members: so:0000704)" in text
    assert "\n  so:0000704" not in text and "3 classes (4 names)" in text
    card = view().card(CODE)
    assert card.splitlines()[0].startswith("orth:Gene, instances: 5")
    assert "Same members: so:0000704" in card and "urn:x:name" in card
    anatomy = view().card(PART)
    assert "All instances are also instances of: bfo:0000004" in anatomy
    assert "Classes whose instances are all instances of it: <urn:x:Anatomy>" in view().card(WHOLE)
