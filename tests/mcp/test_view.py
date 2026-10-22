"""The schema view gives classes, properties, examples and routes as short text."""

from conftest import E, make_schema

from rdfsolve.mcp.view import SchemaView
from rdfsolve.schema_models.class_extensions import ClassExtensions
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.pattern import SchemaPattern


def test_overview_and_class_cards_show_counts_types_examples_and_links():
    view = SchemaView(make_schema())
    overview = view.overview()
    assert 'ex:Pathway "Adverse Outcome Pathway" 2' in overview
    assert "ex: <https://mcp-test.invalid/>" in overview
    card = view.card(str(E.Chemical))
    assert card.splitlines()[0] == "ex:Chemical, instances: 1"
    assert 'ex:cas "CAS number" → literal xsd:string [1]  e.g. "51-52-5"' in card
    assert "ex:Pathway ex:stressor [1]" in card, "Links to the class are listed"
    assert "rdf:type" not in view.card(str(E.Event))
    pathway = view.card(str(E.Pathway))
    assert "ex:stressor → ex:Chemical [1] (< 2 instances)" in pathway, (
        "Some pathways have no stressor"
    )
    assert '    ex:event → ex:Event "Key Event" [2]\n' in pathway, (
        "No note when counts reach the instances"
    )


def test_names_curies_and_iris_give_classes():
    view = SchemaView(make_schema())
    for name in ["Key Event", "key-event", "Event", "ex:Event", f"<{E.Event}>", str(E.Event)]:
        assert view.match_classes(name) == [str(E.Event)], name
    assert view.match_classes("ex:cas") == [], "A property is not a class"
    assert view.expand("rdf:type").endswith("#type"), "Common prefixes are known"
    assert view.curie("https://mcp-test.invalid/a/b") == "<https://mcp-test.invalid/a/b>"
    assert view.similar("ex:Chemicals", view.classes) == ["ex:Chemical"]


def test_search_matches_all_words_of_one_phrase():
    view = SchemaView(make_schema())
    text = view.search(["cas number"])
    assert 'ex:cas "CAS number": ex:Chemical → literal xsd:string [1]' in text
    assert "No class name matches." in text
    assert "ex:Pathway" in view.search(["outcome pathway"])
    assert "No property name matches." in view.search(["nothing here"])


def test_routes_are_shortest_first_and_merge_middle_classes():
    schema = make_schema()
    for subject, prop, obj in [
        (E.Pathway, E.event, E.Gene),
        (E.Pathway, E.event, E.Chemical),
        (E.Chemical, E.gene, E.Gene),
    ]:
        schema.patterns.append(
            SchemaPattern(
                subject_class=str(subject), property_uri=str(prop), object_class=str(obj), count=1
            )
        )
    view = SchemaView(schema)
    texts = [view.route_text(g) for g in view.routes(str(E.Pathway), str(E.Gene), max_hops=2)]
    assert texts == [
        "?source ex:event ?target .  (count 1)",
        "?source ex:event ?n1 . ?n1 ex:gene ?target .  (?n1 a ex:Event or ex:Chemical; counts 2, 1)",
        "?source ex:stressor ?n1 . ?n1 ex:gene ?target .  (?n1 a ex:Chemical; counts 1, 1)",
    ]
    inverse = view.routes(str(E.Gene), str(E.Pathway), max_hops=1)
    assert view.route_text(inverse[0]) == "?target ex:event ?source .  (count 1)"
    assert view.routes(str(E.Pathway), str(E.Pathway), max_hops=1) == [], "No self link"


ORTH, OBO = "http://purl.org/net/orth#", "http://purl.obolibrary.org/obo/"
GENE, CODE, PART = ORTH + "Gene", OBO + "SO_0000704", "urn:x:Anatomy"
WHOLE = OBO + "BFO_0000004"


def row(subject, prop, value, count):
    return SchemaPattern(subject_class=subject, property_uri=prop, object_class=value, count=count)


def view():
    schema = MinedSchema(
        patterns=[
            row(GENE, "urn:x:name", "Literal", 5),
            row(CODE, "urn:x:name", "Literal", 5),
            row(GENE, "urn:x:in", PART, 7),
            row(CODE, "urn:x:in", PART, 7),
            row(PART, "urn:x:name", "Literal", 3),
            row(WHOLE, "urn:x:name", "Literal", 4),
        ]
    )
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


def test_routes_go_through_the_shown_class():
    groups = view().routes(CODE, PART)
    assert len(groups) == 1 and len(groups[0]) == 1, "One route, not one for each name"
    (step,) = groups[0][0]
    assert (step[0], step[1], step[2]) == (GENE, "urn:x:in", PART)
