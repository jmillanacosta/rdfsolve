"""The schema view gives classes, properties, examples and routes as short text."""

from conftest import E, make_schema
from rdfsolve.mcp.view import SchemaView
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
    for subject, prop, obj in [(E.Pathway, E.event, E.Gene), (E.Pathway, E.event, E.Chemical), (E.Chemical, E.gene, E.Gene)]:
        schema.patterns.append(
            SchemaPattern(subject_class=str(subject), property_uri=str(prop), object_class=str(obj), count=1)
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
