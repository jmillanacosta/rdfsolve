"""Values of an answer are resolved to the resources they show, with the field of the view."""

from conftest import E, text, uri
from rdflib import DC, FOAF, RDFS
from rdfsolve.evaluation.views import View, resolvable, resolve, term_key

XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"


def test_keys_make_plain_and_xsd_string_literals_equal():
    assert term_key(text("a")) == term_key(text("a", datatype=XSD_STRING))
    assert term_key({"type": "typed-literal", "value": "5", "datatype": "urn:t"})[0] == "literal"
    assert term_key(text("a", **{"xml:lang": "EN"}))[2] == "en"
    assert not resolvable(text("12", datatype="http://www.w3.org/2001/XMLSchema#integer"))
    assert not resolvable(text("x" * 301)), "Long texts are values, not names"


def test_names_identifiers_and_pages_resolve_to_their_resource(select):
    views = resolve(
        [uri(E.ke1), text("KE 1"), text("Event title 1", datatype=XSD_STRING), uri(E.page1),
         text("Shared text"), text("11040"), text("5", datatype="http://www.w3.org/2001/XMLSchema#integer")],
        select,
        batch=2,
    )
    assert views[term_key(uri(E.ke1))] == {View(str(E.ke1), "self"), View(str(E.ke1), str(DC.identifier))}
    assert views[term_key(text("KE 1"))] == {View(str(E.ke1), str(RDFS.label))}
    assert {v.kind for v in views[term_key(text("Event title 1"))]} == {"name"}
    assert views[term_key(uri(E.page1))] >= {View(str(E.ke1), str(FOAF.page))}
    assert term_key(text("Shared text")) not in views, "A description is not a view"
    assert term_key(text("11040")) not in views, "A field that is not a view field"
    assert len(select.queries) == 3, "Batches of two values"
