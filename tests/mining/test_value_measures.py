"""rdfsolve.mining.value_measures: the members with each property, the most values of one, and
the namespaces of IRI values without a type, measured in the data and written as SHACL."""

from __future__ import annotations

import pytest
from rdflib import SH, Graph, Literal, URIRef

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.value_measures import classify_iris

pyshacl = pytest.importorskip("pyshacl")

DATA = """
@prefix e: <http://example.org/vocabulary/> .
<http://example.org/a/1> a e:A ; e:link <http://example.org/t/1> ; e:tag "x", "y", "z" .
<http://example.org/a/2> a e:A ; e:link <https://identifiers.org/ncbigene/1017> ; e:tag "x" .
<http://example.org/a/3> a e:A ; e:link <https://identifiers.org/hgnc/5> ; e:tag "y" ;
    e:page <https://web.example.org/pages/a3> .
<http://example.org/t/1> a e:T .
"""
E = "http://example.org/vocabulary/"


def _schema():
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        return miner.mine(dataset_name="fixture")


def _severities(data: Graph, shapes: Graph) -> dict[str, set[str]]:
    _, report, _ = pyshacl.validate(data, shacl_graph=shapes)
    found: dict[str, set[str]] = {}
    for result in report.subjects(SH.resultSeverity, None):
        component = str(report.value(result, SH.sourceConstraintComponent)).rsplit("#", 1)[-1]
        severity = str(report.value(result, SH.resultSeverity)).rsplit("#", 1)[-1]
        found.setdefault(component, set()).add(severity)
    return found


def test_values_are_measured_per_class_and_property():
    schema = _schema()
    counts = {(c.subject_class, c.property_uri): (c.subjects, c.most) for c in schema.value_counts}
    assert counts[(E + "A", E + "link")] == (3, 1), "one link each, of two kinds"
    assert counts[(E + "A", E + "tag")] == (3, 3)
    (parts,) = [p for p in schema.value_parts if p.property_uri == E + "link"]
    assert parts.values == 2
    assert {(p.kind, p.prefix) for p in parts.parts} == {
        ("identifier", "ncbigene"),
        ("identifier", "hgnc"),
    }


def test_the_measures_give_required_single_values_caps_and_namespaces():
    shapes = Graph().parse(data=_schema().to_shacl(void=False), format="turtle")
    data = Graph().parse(data=DATA, format="turtle")
    assert _severities(data, shapes) == {}, "the data conforms"
    second = Graph().parse(data=DATA, format="turtle")
    second.add(
        (URIRef("http://example.org/a/2"), URIRef(E + "link"), URIRef("http://example.org/t/1"))
    )
    assert _severities(second, shapes) == {"MaxCountConstraintComponent": {"Violation"}}
    elsewhere = Graph().parse(data=DATA, format="turtle")
    elsewhere.remove((URIRef("http://example.org/a/3"), URIRef(E + "link"), None))
    elsewhere.add(
        (
            URIRef("http://example.org/a/3"),
            URIRef(E + "link"),
            URIRef("http://third.example.org/x/1"),
        )
    )
    assert _severities(elsewhere, shapes) == {"OrConstraintComponent": {"Violation"}}
    more = Graph().parse(data=DATA, format="turtle")
    more.add((URIRef("http://example.org/a/2"), URIRef(E + "tag"), Literal("w")))
    assert _severities(more, shapes) == {}, "below the most of the data"
    many = Graph().parse(data=DATA, format="turtle")
    for value in "pqrs":
        many.add((URIRef("http://example.org/a/1"), URIRef(E + "tag"), Literal(value)))
    assert _severities(many, shapes) == {"MaxCountConstraintComponent": {"Info"}}
    missing = Graph().parse(data=DATA, format="turtle")
    missing.remove((URIRef("http://example.org/a/2"), URIRef(E + "tag"), None))
    assert _severities(missing, shapes) == {"MinCountConstraintComponent": {"Violation"}}


def test_values_can_be_left_unmeasured():
    graph = Graph().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(graph, delay=0, value_measures=False) as miner:
        schema = miner.mine(dataset_name="fixture")
    assert schema.value_counts is None and schema.value_parts is None


def test_iris_without_a_scheme_are_a_part_of_their_own():
    parts = {p.kind: p for p in classify_iris(["177_event", "https://identifiers.org/ncbigene/1"])}
    assert set(parts) == {"relative_iri", "identifier"}


def test_web_addresses_of_a_new_host_are_reported_at_info():
    """Values in namespaces of no registry (web addresses) may be of any host: Info."""
    shapes = Graph().parse(data=_schema().to_shacl(void=False), format="turtle")
    moved = Graph().parse(data=DATA, format="turtle")
    page = URIRef(E + "page")
    moved.remove((URIRef("http://example.org/a/3"), page, None))
    moved.add((URIRef("http://example.org/a/3"), page, URIRef("https://another.example.net/x")))
    assert _severities(moved, shapes) == {"PatternConstraintComponent": {"Info"}}
