"""rdfsolve.reconciliation.engines: a rule bound to its records gives the same result on every
engine (graphs compared as graphs, rows as multisets, "x" and "x"^^xsd:string as equal), and a
rule whose results differ between engines fails with the differences."""

import pytest
from rdflib import Dataset

from rdfsolve.mining.local_graph import LocalGraphHelper
from rdfsolve.reconciliation.engines import RuleDisagreementError, agreed, engine
from rdfsolve.reconciliation.rules import Rule

DATA = (
    '<urn:a> <urn:name> "A" . <urn:b> <urn:name> "B"^^<http://www.w3.org/2001/XMLSchema#string> .'
)
CONVERT = Rule(
    "urn:rule/label",
    "CONSTRUCT { ?record <urn:label> ?name } WHERE { ?record <urn:name> ?name }",
)
CHECK = Rule("urn:rule/names", "SELECT ?record ?name WHERE { ?record <urn:name> ?name }")


def engines(**extra):
    found = {
        backend: engine(
            LocalGraphHelper(
                "urn:local", Dataset().parse(data=DATA, format="turtle"), backend=backend
            )
        )
        for backend in ("oxigraph", "rdflib")
    }
    return {**found, **extra}


def test_a_rule_gives_the_same_result_on_every_engine():
    graph = agreed(CONVERT, ["urn:a", "urn:b"], engines())
    assert len(graph) == 2
    rows = agreed(CHECK, ["urn:b"], engines())
    assert rows == [{"record": "<urn:b>", "name": '"B"^^<http://www.w3.org/2001/XMLSchema#string>'}]


def test_a_rule_whose_results_differ_fails_with_the_differences():
    other = LocalGraphHelper(
        "urn:other", Dataset().parse(data='<urn:a> <urn:name> "Other" .', format="turtle")
    )
    with pytest.raises(RuleDisagreementError, match="urn:rule/names") as failed:
        agreed(CHECK, ["urn:a"], engines(other=engine(other)))
    xsd = "^^<http://www.w3.org/2001/XMLSchema#string>"
    assert failed.value.only == {
        "other": [{"record": "<urn:a>", "name": f'"Other"{xsd}'}],
        "oxigraph": [{"record": "<urn:a>", "name": f'"A"{xsd}'}],
    }, "Rows of one engine and not the first, and rows of the first that it lacks"
