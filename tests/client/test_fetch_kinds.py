"""fetch without kind=: identifiers of several kinds are grouped by prefix, each group fetched
with the class whose example subjects carry that prefix in the schema."""

import pytest
from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm

AOP = "http://aopkb.org/aop_ontology#"
TITLE = "http://purl.org/dc/elements/1.1/title"
DATA = """
@prefix aop: <http://aopkb.org/aop_ontology#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
<https://identifiers.org/aop/464> a aop:AdverseOutcomePathway ; dc:title "An AOP" .
<https://identifiers.org/aop.events/177> a aop:KeyEvent ; dc:title "An event" .
"""


@pytest.fixture
def source():
    data = Dataset().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, delay=0) as miner:
        schema = miner.mine("aopwikirdf-small")
    # The example subjects of an enriched run: they tie each prefix to its class.
    examples = [
        PatternExample(
            subject_class=AOP + cls,
            property_uri=TITLE,
            subject=RdfTerm(kind="uri", value=iri),
            value=RdfTerm(kind="literal", value=title),
        )
        for cls, iri, title in (
            ("AdverseOutcomePathway", "https://identifiers.org/aop/464", "An AOP"),
            ("KeyEvent", "https://identifiers.org/aop.events/177", "An event"),
        )
    ]
    schema.enrichment.examples = [*schema.enrichment.examples, *examples]
    with Client(schema, data) as client:
        yield client


def test_identifiers_of_several_kinds_are_fetched_without_kind(source):
    found = source.fetch(["aop:464", "aop.events:177"])
    assert sorted(str(vars(r)["uri"]) for r in found) == [
        "https://identifiers.org/aop.events/177",
        "https://identifiers.org/aop/464",
    ]


def test_a_prefix_without_a_class_still_needs_kind(source):
    with pytest.raises(ValueError, match="Give kind=.*chebi"):
        source.fetch(["aop:464", "chebi:15377"])

