"""rdfsolve.mining.wikibase_strategy: a Wikibase endpoint is mined from its property declarations
and bounded statement windows, with classes from instance-of and IRI prefixes and names from
properties."""

from rdflib import Dataset

from rdfsolve.mining import wikibase_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.wikibase_strategy import WikibaseScopeStrategy, WikibaseStrategy
from rdfsolve.schema_models.exporters.pydantic import build_pydantic_classes
from rdfsolve.sparql_helper import EndpointRateLimitError

WD, WDT = "http://www.wikidata.org/entity/", "http://www.wikidata.org/prop/direct/"
DATA = """
@prefix wd: <http://www.wikidata.org/entity/> . @prefix wdt: <http://www.wikidata.org/prop/direct/> .
@prefix wikibase: <http://wikiba.se/ontology#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
wd:P31 a wikibase:Property; wikibase:propertyType wikibase:WikibaseItem; wikibase:directClaim wdt:P31;
    rdfs:label "instance of"@en .
wd:P50 a wikibase:Property; wikibase:propertyType wikibase:WikibaseItem; wikibase:directClaim wdt:P50;
    rdfs:label "author"@en, "auteur"@fr .
wd:P496 a wikibase:Property; wikibase:propertyType wikibase:ExternalId; wikibase:directClaim wdt:P496;
    rdfs:label "ORCID iD"@en .
wd:P6 a wikibase:Property; wikibase:propertyType wikibase:WikibaseItem; wikibase:directClaim wdt:P6 .
wd:Q1 wdt:P31 wd:Q13442814; wdt:P50 wd:Q2, wd:Q4 .
wd:Q2 wdt:P31 wd:Q5; wdt:P496 "0000-0002-4166-7093" .
wd:Q3 wdt:P50 wd:Q2 .
wd:Q4 wdt:P31 wd:Q5; wdt:P496 "0000-0001-7536-3744" .
"""


def test_declared_properties_and_windows_become_membership_classed_rows():
    strategy = WikibaseStrategy(membership=WDT + "P31", window=3)
    data = Dataset().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, strategy=strategy, delay=0) as miner:
        schema = miner.mine("wikibase")
        report = miner.last_report
    rows = {(p.subject_class, p.property_uri, p.object_class, p.datatype) for p in schema.patterns}
    assert rows == {
        (WD + "Q13442814", WDT + "P50", WD + "Q5", None),
        (WD + "Q5", WDT + "P496", "Literal", "http://www.w3.org/2001/XMLSchema#string"),
    }, "Classes come from the membership property, never rewritten as rdf:type"
    assert schema.about.membership_property == WDT + "P31"
    labels = {a.term_iri: a.text.value for a in schema.enrichment.labels}
    assert labels[WDT + "P496"] == "ORCID iD" and labels[WDT + "P50"] == "author"
    examples = {e.value.value for e in schema.enrichment.examples if e.property_uri == WDT + "P496"}
    assert examples <= {"0000-0002-4166-7093", "0000-0001-7536-3744"} and examples
    windows = report.config["wikibase"]["properties"]
    assert windows[WDT + "P496"] == {"statements": 2, "state": "complete"}, "Window not filled"
    assert windows[WDT + "P50"] == {"statements": 3, "state": "sampled"}, (
        "A full window may miss rows"
    )
    assert report.config["wikibase"]["unclassified_subject_statements"] == 1, "Counted, not dropped"
    assert report.completion_state == "partial", "Sampled properties keep the run partial"


def test_a_rate_limit_stops_the_run_and_leaves_the_rest_for_a_resume(monkeypatch):
    strategy = WikibaseStrategy(membership=WDT + "P31", window=3)
    data = Dataset().parse(data=DATA, format="turtle")
    with SchemaMiner.from_graph(data, strategy=strategy, delay=0) as miner:
        answer = miner._helper.select

        def select(query, purpose=""):
            if purpose == "wikibase/window" and "P50>" in query:  # P496, then P50, then P6
                raise EndpointRateLimitError("Host cooldown exceeds wait budget: example.org")
            return answer(query, purpose=purpose)

        monkeypatch.setattr(miner._helper, "select", select)
        miner.mine("wikibase")
        report = miner.last_report
    windows = report.config["wikibase"]["properties"]
    assert (
        windows[WDT + "P496"]["state"] == "complete"
        and windows[WDT + "P50"]["state"] == "rate_limited"
    )
    assert windows[WDT + "P6"] == {"statements": None, "state": "not_attempted"}, (
        "Not failed one by one"
    )
    assert report.completion_state == "partial"


WIKIBASE_SCOPE_WD, WIKIBASE_SCOPE_WDT, WB = (
    "http://www.wikidata.org/entity/",
    "http://www.wikidata.org/prop/direct/",
    "http://wikiba.se/ontology#",
)
P, PS, PQ = (f"http://www.wikidata.org/prop/{part}" for part in ("", "statement/", "qualifier/"))
ITEM, STATEMENT, BEST = WB + "Item", WB + "Statement", WB + "BestRank"
CLASSES = {WIKIBASE_SCOPE_WD + "Q": ITEM, WIKIBASE_SCOPE_WD + "statement/": STATEMENT}
WIKIBASE_SCOPE_DATA = """
@prefix wd: <http://www.wikidata.org/entity/> . @prefix wds: <http://www.wikidata.org/entity/statement/> .
@prefix wdt: <http://www.wikidata.org/prop/direct/> . @prefix p: <http://www.wikidata.org/prop/> .
@prefix ps: <http://www.wikidata.org/prop/statement/> . @prefix pq: <http://www.wikidata.org/prop/qualifier/> .
@prefix wikibase: <http://wikiba.se/ontology#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
wd:P31 a wikibase:Property; wikibase:directClaim wdt:P31; rdfs:label "instance of"@en .
wd:P50 a wikibase:Property; wikibase:directClaim wdt:P50; wikibase:claim p:P50;
    wikibase:statementProperty ps:P50; rdfs:label "author"@en, "auteur"@fr .
wd:P1545 a wikibase:Property; wikibase:qualifier pq:P1545; rdfs:label "series ordinal"@en .
wd:Q5 rdfs:label "human"@en .
wd:Q1 wdt:P31 wd:Q13442814; rdfs:label "A work"@en; wdt:P50 wd:Q2; p:P50 wds:Q1-a, wds:Q1-b .
wds:Q1-a a wikibase:BestRank; ps:P50 wd:Q2; pq:P1545 "1" .
wds:Q1-b ps:P50 wd:Q3 .
wd:Q2 wdt:P31 wd:Q5; wdt:P496 "0000-0002-4166-7093" .
wd:Q3 wdt:P31 wd:Q5; wdt:P1 "one" .
wd:Q4 wdt:P31 wd:Q5; wdt:P2 "two" .
"""


def mine(strategy, data=WIKIBASE_SCOPE_DATA):
    with SchemaMiner.from_graph(
        Dataset().parse(data=data, format="turtle"), strategy=strategy, delay=0
    ) as miner:
        return miner.mine("scope"), miner.last_report


def test_seeds_and_one_hop_become_rows_of_member_stated_and_implied_classes():
    strategy = WikibaseScopeStrategy(
        [WIKIBASE_SCOPE_WD + "Q1", WIKIBASE_SCOPE_WD + "P50"],
        follow=[P + "P50"],
        membership=WIKIBASE_SCOPE_WDT + "P31",
        prefix_classes=CLASSES,
    )
    schema, report = mine(strategy)
    rows = {(p.subject_class, p.property_uri, p.object_class) for p in schema.patterns}
    assert {
        (ITEM, WIKIBASE_SCOPE_WDT + "P50", WIKIBASE_SCOPE_WD + "Q5"),
        (ITEM, WIKIBASE_SCOPE_WDT + "P50", ITEM),
        (ITEM, WIKIBASE_SCOPE_WDT + "P31", ITEM),
    } <= rows
    assert (
        WIKIBASE_SCOPE_WD + "Q13442814",
        WIKIBASE_SCOPE_WDT + "P50",
        WIKIBASE_SCOPE_WD + "Q5",
    ) in rows, "Members keep the class of P31"
    assert {(ITEM, P + "P50", STATEMENT), (ITEM, P + "P50", BEST)} <= rows, (
        "Stated and implied types"
    )
    assert {(BEST, PQ + "P1545", "Literal"), (STATEMENT, PS + "P50", ITEM)} <= rows, (
        "One hop is mined"
    )
    assert (WB + "Property", WB + "directClaim", "Resource") in rows
    predicates = {p.property_uri for p in schema.patterns}
    assert not predicates & {
        WIKIBASE_SCOPE_WDT + "P496",
        WIKIBASE_SCOPE_WDT + "P1",
        WIKIBASE_SCOPE_WDT + "P2",
    }, "Values of seeds are not seeds"
    assert "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in predicates, "Types are classes"
    labels = {a.term_iri: a.text.value for a in schema.enrichment.labels}
    assert (
        labels[WIKIBASE_SCOPE_WDT + "P50"] == labels[PS + "P50"] == "author"
        and labels[P + "P50"] == "author statement"
    )
    assert labels[PQ + "P1545"] == "series ordinal" and labels[WIKIBASE_SCOPE_WD + "Q5"] == "human"
    models = build_pydantic_classes(schema)
    assert {"author", "author_statement", "instance_of", "label"} <= set(
        models["Item"].model_fields
    )
    assert {"author", "series_ordinal"} <= set(models["BestRank"].model_fields)
    scope = report.config["scope"]
    assert scope["subjects"] == 2 and scope["followed"] == 2 and scope["classes"] == {}
    assert report.completion_state == "complete", "Every statement of the scope was read"


def test_another_endpoint_can_declare_the_properties(monkeypatch):
    """The scholarly graph of the Wikidata Query Service has no property declarations."""
    items, lines = WIKIBASE_SCOPE_DATA.split("wd:P31 a")[0], WIKIBASE_SCOPE_DATA.splitlines()
    works = items + "\n".join(line for line in lines if line.startswith(("wd:Q1 ", "wds:Q1-a")))
    declared = Dataset().parse(data=WIKIBASE_SCOPE_DATA, format="turtle")
    with SchemaMiner.from_graph(declared, delay=0) as main:
        monkeypatch.setattr(wikibase_strategy, "SparqlHelper", lambda url, **options: main.helper)
        strategy = WikibaseScopeStrategy(
            [WIKIBASE_SCOPE_WD + "Q1"],
            follow=[P + "P50"],
            prefix_classes=CLASSES,
            declarations="https://main",
        )
        schema, _ = mine(strategy, works)
    labels = {a.term_iri: a.text.value for a in schema.enrichment.labels}
    assert labels[P + "P50"] == "author statement" and labels[PQ + "P1545"] == "series ordinal"
