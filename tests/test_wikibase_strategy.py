"""Mine a Wikibase endpoint from its property declarations and bounded statement windows."""

from rdflib import Dataset

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.wikibase_strategy import WikibaseStrategy

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
    assert windows[WDT + "P50"] == {"statements": 3, "state": "sampled"}, "A full window may miss rows"
    assert report.config["wikibase"]["unclassified_subject_statements"] == 1, "Counted, not dropped"
    assert report.completion_state == "partial", "Sampled properties keep the run partial"
