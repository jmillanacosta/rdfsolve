"""A route across two datasets is a path in the first dataset, a link, and a path in the second
dataset. It is tested on the data (the owner decision of 2026-09-30): the start instances and
their link values are read, the values are looked up in the second dataset with its path, and
the start instances that reach the end are counted. Between two local indexes every value is
read and a matched route is confirmed; with a sample it is tested."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.routes import Route, check_routes, propose_segments
from rdfsolve.mappings.signatures import Link, LinkEvidence
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

UP = "http://purl.uniprot.org/uniprot/"
HAS_GENE = SchemaPattern(subject_class="urn:Pathway", property_uri="urn:has", object_class="urn:Gene")
XREF = SchemaPattern(subject_class="urn:Gene", property_uri="urn:xref", object_class="Resource")
IN_TAXON = SchemaPattern(subject_class="urn:Protein", property_uri="urn:in", object_class="urn:Taxon")
BINDS = SchemaPattern(subject_class="urn:Protein", property_uri="urn:binds", object_class="urn:Drug")
NAME = SchemaPattern(subject_class="urn:Protein", property_uri="urn:name", object_class="Literal")
GENES = MinedSchema(about=AboutMetadata.build(dataset_name="genes"), patterns=[HAS_GENE, XREF])
PROTEINS = MinedSchema(
    about=AboutMetadata.build(dataset_name="proteins"), patterns=[IN_TAXON, BINDS, NAME]
)
SOURCE = f"""
<urn:pw/1> a <urn:Pathway> ; <urn:has> <urn:gene/1>, <urn:gene/2> .
<urn:pw/2> a <urn:Pathway> ; <urn:has> <urn:gene/3> .
<urn:pw/3> a <urn:Pathway> ; <urn:has> <urn:gene/2> .
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> .
"""
TARGET = f"""
<{UP}P04637> a <urn:Protein> ; <urn:in> <urn:taxon/9606> ; <urn:name> "p53" .
<{UP}P38398> a <urn:Protein> ; <urn:name> "BRCA1" .
<urn:taxon/9606> a <urn:Taxon> .
"""
JOIN = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
LINK = LinkEvidence(JOIN, 3, 2, {UP + "{id}": 2}, [(UP + "P04637", UP + "P04637")], complete=True)


def _check(sample=None):
    befores, afters = propose_segments(JOIN, GENES, PROTEINS)
    source = Dataset().parse(format="turtle", data=SOURCE)
    target = Dataset().parse(format="turtle", data=TARGET)
    with Client(GENES, source) as s, Client(PROTEINS, target) as t:
        return check_routes(JOIN, befores, afters, s, t, sample=sample, read_target=sample is None)


def _steps(route):
    return [p.property_uri for p in route.before], [p.property_uri for p in route.after]


def test_segments_are_the_schema_steps_at_each_end_of_the_link():
    befores, afters = propose_segments(JOIN, GENES, PROTEINS)
    assert [[p.property_uri for p in b] for b in befores] == [[], ["urn:has"]]
    assert [[p.property_uri for p in a] for a in afters] == [[], ["urn:in"], ["urn:binds"]]


def test_a_route_is_counted_by_the_start_instances_that_reach_the_end():
    result = _check()
    found = {(*map(tuple, _steps(e.route)),): (e.starts, e.matched) for e in result.routes}
    assert found == {
        (("urn:has",), ()): (3, 2),  # pathways 1 and 3 have a gene whose protein is found
        ((), ("urn:in",)): (3, 1),  # one gene has a protein with a taxon
        (("urn:has",), ("urn:in",)): (3, 1),  # pathway 1 only
    }, "The link alone is not a route; the route to a drug has no match and is left out"
    assert result.tested == 5 and all(e.complete and e.level == "confirmed" for e in result.routes)
    whole = next(e for e in result.routes if e.route.before and e.route.after)
    assert (whole.route.start_class, whole.route.end_class) == ("urn:Pathway", "urn:Taxon")


def test_a_sampled_route_is_tested_and_not_confirmed():
    result = _check(sample=2)
    assert result.routes and all(not e.complete and e.level == "tested" for e in result.routes)
    assert all(e.starts <= 2 for e in result.routes)


def test_the_budget_stops_the_test_and_is_reported():
    befores, afters = propose_segments(JOIN, GENES, PROTEINS)
    source = Dataset().parse(format="turtle", data=SOURCE)
    target = Dataset().parse(format="turtle", data=TARGET)
    ticks = iter(range(0, 10_000, 50))
    with Client(GENES, source) as s, Client(PROTEINS, target) as t:
        result = check_routes(
            JOIN, befores, afters, s, t, sample=None, budget_s=60, clock=lambda: next(ticks)
        )
    assert result.stop_reason == "budget" and result.tested < 5


def test_a_route_names_its_ends():
    route = Route(JOIN, (HAS_GENE,), ())
    assert (route.start_class, route.end_class) == ("urn:Pathway", "urn:Protein")
