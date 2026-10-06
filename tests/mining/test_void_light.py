"""VoID-first mining stays light and honest (remote rehearsal of 2026-10-06): light queries
that run past their limit stop their purpose, the rest is recorded as gaps; a class population
that the VoID's own partitions contradict is a lower bound, never an exact number; and the
evidence that the VoID states is not queried again."""

from __future__ import annotations

from functools import partial

from rdflib import RDF, Dataset, Graph, Literal, URIRef

from rdfsolve.evidence.observed import collect_property_usage_evidence
from rdfsolve.mining import void_strategy
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.void_strategy import VoidStrategy, void_property_usage
from rdfsolve.schema_models.exporters.void import to_void_graph
from rdfsolve.schema_models.readers.void import (
    scope_void_graph,
    void_class_populations,
    void_datasets_of_graphs,
)
from rdfsolve.sparql_helper import EndpointTimeoutError, QueryCuts

from .test_void_strategy import G1, _data, _strategy

SV = "http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#SubstanceVersion"
HAS_VALUE = "http://semanticscience.org/resource/has-value"

# IDSM's VoID of 2026-09-28 for PubChem's substance descriptors (counts as published): the
# class partition states 1 distinct subject; its has-value partition has 347,175,584.
SUBSTANCE = f"""
@prefix void: <http://rdfs.org/ns/void#> .
@prefix void-ext: <http://ldf.fi/void-ext#> .
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

[] sd:namedGraph [ sd:name <urn:substance> ; sd:graph <urn:void:substance> ] .
<urn:void:substance> void:classPartition <urn:cp:sv> .
<urn:cp:sv> void:class <{SV}> ; void:distinctSubjects 1 ; void:triples 1388702336 ;
    void:propertyPartition <urn:pp:value> .
<urn:pp:value> void:property <{HAS_VALUE}> ; void:triples 347175584 ;
    void:distinctSubjects 347175584 ; void:distinctObjects 220 ;
    void-ext:distinctLiterals 220 ; void-ext:distinctIRIReferenceObjects 0 ;
    void-ext:datatypePartition [ void-ext:datatype xsd:int ; void:triples 347175584 ] .
"""


def _substance() -> Graph:
    void = Graph().parse(data=SUBSTANCE, format="turtle")
    return scope_void_graph(void, void_datasets_of_graphs(void, ["urn:substance"]))


def test_a_class_count_that_the_void_contradicts_is_a_lower_bound():
    """Regression (pubchem.descriptor.substance, job 115211): subject_count 1 recorded as
    complete for a class whose property has 347 M subjects."""
    counts, states, contradicted = void_class_populations(_substance(), subjects_fallback=True)
    assert counts == {SV: 347175584} and states == {SV: "partial"}
    assert contradicted == [
        {"class": SV, "stated": 1, "lower_bound": 347175584, "property": HAS_VALUE}
    ]
    # Without the fallback, a class partition with no void:entities has no count at all.
    assert void_class_populations(_substance())[0] == {}

    strategy = VoidStrategy(_substance(), void_graph="urn:void")
    strategy.void, strategy.record = _substance(), {}
    counts, states, _ = void_class_populations(strategy.void, subjects_fallback=True)
    evidence = collect_property_usage_evidence(
        dataset_id="substance",
        classes=[SV],
        class_entity_counts=counts,
        class_entity_count_states=states,
        helper=None,  # type: ignore[arg-type]  # nothing is queried: the VoID states it all
        graph_uris=["urn:substance"],
        stated=void_property_usage(strategy.void),
        stated_by="void/urn:void",
    )
    (population,) = evidence.class_populations
    assert population.subject_count == 347175584
    assert population.count_status == "partial" and population.count_bound == "lower_bound"
    (record,) = evidence.records
    assert record.subjects_with_property == 347175584 and record.triple_count == 347175584
    assert record.distinct_objects == 220
    assert record.node_kind_counts == {"Literal": 347175584}
    assert record.datatype_counts == {"http://www.w3.org/2001/XMLSchema#int": 347175584}
    assert record.summary_state.purpose == "stated/void/urn:void"
    assert record.denominator_state == "partial" and record.support_fraction is None


def test_measured_subjects_above_a_complete_population_make_it_a_lower_bound():
    """Whatever stated the population: its own records show it is too small."""
    from rdfsolve.evidence.observed import (
        ClassPopulationEvidence,
        MeasurementState,
        PropertyUsageEvidence,
        _bound_contradicted_populations,
    )

    population = ClassPopulationEvidence(
        class_iri=SV,
        scope_semantics="endpoint_default_graph",
        subject_count=1,
        count_status="complete",
    )
    record = PropertyUsageEvidence(
        subject_class=SV,
        property_uri=HAS_VALUE,
        scope_semantics="endpoint_default_graph",
        eligible_subjects=1,
        denominator_state="complete",
        subjects_with_property=349259567,
        summary_state=MeasurementState(status="complete", purpose="evidence/property-usage"),
    )
    assert record.support_fraction == 349259567.0, "the wrong number this guards against"
    _bound_contradicted_populations([population], [record])
    assert (population.subject_count, population.count_bound) == (349259567, "lower_bound")
    assert record.eligible_subjects == 349259567 and record.support_fraction is None


def test_a_lower_bound_is_not_written_as_void_entities():
    from rdfsolve.models import MinedSchema

    schema = MinedSchema.model_validate(
        {"patterns": [{"subject_class": SV, "property_uri": HAS_VALUE, "object_class": "Literal"}]}
    )
    schema.about.class_entity_counts = {SV: 347175584}
    schema.about.class_entity_count_states = {SV: "partial"}
    assert "entities" not in to_void_graph(schema).serialize(format="turtle")


def test_the_mined_schema_keeps_the_state_of_a_contradicted_count():
    """VoidStrategy hands the state to the miner, so the schema's about says partial."""
    void = Graph().parse(data=SUBSTANCE, format="turtle")
    strategy = VoidStrategy(
        scope_void_graph(void, void_datasets_of_graphs(void, ["urn:substance"])),
        void_graph="urn:void",
        drift_largest=0,
        drift_random=0,
        example_patterns=0,
    )
    data = Dataset(default_union=True)
    graph = data.graph(URIRef("urn:substance"))
    graph.add((URIRef("urn:sv1"), RDF.type, URIRef(SV)))
    graph.add((URIRef("urn:sv1"), URIRef(HAS_VALUE), Literal(1)))
    miner = SchemaMiner.from_graph(data, graph_uris=["urn:substance"], strategy=strategy, delay=0)
    try:
        schema = miner.mine(dataset_name="substance")
    finally:
        miner.close()
    assert schema.about.class_entity_counts[SV] == 347175584
    assert schema.about.class_entity_count_states[SV] == "partial"
    assert miner.last_report.config["void_source"]["class_counts_contradicted"][0]["stated"] == 1


def test_light_queries_that_run_past_their_limit_stop_and_the_rest_are_gaps(monkeypatch):
    """IDSM: each drift re-count of a 347 M-triple partition ran to the 60 s limit, one after
    another. After the stop, each query not sent is a measurement gap with its reason."""
    monkeypatch.setattr(void_strategy, "QueryCuts", partial(QueryCuts, timeouts_after=1))
    strategy = _strategy()
    miner = SchemaMiner.from_graph(_data(), graph_uris=[G1], strategy=strategy, delay=0)
    select = miner.helper.select
    sent = []

    def slow(query, purpose=""):
        sent.append(purpose)
        if purpose == "void/drift":
            raise EndpointTimeoutError("Timeout: Read timed out. (read timeout=30.0)")
        return select(query, purpose=purpose)

    miner.helper.select = slow
    try:
        schema = miner.mine(dataset_name="fixture")
    finally:
        miner.close()
    assert sent.count("void/drift") == 1, "Two re-counts: one ran past its limit, one not sent"
    record = miner.last_report.config["void_source"]
    assert record["stopped"]["void/drift"]["queries_not_sent"] == 1
    assert record["stopped"]["void/drift"]["cut_by"] == "client time limit"
    gaps = [g for g in miner.last_report.measurement_gaps if g.purpose == "void/drift"]
    assert any(g.message.startswith("not sent: 1 queries in a row ran past") for g in gaps)
    # Three from the VoID and its object gap; one of the IRI subject without a class (s1).
    assert len(schema.patterns) == 4 and not miner.last_report.query_failures
    assert sum(p.untyped_subject for p in schema.patterns) == 1


def test_client_timeouts_do_not_stop_a_step_that_is_not_light():
    cuts = QueryCuts()
    for _ in range(10):
        cuts.failed("evidence/property-usage", EndpointTimeoutError("Timeout"))
    assert not cuts.stopped()
    light = QueryCuts(client_timeouts=True)
    for _ in range(4):
        light.failed("void/drift", EndpointTimeoutError("Timeout"))
    assert not light.stopped(), "Runs of three timeouts then an answer were seen: wait for five"
    light.failed("void/drift", EndpointTimeoutError("Timeout"))
    assert light.stopped("void/drift") == "5 queries in a row ran past the step's time limit"
    assert light.skip("void/drift") and not light.skip("void/examples")


class _VirtuosoVoid:
    """A Virtuoso whose CONSTRUCT of the VoID graph fails as on SIBiLS (D1CTX)."""

    def __init__(self, refuse_terms: bool):
        self.refuse_terms = refuse_terms
        self.max_response_bytes = 1
        self.void = Graph().parse(data=SUBSTANCE, format="turtle")

    def budget(self, seconds):
        from contextlib import nullcontext

        return nullcontext()

    def select(self, query, purpose=""):
        return {"results": {"bindings": [{"g": {"value": "urn:void"}, "n": {"value": "1"}}]}}

    def construct(self, query):
        from rdfsolve.sparql_helper import EndpointError

        if "FILTER" not in query or self.refuse_terms:
            raise EndpointError("HTTP 500: D1CTX: Hash dictionary is full")
        return self.void.serialize(format="turtle")

    prepare_paginated_query = staticmethod(lambda q: q + "\nOFFSET {offset}\nLIMIT {limit}")

    def select_chunked(self, template, **kwargs):
        def binding(t):
            if isinstance(t, URIRef):
                return {"type": "uri", "value": str(t)}
            if isinstance(t, Literal):
                row = {"type": "literal", "value": str(t)}
                if t.datatype:
                    row["datatype"] = str(t.datatype)
                return row
            return {"type": "bnode", "value": str(t)}

        yield [{"s": binding(s), "p": binding(p), "o": binding(o)} for s, p, o in self.void]


def test_a_void_graph_that_virtuoso_refuses_to_construct_is_read_another_way():
    from rdfsolve.mining.void_strategy import find_published_void

    found = find_published_void(_VirtuosoVoid(refuse_terms=False))
    assert found is not None and found.read_by == "construct_void_terms"
    assert void_class_populations(found.void, subjects_fallback=True)[0] == {SV: 347175584}
    # The SUBSTANCE fixture has a blank node (its datatype partition): pages cannot hold it.
    assert find_published_void(_VirtuosoVoid(refuse_terms=True)) is None
    endpoint = _VirtuosoVoid(refuse_terms=True)
    for triple in list(endpoint.void):
        if any(not isinstance(t, (URIRef, Literal)) for t in triple):
            endpoint.void.remove(triple)
    found = find_published_void(endpoint)
    assert found is not None and found.read_by == "select_pages"
    assert len(found.void) == len(endpoint.void)
