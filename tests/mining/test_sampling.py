"""A query the endpoint refuses as a whole is asked again over a bounded sample: its rows stand,
flagged sampled, with counts that are lower bounds, and the source is not partial for it.

The local endpoint here refuses every count of one property (or class) unless the query holds a
LIMIT, as STRING's gateway refuses the count of functional-any-confidence-cutoff (354 M triples:
HTTP 502 after about 540 s) but answers a sub-select of its first edges.
"""

from __future__ import annotations

import json

import pytest
from rdflib import RDFS, Dataset, Graph, Literal, Namespace, URIRef
from rdflib.namespace import SH

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.sampling import sample_query, sample_size, sample_sizes
from rdfsolve.schema_models import MinedSchema
from rdfsolve.sparql_helper import EndpointTimeoutError
from tests.mining.test_untyped_subjects import FUNCTIONAL, GRAPH, string_like

EX = Namespace("urn:ex:")
VOID = Namespace("http://rdfs.org/ns/void#")
CUT = "HTTP 502 Bad Gateway (overload): 502 Server Error"


@pytest.fixture(autouse=True)
def no_cooldown(monkeypatch):
    """A refused page is retried after a cooldown: none in these tests."""
    import rdfsolve.sparql_helper

    monkeypatch.setattr(rdfsolve.sparql_helper.time, "sleep", lambda seconds: None)


def refusing(miner: SchemaMiner, prop: str, *, samples: bool = True) -> None:
    """Refuse each count of the edges of *prop*, unless it is bounded (a LIMIT sub-select);
    with *samples* False, refuse the bounded ones too."""
    select = miner.helper.select
    edges = (f"?s {prop} ?o", f"VALUES ?p {{ {prop} }}")

    def answer(query, purpose="", **kwargs):
        bounded = "} LIMIT " in query
        named = any(edge in query for edge in edges)
        if named and "COUNT" in query and (not bounded or not samples):
            raise EndpointTimeoutError(CUT)
        return select(query, purpose, **kwargs)

    miner.helper.select = answer


def test_a_refused_untyped_property_is_kept_as_a_sample_with_lower_bounds():
    """STRING-like data: the counts of functional-any are refused, its census too. The census,
    the structural discovery and the untyped pass ask over a sample of 2 edges: the pattern
    stands, flagged sampled, with lower-bound counts, and the source is complete."""
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH], sample_size=2) as miner:
        refusing(miner, f"<{FUNCTIONAL}>")
        schema = miner.mine(dataset_name="sampled")
        report = miner.last_report
    (pattern,) = [p for p in schema.patterns if p.property_uri == str(FUNCTIONAL)]
    assert pattern.object_class == "Resource" and pattern.untyped_subject
    assert pattern.count_bound == "lower_bound" and pattern.sampled is not None
    assert pattern.sampled.size == 2 and pattern.sampled.unit == "edges"
    assert set(pattern.sampled.covers) == {"patterns", "counts"}
    assert "timeout" in pattern.sampled.reason and "502" in pattern.sampled.reason
    assert pattern.count == 2, "The 2 edges of the sample, of 6"
    assert report.completion_state == "complete" and not report.query_failures
    assert report.config["untyped_subjects"]["sampled"] == [str(FUNCTIONAL)]
    purposes = {s.purpose for s in report.sampled_queries}
    assert any(p.startswith("untyped-subjects/untyped-uri") for p in purposes)
    assert report.config["count_coverage"]["lower_bound_counts"] >= 1
    # The other properties are exact.
    others = [p for p in schema.patterns if p.property_uri != str(FUNCTIONAL)]
    assert others and all(p.sampled is None and p.count_bound is None for p in others)


def test_a_refused_census_and_discovery_are_answered_over_samples(monkeypatch):
    """Through an endpoint (not the local bulk census) the census of functional-any is
    refused: it is counted over a sample, and its structural patterns are discovered over a
    sample of its edges, with the counts of the sample when their recount is refused too."""
    from rdfsolve.mining import structural_strategy

    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH], sample_size=2) as miner:
        refusing(miner, f"<{FUNCTIONAL}>")
        schema = miner.mine(dataset_name="census")
        report = miner.last_report
    (entry,) = report.config["structural_coverage"]
    census = entry["census_properties"][str(FUNCTIONAL)]
    assert "refused" in census and census["sampled"]["size"] == 2
    assert census["sampled"]["triples"] == 2 and census["sampled"]["untypedTriples"] == 2
    assert entry["state"] == "sampled" and "discovery_sampled" in census
    shapes = [p for p in schema.structural_patterns or [] if p.property_uri == str(FUNCTIONAL)]
    assert shapes and all(p.sampled is not None for p in shapes)
    assert all(p.count_bound == "lower_bound" and p.count <= 2 for p in shapes)
    assert {s.purpose for s in report.sampled_queries} >= {
        "structural/coverage",
        "structural/discovery",
    }
    assert report.completion_state == "complete" and not report.query_failures
    assert report.config["untyped_subjects"]["selected_by"] == "structural census"
    assert str(FUNCTIONAL) in report.config["untyped_subjects"]["with_untyped_subjects"]


def test_when_the_sample_is_refused_too_the_failure_stands():
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH], sample_size=2) as miner:
        refusing(miner, f"<{FUNCTIONAL}>", samples=False)
        schema = miner.mine(dataset_name="refused")
        report = miner.last_report
    assert not [p for p in schema.patterns if p.property_uri == str(FUNCTIONAL)]
    assert report.completion_state == "partial" and report.query_failures
    assert not report.sampled_queries


def typed_graph() -> Graph:
    """Six members of A with a literal and a link to a B; one B."""
    g = Graph()
    g.add((EX.b, RDFS.label, Literal("b")))
    g.add((EX.b, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), EX.B))
    for i in range(6):
        node = EX[f"a{i}"]
        g.add((node, URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), EX.A))
        g.add((node, EX.name, Literal(f"a{i}")))
        g.add((node, EX.link, EX.b))
    return g


def test_a_refused_class_count_becomes_a_sampled_lower_bound(tmp_path):
    """The counts of class A are refused (also class by class and in pages); a sample of 2
    members answers. The patterns of A keep their rows, with lower-bound counts; the exports do
    not state them as exact."""
    with SchemaMiner.from_graph(typed_graph(), delay=0, sample_size=2) as miner:
        select = miner.helper.select

        def answer(query, purpose="", **kwargs):
            if (
                purpose.startswith(("counts/", "class-entity-counts"))
                and "<urn:ex:A>" in query
                and "} LIMIT " not in query
            ):
                raise EndpointTimeoutError(CUT)
            return select(query, purpose, **kwargs)

        miner.helper.select = answer
        schema = miner.mine(dataset_name="classes")
        report = miner.last_report
    a = {
        (p.property_uri, p.object_class): p for p in schema.patterns if p.subject_class == str(EX.A)
    }
    name, link = a[(str(EX.name), "Literal")], a[(str(EX.link), str(EX.B))]
    for pattern in (name, link):
        assert pattern.count_bound == "lower_bound" and pattern.sampled is not None
        assert pattern.sampled.unit == "members" and pattern.sampled.covers == ["counts"]
        assert pattern.count == 2, "The edges of 2 sampled members, of 6"
    (b,) = [p for p in schema.patterns if p.subject_class == str(EX.B)]
    assert b.count == 1 and b.count_bound is None and b.sampled is None
    assert report.completion_state == "complete" and report.sampled_queries
    assert schema.about.class_entity_counts[str(EX.A)] == 2
    assert schema.about.class_entity_count_states[str(EX.A)] == "partial", "A lower bound"

    void = Graph().parse(data=json.dumps(schema.to_jsonld()), format="json-ld")
    stated = {
        int(n)
        for node in void.subjects(VOID["class"], EX.B)
        for n in void.objects(node, VOID.triples)
    }
    assert 2 not in stated, "No void:triples from a sample"
    assert not list(void.objects(None, VOID.entities)) or all(
        int(n) != 2 for n in void.objects(None, VOID.entities)
    )
    assert any("sample of 2 members" in str(c) for c in void.objects(None, RDFS.comment))

    # Canonical JSON carries the flags.
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(schema.to_dict()))
    again = MinedSchema.from_json(path)
    flagged = [p for p in again.patterns if p.sampled is not None]
    assert {(p.property_uri, p.count_bound) for p in flagged} == {
        (str(EX.name), "lower_bound"),
        (str(EX.link), "lower_bound"),
    }


def test_sampled_value_kinds_deactivate_their_shape():
    """A pattern found in a sample (covers patterns) gives a deactivated property shape, even
    when observed shapes are activated: the sample may miss a kind."""
    from rdfsolve.schema_models import AboutMetadata, PatternSample, SchemaPattern
    from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

    sample = PatternSample(size=10, unit="edges", reason="timeout: cut")
    schema = MinedSchema(
        patterns=[
            SchemaPattern(
                subject_class=str(EX.A),
                property_uri=str(EX.name),
                object_class="Literal",
                count=10,
                sampled=sample,
            ),
            SchemaPattern(
                subject_class=str(EX.A), property_uri=str(EX.link), object_class="Resource"
            ),
        ],
        about=AboutMetadata(dataset_name="shapes"),
    )
    (name,) = [p for p in schema.patterns if p.property_uri == str(EX.name)]
    assert name.count_bound == "lower_bound", "A count of a sample is a lower bound"
    shapes = minedschema_to_shacl(schema, activate_observed=True)
    by_path = {s.path: s for n in shapes.node_shapes for s in n.property_shapes}
    assert by_path[str(EX.name)].deactivated and "sample of 10 edges" in (
        by_path[str(EX.name)].description or ""
    )
    assert not by_path[str(EX.link)].deactivated
    graph = shapes.to_rdf()
    assert not list(graph.subject_objects(SH.minCount)) and not list(
        graph.subject_objects(SH.maxCount)
    )


def ontology_dataset() -> Dataset:
    data = Dataset(default_union=True)
    g = data.graph(URIRef("urn:data"))
    owl_class = URIRef("http://www.w3.org/2002/07/owl#Class")
    rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    for i in range(4):
        term = EX[f"T{i}"]
        g.add((term, rdf_type, owl_class))
        g.add((term, EX.smiles, Literal(f"C{i}")))
        g.add((EX[f"r{i}"], rdf_type, term))
        g.add((EX[f"r{i}"], EX.value, Literal(i)))
    return data


def mine_terms(refuse) -> tuple[MinedSchema, object]:
    from rdfsolve.mining import mine_with_ontology

    with SchemaMiner.from_graph(ontology_dataset(), delay=0, sample_size=1) as miner:
        select = miner.helper.select

        def answer(query, purpose="", **kwargs):
            if refuse(query, purpose):
                raise EndpointTimeoutError(CUT)
            return select(query, purpose, **kwargs)

        miner.helper.select = answer
        result = mine_with_ontology(
            miner, dataset_name="terms", ontology_as_data=True, ontology_term_budget=100
        )
        return result.data_schema, miner.last_report


def subject_rows(schema: MinedSchema) -> dict:
    return {
        (p.subject_class, p.property_uri): (p.count, p.count_bound)
        for p in schema.term_patterns or []
        if p.subject_binding == "term"
    }


def test_refused_term_subject_counts_are_read_in_batches_of_terms():
    """DGIdb on med2rdf: the subject counts of every term are cut at 120 s. The declared terms
    are listed and counted in batches: the rows are exact and the source complete."""
    exact, _ = mine_terms(lambda q, p: False)
    batched, report = mine_terms(lambda q, p: p == "ontology-terms/subject")
    assert subject_rows(batched) == subject_rows(exact) and len(subject_rows(exact)) == 4
    assert all(bound is None for _, bound in subject_rows(batched).values())
    assert report.completion_state == "complete" and not report.query_failures


def test_refused_term_batches_fall_back_to_samples_and_never_fail_the_source():
    """Every batch is refused, also term by term: each term is counted over a sample of its
    edges (lower bounds). When even the samples are refused, the counts are gaps: the source
    stays complete, because term counts are enrichment."""
    sampled, report = mine_terms(
        lambda q, p: p.startswith("ontology-terms/subject") and not p.endswith("/sample")
    )
    rows = subject_rows(sampled)
    assert len(rows) == 4 and all(bound == "lower_bound" for _, bound in rows.values())
    assert report.completion_state == "complete" and not report.query_failures
    assert report.config["ontology_term_probe"]["state"] == "sampled"

    gaps, report = mine_terms(lambda q, p: p.startswith("ontology-terms/subject"))
    assert not subject_rows(gaps)
    assert report.completion_state == "complete" and not report.query_failures
    assert any(g.purpose.startswith("ontology-terms/subject") for g in report.measurement_gaps)
    assert report.config["ontology_term_probe"]["state"] == "gaps"


def test_sample_sizes_and_the_bounded_query():
    with sample_size(100_000):
        assert sample_sizes() == [100_000, 10_000, 1_000]
    with sample_size(0):
        assert sample_sizes() == []
    query = "SELECT ?p (COUNT(*) AS ?n) WHERE {\n  ?s ?p ?o .\n}\nGROUP BY ?p\nORDER BY ?p"
    assert sample_query(query, 5) == (
        "SELECT ?p (COUNT(*) AS ?n) WHERE { { SELECT * WHERE {\n  ?s ?p ?o .\n} LIMIT 5 } }"
        "\nGROUP BY ?p\n"
    )


def test_the_release_counts_sampled_rows(tmp_path):
    """The release summary counts the rows observed in samples and their lower-bound counts."""
    from datetime import UTC, datetime

    from rdfsolve.release.model import ReleaseArtifact, ReleaseManifest
    from rdfsolve.release.summary import _summarize_observed

    rows = [
        {"pattern_type": "object_property", "count": 3, "sampled": None},
        {
            "pattern_type": "object_property",
            "count": 2,
            "count_bound": "lower_bound",
            "sampled": {"size": 2, "unit": "edges", "reason": "timeout", "covers": ["counts"]},
        },
    ]
    (tmp_path / "s.json").write_text(json.dumps({"schema": {"patterns": rows}}))
    manifest = ReleaseManifest(
        release_id="r",
        issued=datetime.now(UTC),
        run_root=str(tmp_path),
        artifacts=[
            ReleaseArtifact(
                artifact_id="a",
                path="s.json",
                sha256="0" * 64,
                byte_size=1,
                dataset_id="d",
                role="canonical_schema",
            )
        ],
    )
    summary = _summarize_observed(manifest, tmp_path)
    assert summary["sampled_patterns"] == 1
    assert summary["patterns_with_lower_bound_counts"] == 1


def _refusing_class_listing(miner: SchemaMiner, *, samples: bool = True) -> None:
    """Cut every form of the class listing (one query and its pages), as pdbj.bmrb's proxy
    did (job 115591); with *samples*, a listing over a LIMIT sub-select answers."""
    select = miner.helper.select

    def answer(query, purpose="", **kwargs):
        if purpose.startswith("two-phase/classes") and (not samples or "} LIMIT " not in query):
            raise EndpointTimeoutError(CUT)
        return select(query, purpose, **kwargs)

    miner.helper.select = answer


@pytest.mark.parametrize("samples", [True, False])
def test_a_refused_class_listing_does_not_fail_the_source(samples):
    """pdbj.bmrb ended FAILED after its class listing was cut in every form. The classes of a
    sample of the type statements are mined instead (a lower bound); when the samples are
    refused too, the listing is a gap and the untyped subjects are still mined. Either way the
    source ends partial, not failed."""
    graph = typed_graph()
    graph.add((EX.loose, EX.name, Literal("untyped")))
    with SchemaMiner.from_graph(graph, delay=0, sample_size=1000) as miner:
        _refusing_class_listing(miner, samples=samples)
        schema = miner.mine(dataset_name="classes")
        report = miner.last_report
    listing = report.config["class_listing"]
    assert report.completion_state == "partial"
    assert any(p.untyped_subject for p in schema.patterns), "Untyped subjects mined"
    typed = {p.subject_class for p in schema.patterns if not p.untyped_subject}
    if samples:
        assert listing["state"] == "sampled" and listing["count_bound"] == "lower_bound"
        assert typed == {str(EX.A), str(EX.B)}
        assert [f.category for f in report.query_failures] == ["sampled"]
    else:
        assert listing["state"] == "refused" and not typed
        assert report.config["class_schema_state"] == "class_listing_refused"


def test_a_sampled_census_whose_discovery_sample_is_refused_is_recorded(monkeypatch):
    """The census of functional-any is answered over a sample, and the discovery of its edges
    is refused even over a sample: the property is recorded as refused (the source partial),
    as STRING's census was in job 115712, where it raised UnboundLocalError instead."""
    from rdfsolve.mining import structural_strategy

    monkeypatch.setattr(structural_strategy, "LocalGraphHelper", type("Remote", (), {}))
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH], sample_size=2) as miner:
        refusing(miner, f"<{FUNCTIONAL}>")
        select = miner.helper.select

        def answer(query, purpose="", **kwargs):
            if purpose.startswith("structural/discovery") and str(FUNCTIONAL) in query:
                raise EndpointTimeoutError(CUT)
            return select(query, purpose, **kwargs)

        miner.helper.select = answer
        miner.mine(dataset_name="refused-discovery")
        report = miner.last_report
    (entry,) = report.config["structural_coverage"]
    census = entry["census_properties"][str(FUNCTIONAL)]
    assert "sampled" in census and "discovery_refused" in census
    assert report.completion_state == "partial"
    assert any(f.purpose == "structural/discovery" for f in report.query_failures)
