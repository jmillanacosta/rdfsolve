"""Untyped data gets a schema: IRI subjects without a type get property-level patterns
(rdfs:Resource, subject_binding "untyped") with exact counts, in scan mining (count_patterns)
and through an endpoint (rdfsolve.mining.untyped_subjects), and the exports describe them.

The sample is shaped like STRING's main graph (no rdf:type at all): proteins linked to
proteins, a taxon IRI, a UniProt cross-reference, a label and a comment.
"""

from __future__ import annotations

import json

import pytest
from rdflib import RDF, RDFS, Dataset, Graph, Literal, Namespace, URIRef
from rdflib.namespace import XSD

from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.scan import ScanStrategy, count_patterns, store_from_graph
from rdfsolve.schema_models import UNTYPED_SUBJECT, MinedSchema, SchemaPattern
from rdfsolve.sparql_helper import EndpointTimeoutError

N = Namespace("http://string-db.org/network/")
INTERACTION = Namespace("http://string-db.org/rdf/interaction/")
ORGANISM = URIRef("http://string-db.org/rdf/organism/")
GRAPH = "http://string-db.org/rdf"
FUNCTIONAL = INTERACTION["functional-any-confidence-cutoff"]
PHYSICAL = INTERACTION["physical-any-confidence-cutoff"]


def string_like() -> Dataset:
    """A STRING-like graph: no subject has a type."""
    data = Dataset(default_union=True)
    g = data.graph(URIRef(GRAPH))
    proteins = [N[f"9606.ENSP{i}"] for i in range(1, 5)]
    for i, protein in enumerate(proteins):
        g.add((protein, RDFS.label, Literal(f"P{i}")))
        g.add((protein, RDFS.comment, Literal(f"protein {i}", lang="en")))
        g.add((protein, ORGANISM, URIRef("http://identifiers.org/taxonomy/9606")))
        g.add((protein, RDFS.seeAlso, URIRef(f"http://purl.uniprot.org/uniprot/P{i}")))
        for other in proteins[i + 1 :]:
            g.add((protein, FUNCTIONAL, other))
    g.add((proteins[0], PHYSICAL, proteins[1]))
    return data


def rows(schema_or_patterns) -> dict:
    """The patterns by key, with their counts."""
    patterns = getattr(schema_or_patterns, "patterns", schema_or_patterns)
    return {
        (p.subject_class, p.subject_binding, p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
            p.distinct_objects,
            p.count_semantics,
            p.graphs,
        )
        for p in patterns
    }


EXPECTED = {
    (str(FUNCTIONAL), "Resource", None): (6, 3, 3),
    (str(PHYSICAL), "Resource", None): (1, 1, 1),
    (str(ORGANISM), "Resource", None): (4, 4, 1),
    (str(RDFS.seeAlso), "Resource", None): (4, 4, 4),
    (str(RDFS.label), "Literal", str(XSD.string)): (4, 4, 4),
    (str(RDFS.comment), "Literal", str(RDF.langString)): (4, 4, 4),
}


def mine(data, strategy=None, **kwargs) -> tuple[MinedSchema, dict]:
    """Mine *data* through the local endpoint (SPARQL queries), or with *strategy*."""
    extra = {"strategy": strategy} if strategy is not None else {}
    with SchemaMiner.from_graph(data, delay=0, **extra, **kwargs) as miner:
        schema = miner.mine(dataset_name="stringlike")
        return schema, miner.last_report.config


def test_a_source_without_types_has_property_level_patterns(tmp_path):
    """Both channels give the same untyped patterns, with exact counts, for data that has no
    class at all; no class is reported."""
    store = store_from_graph(string_like(), tmp_path / "store")
    scanned = count_patterns(store)
    assert all(p.untyped_subject and p.subject_class == UNTYPED_SUBJECT for p in scanned)
    found = {
        (p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
            p.distinct_objects,
        )
        for p in scanned
    }
    assert found == EXPECTED
    remote, config = mine(string_like(), graph_uris=[GRAPH])
    scan, _ = mine(string_like(), ScanStrategy(store=store), graph_uris=[GRAPH])
    assert rows(remote) == rows(scan)
    assert {(k[2], k[3], k[4]): v[:3] for k, v in rows(remote).items()} == EXPECTED
    assert remote.get_classes() == [] and remote.about.class_count == 0
    record = config["untyped_subjects"]
    assert record["state"] == "complete" and record["selected_by"] == "structural census"
    assert record["patterns"] == len(EXPECTED)


def test_a_property_whose_subjects_are_all_untyped_is_counted_without_the_type_test():
    """The census found no typed subject of these properties: only FILTER(isIRI(?s)) is sent."""
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH]) as miner:
        select, sent = miner.helper.select, []

        def record(query, purpose=""):
            sent.append((purpose, query))
            return select(query, purpose)

        miner.helper.select = record
        miner.mine(dataset_name="stringlike")
    counts = [q for p, q in sent if p.startswith("untyped-subjects/")]
    assert counts and all("isIRI(?s)" in q for q in counts)
    assert not [q for q in counts if "_subjectType" in q]


MIXED = """
@prefix e: <https://example.org/> .
e:a a e:A ; e:name "a" ; e:link e:b , e:u .
e:b a e:B ; e:name "b" .
e:u e:name "u" ; e:link e:b , e:v ; e:part [ e:name "blank" ] .
e:v e:link e:u .
_:x e:name "blank subject" .
"""


def mixed() -> Graph:
    return Graph().parse(data=MIXED, format="turtle")


def typed_only() -> Graph:
    """MIXED without the triples of untyped subjects."""
    graph = mixed()
    typed = set(graph.subjects(RDF.type, None))
    for s, p, o in list(graph):
        if s not in typed:
            graph.remove((s, p, o))
    return graph


def test_typed_subjects_keep_their_patterns_and_are_never_counted_as_untyped(tmp_path):
    """The typed patterns are those of the data without the untyped subjects; the untyped
    patterns count the IRI subjects without a type only (not the blank nodes)."""
    for channel in ("sparql", "scan"):
        if channel == "scan":
            schema, _ = mine(mixed(), ScanStrategy(store=store_from_graph(mixed(), tmp_path / "m")))
            alone, _ = mine(
                typed_only(), ScanStrategy(store=store_from_graph(typed_only(), tmp_path / "t"))
            )
        else:
            schema, _ = mine(mixed())
            alone, _ = mine(typed_only())
        typed = [p for p in schema.patterns if not p.untyped_subject]
        assert rows(typed) == rows(alone), channel
        untyped = {
            (p.property_uri, p.object_class): (p.count, p.distinct_subjects, p.distinct_objects)
            for p in schema.patterns
            if p.untyped_subject
        }
        assert untyped == {
            ("https://example.org/name", "Literal"): (1, 1, 1),
            ("https://example.org/link", "https://example.org/B"): (1, 1, 1),
            ("https://example.org/link", "Resource"): (2, 2, 2),
            ("https://example.org/part", "BlankNode"): (1, 1, 1),
        }, channel
        (part,) = [
            p for p in schema.patterns if p.untyped_subject and p.object_class == "BlankNode"
        ]
        assert part.blank_node_predicates == ["https://example.org/name"], channel
        assert sorted(schema.get_classes()) == ["https://example.org/A", "https://example.org/B"]


def test_the_untyped_pass_can_be_turned_off(tmp_path):
    with SchemaMiner.from_graph(mixed(), delay=0, untyped_subjects=False) as miner:
        schema = miner.mine(dataset_name="off")
    assert schema.patterns and not any(p.untyped_subject for p in schema.patterns)
    store = store_from_graph(mixed(), tmp_path / "store")
    with SchemaMiner.from_graph(
        mixed(), delay=0, untyped_subjects=False, strategy=ScanStrategy(store=store)
    ) as miner:
        schema = miner.mine(dataset_name="off")
    assert schema.patterns and not any(p.untyped_subject for p in schema.patterns)


def test_without_a_census_the_properties_are_probed_and_refused_probes_are_gaps(monkeypatch):
    """Typed mining incomplete: no census. Each property is probed under a time limit; a probe
    that runs past it is a measurement gap, and after five in a row no more are sent."""
    from rdfsolve.mining import structural_strategy

    monkeypatch.setattr(structural_strategy.StructuralStrategy, "mine", lambda self, c: [])
    data = string_like()
    with SchemaMiner.from_graph(data, delay=0, graph_uris=[GRAPH]) as miner:
        schema = miner.mine(dataset_name="probed")
        record = miner.last_report.config["untyped_subjects"]
    assert record["selected_by"] == "probe" and record["probed"] == 6
    assert {(k[2], k[3], k[4]): v[:3] for k, v in rows(schema).items()} == EXPECTED

    with SchemaMiner.from_graph(data, delay=0, graph_uris=[GRAPH]) as miner:
        select = miner.helper.select

        def refuse(query, purpose=""):
            if purpose == "untyped-subjects/probe":
                raise EndpointTimeoutError("Timeout: Read timed out. (read timeout=30.0)")
            return select(query, purpose)

        miner.helper.select = refuse
        schema = miner.mine(dataset_name="refused")
        report = miner.last_report
    record = report.config["untyped_subjects"]
    assert not schema.patterns and record["state"] == "partial" and record["not_checked"] == 6
    assert record["stopped"]["untyped-subjects/probe"]["queries_not_sent"] == 1
    gaps = [g for g in report.measurement_gaps if g.purpose == "untyped-subjects/probe"]
    assert len(gaps) == 6 and any("not checked" in g.message for g in gaps)


def test_a_refused_count_is_a_failure_of_the_source():
    with SchemaMiner.from_graph(string_like(), delay=0, graph_uris=[GRAPH]) as miner:
        select = miner.helper.select

        def refuse(query, purpose=""):
            if purpose.startswith("untyped-subjects/untyped-uri") and str(ORGANISM) in query:
                raise EndpointTimeoutError("Timeout")
            return select(query, purpose)

        miner.helper.select = refuse
        schema = miner.mine(dataset_name="refused")
        report = miner.last_report
    assert report.config["untyped_subjects"]["state"] == "partial"
    assert report.completion_state != "complete" and report.query_failures
    assert not [p for p in schema.patterns if p.property_uri == str(ORGANISM)]


def test_exports_describe_untyped_patterns(tmp_path):
    """VoID: a subset of the dataset with property partitions, no class partition; SHACL: a
    shape of the subjects of each property that holds for typed subjects; canonical JSON and
    SHACL read them back."""
    from pyshacl import validate

    from rdfsolve.schema_models.exporters.shacl import minedschema_to_shacl

    schema, _ = mine(mixed())
    void = schema.to_void_graph()
    VOID = Namespace("http://rdfs.org/ns/void#")
    assert URIRef(UNTYPED_SUBJECT) not in set(void.objects(None, VOID["class"]))
    (subset,) = void.objects(None, VOID.subset)
    partitions = {
        void.value(pp, VOID.property): pp for pp in void.objects(subset, VOID.propertyPartition)
    }
    name = partitions[URIRef("https://example.org/name")]
    assert void.value(name, VOID.triples) == Literal(1, datatype=XSD.integer)
    assert void.value(name, VOID.distinctSubjects) == Literal(1, datatype=XSD.integer)
    link = partitions[URIRef("https://example.org/link")]
    assert {void.value(c, VOID["class"]) for c in void.objects(link, VOID.classPartition)} == {
        URIRef("https://example.org/B")
    }
    json.dumps(schema.to_jsonld())

    back = MinedSchema.from_dict(json.loads(json.dumps(schema.to_dict())))
    assert rows(back) == rows(schema)

    shapes = minedschema_to_shacl(schema, activate_observed=True)
    graph = shapes.to_rdf()
    SH = Namespace("http://www.w3.org/ns/shacl#")
    targets = set(graph.objects(None, SH.targetSubjectsOf))
    assert targets == {
        URIRef("https://example.org/name"),
        URIRef("https://example.org/link"),
        URIRef("https://example.org/part"),
    }
    conforms, _, text = validate(mixed(), shacl_graph=graph)
    assert conforms, text
    broken = mixed()
    broken.add(
        (
            URIRef("https://example.org/w"),
            URIRef("https://example.org/name"),
            URIRef("https://example.org/b"),
        )
    )
    broken.add(
        (
            URIRef("https://example.org/a"),
            URIRef("https://example.org/part"),
            Literal("typed: not checked"),
        )
    )
    conforms, report, _ = validate(broken, shacl_graph=graph)
    focus = set(report.objects(None, SH.focusNode))
    assert not conforms and focus == {URIRef("https://example.org/w")}, (
        "The untyped subject with an unobserved value kind; the typed subject a is not checked"
    )

    read = MinedSchema.from_shacl(graph.serialize(format="turtle"))
    untyped = {(p.property_uri, p.object_class) for p in read.patterns if p.untyped_subject}
    assert untyped == {
        (p.property_uri, p.object_class) for p in schema.patterns if p.untyped_subject
    }


def test_an_untyped_binding_names_rdfs_resource():
    with pytest.raises(ValueError, match="untyped subject binding"):
        SchemaPattern(
            subject_class="https://example.org/A",
            subject_binding="untyped",
            property_uri="https://example.org/p",
            object_class="Resource",
        )
    pattern = SchemaPattern(
        subject_class=UNTYPED_SUBJECT,
        subject_binding="untyped",
        property_uri="https://example.org/p",
        object_class="Resource",
    )
    assert pattern.untyped_subject
    assert MinedSchema(patterns=[pattern]).get_classes() == []
