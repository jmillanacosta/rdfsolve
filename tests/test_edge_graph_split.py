"""A schema mined across a provider's graphs splits into one schema per dataset graph."""

from rdfsolve.config import mint
from rdfsolve.mining.edge_graph_split import split_by_edge_graph, unattributed_patterns
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

SUBSTANCE = "urn:graph:substance"
COMPOUND = "urn:graph:compound"


def _group() -> MinedSchema:
    return MinedSchema(
        patterns=[
            # Edge in the substance graph; the compound class comes from the compound graph.
            SchemaPattern(
                subject_class="urn:Substance",
                property_uri="urn:cid",
                object_class="urn:Compound",
                count=5,
                graphs={SUBSTANCE: 5},
                distinct_subjects=5,
                distinct_objects=4,
            ),
            SchemaPattern(
                subject_class="urn:Compound",
                property_uri="urn:label",
                object_class="Literal",
                count=7,
                graphs={SUBSTANCE: 2, COMPOUND: 5},
                distinct_subjects=6,
            ),
            SchemaPattern(subject_class="urn:X", property_uri="urn:p", object_class="Resource"),
        ],
        about=AboutMetadata.build(
            dataset_name="pubchem.ftp",
            graph_uris=[SUBSTANCE, COMPOUND],
            started_at="2026-09-21T10:00:00+00:00",
            content_sha256="abc",
        ),
    )


def test_each_dataset_keeps_the_edges_in_its_graph_with_cross_graph_types():
    part = split_by_edge_graph(_group(), SUBSTANCE, "pubchem.ftp.substance")
    rows = {(p.subject_class, p.object_class): p for p in part.patterns}
    assert set(rows) == {("urn:Substance", "urn:Compound"), ("urn:Compound", "Literal")}
    assert rows[("urn:Substance", "urn:Compound")].count == 5
    assert rows[("urn:Compound", "Literal")].count == 2
    assert rows[("urn:Compound", "Literal")].graphs == {SUBSTANCE: 2}


def test_distinct_counts_survive_only_for_single_graph_patterns():
    part = split_by_edge_graph(_group(), SUBSTANCE, "pubchem.ftp.substance")
    rows = {(p.subject_class, p.object_class): p for p in part.patterns}
    assert rows[("urn:Substance", "urn:Compound")].distinct_subjects == 5
    assert rows[("urn:Compound", "Literal")].distinct_subjects is None


def test_the_dataset_gets_its_own_identity_and_counts():
    part = split_by_edge_graph(
        _group(), COMPOUND, "pubchem.ftp.compound", declared_classes=frozenset({"urn:Compound"})
    )
    about = part.about
    assert about.dataset_name == "pubchem.ftp.compound"
    assert about.graph_uris == [COMPOUND]
    assert about.snapshot_id == mint(
        "snapshot", "pubchem.ftp.compound", "2026-09-21T10:00:00+00:00"
    )
    assert about.content_sha256 is None
    assert about.schema_uri == mint("schema", "pubchem.ftp.compound")
    assert (about.pattern_count, about.class_count, about.declared_class_count) == (1, 1, 1)
    assert part.navigation is None


def test_patterns_without_graph_counts_are_reported():
    assert [p.subject_class for p in unattributed_patterns(_group())] == ["urn:X"]
