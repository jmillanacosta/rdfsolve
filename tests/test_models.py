"""Test model classes."""
from rdfsolve.models import Connection, ConnectionType, EvidenceSource, SchemaPattern


def test_connection_basic():
    conn = Connection(
        source_dataset="ds1",
        source_entity="http://ex.org/C1",
        target_dataset="ds2",
        target_entity="http://ex.org/C2",
        connection_type=ConnectionType.CLASS_EQUIVALENCE,
    )
    assert conn.confidence == 1.0


def test_connection_add_evidence():
    conn = Connection(
        source_dataset="ds1",
        source_entity="http://ex.org/C1",
        target_dataset="ds2",
        target_entity="http://ex.org/C2",
        connection_type=ConnectionType.INSTANCE_IDENTITY,
        evidence_sources=[],
    )
    conn.add_evidence(EvidenceSource(source_type="uri_matching", evidence_count=5, confidence=0.9))
    assert len(conn.evidence_sources) == 1
    assert conn.total_evidence_count == 5


def test_schema_pattern():
    pattern = SchemaPattern(
        subject_class="http://ex.org/Person",
        property_uri="http://ex.org/name",
        object_class="Literal",
        datatype="http://www.w3.org/2001/XMLSchema#string",
        count=100,
    )
    assert pattern.count == 100
