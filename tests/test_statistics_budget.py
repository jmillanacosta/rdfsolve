"""The distinct counts of each property stop after a time budget (PubChem: 29.8e9 triples, a
72 h job). The properties are counted from the smallest; the rest keep their triples and are
recorded as not counted."""

from rdfsolve.mining import dataset_statistics
from tests.test_dataset_statistics import mine


def test_properties_after_the_time_budget_keep_their_triples(monkeypatch):
    monkeypatch.setattr(dataset_statistics, "PARTITION_BUDGET_S", -1.0)
    schema, record = mine(monkeypatch)
    assert record["state"] == "counted" and schema.about.distinct_subject_count == 3
    assert schema.about.property_partitions["urn:p"] == {"triples": 3}
    assert "time budget" in record["refused_partitions"]["urn:p"]
