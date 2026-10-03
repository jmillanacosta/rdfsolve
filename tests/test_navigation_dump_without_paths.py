"""A summary of tested paths can be written without its paths (the client describes a schema
with model_dump(exclude={"paths"})). The compact writer of the paths failed with KeyError
'paths' there, and every link of rehearsal attempt 14 failed with it (job 114537, 2026-10-01)."""

from rdfsolve.schema_models import SchemaPattern
from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary

AB = SchemaPattern(subject_class="urn:A", property_uri="urn:p", object_class="urn:B")
BC = SchemaPattern(subject_class="urn:B", property_uri="urn:q", object_class="urn:C")


def test_tested_paths_can_be_left_out_of_a_dump():
    summary = NavigationSummary(
        max_hops=2, max_paths_per_length=0, edge_count=2, walk_counts={2: 1}, strategy="tested",
        paths=[NavigationPath(steps=[AB, BC], evidence="instance_tested", instance_support="matched")],
    )
    written = summary.model_dump(mode="json", exclude={"paths"})
    assert "paths" not in written and written["strategy"] == "tested"
    assert summary.model_dump(mode="json")["paths"]["format"] == "edges-1", "The full dump is compact"
