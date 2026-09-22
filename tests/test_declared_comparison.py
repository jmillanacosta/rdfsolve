from rdflib import RDF, SH, Graph, Namespace
from rdfsolve.analysis.declared_comparison import compare_observed_with_declared_shacl
from rdfsolve.evidence.declared import DeclaredArtifact, project_declared_evidence
from rdfsolve.schema_models.pattern import SchemaPattern


def test_declared_comparison_reports_neutral_set_relationships():
    ex = Namespace("urn:ex:")
    g = Graph()
    g.add((ex.Shape, RDF.type, SH.NodeShape))
    g.add((ex.Shape, SH.targetClass, ex.Person))
    g.add((ex.Shape, SH.property, ex.PS))
    g.add((ex.PS, SH.path, ex.p))
    g.add((ex.PS, SH["class"], ex.Place))
    g.add((ex.PS, SH.nodeKind, SH.IRI))
    artifact = DeclaredArtifact(
        artifact_id="declared:test",
        dataset_id="demo",
        kind="shacl",
        retrieved_at="2026-09-21T00:00:00+00:00",
        sha256="0" * 64,
        local_path="shapes.ttl",
        representation="source_bytes",
        retrieval_method="fixture",
    )
    declared = project_declared_evidence(g, artifact)
    patterns = [
        SchemaPattern(
            subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.Place)
        ),
        SchemaPattern(
            subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.City)
        ),
    ]
    rows = compare_observed_with_declared_shacl(
        dataset_id="demo", patterns=patterns, declared=declared
    )
    by_dim = {row.dimension: row for row in rows}
    assert by_dim["property"].relation == "both"
    assert by_dim["class"].relation == "declared_subset_of_observed"
    assert by_dim["node_kind"].relation == "equal"
