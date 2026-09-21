from rdflib import Graph, Literal, Namespace, RDF, SH

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
        artifact_id="declared:test", dataset_id="demo", kind="shacl",
        retrieved_at="2026-09-21T00:00:00+00:00", sha256="0"*64,
        local_path="shapes.ttl", representation="source_bytes", retrieval_method="fixture",
    )
    declared = project_declared_evidence(g, artifact)
    patterns = [
        SchemaPattern(subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.Place)),
        SchemaPattern(subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.City)),
    ]
    rows = compare_observed_with_declared_shacl(dataset_id="demo", patterns=patterns, declared=declared)
    by_dim = {row.dimension: row for row in rows}
    assert by_dim["property"].relation == "both"
    assert by_dim["class"].relation == "declared_subset_of_observed"
    assert by_dim["node_kind"].relation == "equal"


def test_rdfs_semantics_are_not_folded_into_shacl_set_comparison():
    ex = Namespace("urn:ex:")
    g = Graph()
    from rdflib import RDFS
    g.add((ex.p, RDFS.range, ex.Place))
    artifact = DeclaredArtifact(
        artifact_id="declared:test", dataset_id="demo", kind="ontology",
        retrieved_at="2026-09-21T00:00:00+00:00", sha256="0"*64,
        local_path="onto.ttl", representation="source_bytes", retrieval_method="fixture",
    )
    declared = project_declared_evidence(g, artifact)
    patterns = [SchemaPattern(subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.City))]
    rows = compare_observed_with_declared_shacl(dataset_id="demo", patterns=patterns, declared=declared)
    assert len(rows) == 3  # property + observed class and node-kind profiles
    assert {row.dimension for row in rows} == {"property", "class", "node_kind"}
    assert all(row.relation == "observed_only" for row in rows)


def test_shacl_combined_node_kind_is_compared_as_allowed_set():
    ex = Namespace("urn:ex:")
    g = Graph()
    g.add((ex.Shape, RDF.type, SH.NodeShape))
    g.add((ex.Shape, SH.targetClass, ex.Person))
    g.add((ex.Shape, SH.property, ex.PS))
    g.add((ex.PS, SH.path, ex.p))
    g.add((ex.PS, SH.nodeKind, SH.IRIOrLiteral))
    artifact = DeclaredArtifact(
        artifact_id="declared:test", dataset_id="demo", kind="shacl",
        retrieved_at="2026-09-21T00:00:00+00:00", sha256="0"*64,
        local_path="shapes.ttl", representation="source_bytes", retrieval_method="fixture",
    )
    declared = project_declared_evidence(g, artifact)
    patterns = [
        SchemaPattern(subject_class=str(ex.Person), property_uri=str(ex.p), object_class=str(ex.Place)),
    ]
    rows = compare_observed_with_declared_shacl(dataset_id="demo", patterns=patterns, declared=declared)
    node_kind = next(row for row in rows if row.dimension == "node_kind")
    assert node_kind.observed_values == ["IRI"]
    assert node_kind.declared_values == ["IRI", "Literal"]
    assert node_kind.relation == "observed_subset_of_declared"
