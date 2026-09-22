from rdflib import OWL, RDF, RDFS, SH, Graph, Namespace
from rdfsolve.evidence.declared import DeclaredArtifact, project_declared_evidence


def _artifact() -> DeclaredArtifact:
    return DeclaredArtifact(
        artifact_id="declared:test",
        dataset_id="demo",
        kind="shacl",
        retrieved_at="2026-09-21T00:00:00+00:00",
        sha256="0" * 64,
        local_path="shapes.ttl",
        representation="source_bytes",
        retrieval_method="fixture",
    )


def test_declared_projection_keeps_semantic_types_distinct():
    ex = Namespace("urn:ex:")
    g = Graph()
    g.add((ex.p, RDFS.domain, ex.Person))
    g.add((ex.p, RDFS.range, ex.Place))
    g.add((ex.City, RDFS.subClassOf, ex.Place))
    g.add((ex.Town, OWL.equivalentClass, ex.City))
    shape = ex.Shape
    prop = ex.PropertyShape
    g.add((shape, RDF.type, SH.NodeShape))
    g.add((shape, SH.targetClass, ex.Person))
    g.add((shape, SH.property, prop))
    g.add((prop, SH.path, ex.p))
    g.add((prop, SH["class"], ex.Place))
    g.add((prop, SH.minCount, __import__("rdflib").Literal(1)))
    g.add((shape, SH.targetNode, ex.alice))
    rows = project_declared_evidence(g, _artifact())
    kinds = {row.declaration_type for row in rows}
    assert {
        "rdfs_domain",
        "rdfs_range",
        "subclass",
        "equivalent_class",
        "shacl_class",
        "shacl_min_count",
    } <= kinds
    assert all((row.artifact_id == "declared:test" for row in rows))
    assert (shape, SH.targetNode, ex.alice) in g
