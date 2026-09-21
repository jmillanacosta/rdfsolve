"""Provider-declared RDF artifacts and a deliberately small comparable projection.

The archived RDF artifact remains the source of truth.  ``DeclaredEvidence`` is
only a normalized view of statements for which rdfsolve defines comparison
semantics.  Unsupported RDF is never discarded merely because it is not
projected.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from rdflib import OWL, RDF, RDFS, SH, BNode, Graph, URIRef
from rdflib import Literal as RdfLiteral

from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path

DeclaredArtifactKind = Literal[
    "void",
    "shacl",
    "ontology",
    "service_description",
    "sparql_examples",
    "other_rdf",
]
ArtifactRepresentation = Literal["source_bytes", "constructed_graph", "serialized_graph"]
ParseStatus = Literal["parsed", "parse_error", "not_parsed"]


class DeclaredArtifact(BaseModel):
    """One provider-origin RDF artifact retained independently of mined evidence."""

    artifact_id: str
    dataset_id: str
    kind: DeclaredArtifactKind
    source_url: str | None = None
    source_graph: str | None = None
    retrieved_at: str
    sha256: str
    media_type: str | None = None
    local_path: str
    representation: ArtifactRepresentation
    parse_status: ParseStatus = "parsed"
    complete: bool | None = None
    retrieval_method: str
    query_ids: list[str] = Field(default_factory=list)


DeclarationType = Literal[
    "rdfs_domain",
    "rdfs_range",
    "subclass",
    "equivalent_class",
    "equivalent_property",
    "inverse_property",
    "disjoint_class",
    "shacl_class",
    "shacl_datatype",
    "shacl_node_kind",
    "shacl_min_count",
    "shacl_max_count",
]


class DeclaredEvidence(BaseModel):
    """Comparable projection of one statement from a provider artifact."""

    evidence_id: str
    dataset_id: str
    artifact_id: str
    declaration_type: DeclarationType
    focus_class: str | None = None
    property_uri: str | None = None
    path: PropertyPath | None = None
    value: RdfTerm
    source_subject: RdfTerm
    source_graph: str | None = None


def _term(value: object) -> RdfTerm:
    if isinstance(value, URIRef):
        return RdfTerm(kind="uri", value=str(value))
    if isinstance(value, BNode):
        return RdfTerm(kind="bnode", value=str(value))
    if isinstance(value, RdfLiteral):
        return RdfTerm(
            kind="literal",
            value=str(value),
            datatype=str(value.datatype) if value.datatype else None,
            language=value.language,
        )
    raise TypeError(f"Unsupported RDF term: {type(value).__name__}")


def _evidence_id(
    artifact_id: str,
    declaration_type: str,
    subject: object,
    predicate: object,
    value: object,
) -> str:
    key = "\n".join(map(str, (artifact_id, declaration_type, subject, predicate, value)))
    return "declared:" + sha256(key.encode()).hexdigest()


def _record(
    *,
    dataset_id: str,
    artifact: DeclaredArtifact,
    declaration_type: DeclarationType,
    subject: object,
    predicate: object,
    value: object,
    focus_class: str | None = None,
    property_uri: str | None = None,
    path: PropertyPath | None = None,
) -> DeclaredEvidence:
    return DeclaredEvidence(
        evidence_id=_evidence_id(artifact.artifact_id, declaration_type, subject, predicate, value),
        dataset_id=dataset_id,
        artifact_id=artifact.artifact_id,
        declaration_type=declaration_type,
        focus_class=focus_class,
        property_uri=property_uri,
        path=path,
        value=_term(value),
        source_subject=_term(subject),
        source_graph=artifact.source_graph,
    )


def project_declared_evidence(
    graph: Graph,
    artifact: DeclaredArtifact,
) -> list[DeclaredEvidence]:
    """Project a documented subset of RDFS/OWL/SHACL into comparable records."""
    out: list[DeclaredEvidence] = []
    direct = {
        RDFS.domain: "rdfs_domain",
        RDFS.range: "rdfs_range",
        RDFS.subClassOf: "subclass",
        OWL.equivalentClass: "equivalent_class",
        OWL.equivalentProperty: "equivalent_property",
        OWL.inverseOf: "inverse_property",
        OWL.disjointWith: "disjoint_class",
    }
    for predicate, declaration_type in direct.items():
        for subject, value in graph.subject_objects(predicate):
            if not isinstance(subject, (URIRef, BNode)):
                continue
            if not isinstance(value, (URIRef, BNode, RdfLiteral)):
                continue
            out.append(
                _record(
                    dataset_id=artifact.dataset_id,
                    artifact=artifact,
                    declaration_type=declaration_type,  # type: ignore[arg-type]
                    subject=subject,
                    predicate=predicate,
                    value=value,
                    property_uri=str(subject)
                    if predicate in {RDFS.domain, RDFS.range, OWL.inverseOf, OWL.equivalentProperty}
                    else None,
                )
            )

    # SHACL is projected only when a target class and a property shape can be
    # connected. Compound paths are retained in the raw artifact; read_path is
    # used where representable by rdfsolve's path model.
    constraints = {
        SH["class"]: "shacl_class",
        SH.datatype: "shacl_datatype",
        SH.nodeKind: "shacl_node_kind",
        SH.minCount: "shacl_min_count",
        SH.maxCount: "shacl_max_count",
    }
    for shape, target_class in graph.subject_objects(SH.targetClass):
        if not isinstance(target_class, URIRef):
            continue
        for prop_shape in graph.objects(shape, SH.property):
            path_node = graph.value(prop_shape, SH.path)
            if path_node is None:
                continue
            try:
                path = read_path(graph, path_node)
            except Exception:
                continue
            property_uri = path.iri if path.operator == "predicate" else None
            for predicate, declaration_type in constraints.items():
                for value in graph.objects(prop_shape, predicate):
                    out.append(
                        _record(
                            dataset_id=artifact.dataset_id,
                            artifact=artifact,
                            declaration_type=declaration_type,  # type: ignore[arg-type]
                            subject=prop_shape,
                            predicate=predicate,
                            value=value,
                            focus_class=str(target_class),
                            property_uri=property_uri,
                            path=path,
                        )
                    )
    out.sort(key=lambda row: row.evidence_id)
    return out


def archive_graph_artifact(
    *,
    graph: Graph,
    dataset_id: str,
    kind: DeclaredArtifactKind,
    output_path: str | Path,
    source_url: str | None = None,
    source_graph: str | None = None,
    retrieval_method: str,
    complete: bool | None = None,
) -> DeclaredArtifact:
    """Serialize a retrieved graph and describe the resulting snapshot artifact."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = graph.serialize(format="turtle").encode()
    path.write_bytes(payload)
    digest = sha256(payload).hexdigest()
    return DeclaredArtifact(
        artifact_id=f"declared:{digest}",
        dataset_id=dataset_id,
        kind=kind,
        source_url=source_url,
        source_graph=source_graph,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        sha256=digest,
        media_type="text/turtle",
        local_path=str(path),
        representation="constructed_graph" if source_graph else "serialized_graph",
        parse_status="parsed",
        complete=complete,
        retrieval_method=retrieval_method,
    )


__all__ = [
    "DeclaredArtifact",
    "DeclaredEvidence",
    "archive_graph_artifact",
    "project_declared_evidence",
]
