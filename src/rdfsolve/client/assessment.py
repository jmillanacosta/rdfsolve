"""Assess selected RDF against supplied declarations without changing either."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from importlib.metadata import version
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field
from rdflib import RDF, RDFS, SH, Graph, URIRef

from rdfsolve.analysis.declared_comparison import (
    EvidenceComparison,
    compare_observed_with_declared_shacl,
)
from rdfsolve.evidence.declared import DeclaredArtifact, project_declared_evidence
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.navigation import NavigationPath

if TYPE_CHECKING:
    from rdfsolve.client.extraction import Extraction

Inference = Literal["none", "rdfs", "owlrl", "both"]


class ConstraintViolation(BaseModel):
    """A validator result with its actual RDF terms."""

    focus: RdfTerm | None = None
    path: RdfTerm | None = None
    value: RdfTerm | None = None
    shape: RdfTerm | None = None
    component: str | None = None
    messages: list[str] = Field(default_factory=list)


class SelectionAssessment(BaseModel):
    """Separate selected-data conformance, source evidence and ontology diagnostics."""

    state: Literal["conforms", "violations", "not_checked", "error"] = "not_checked"
    conforms: bool | None = None
    source_conforms: bool | None = None
    selection_rows: int
    active_shapes: int = 0
    deactivated_shapes: int = 0
    violations: list[ConstraintViolation] = Field(default_factory=list)
    scope_warnings: list[str] = Field(default_factory=list)
    comparisons: list[EvidenceComparison] = Field(default_factory=list)
    retained_paths: list[NavigationPath] = Field(default_factory=list)
    inference: Inference = "none"
    ontology_consistency: str = "not_checked"
    ontology_errors: list[str] = Field(default_factory=list)
    engine: dict[str, str] = Field(default_factory=dict)
    shapes_turtle: str
    ontology_turtle: str | None = None
    report_turtle: str = ""
    message: str = ""


def assess(
    extraction: Extraction, shapes: Graph, ontology: Graph | None, inference: Inference
) -> SelectionAssessment:
    """Check the returned RDF union using external SHACL and optional OWL-RL rules."""
    if inference not in {"none", "rdfs", "owlrl", "both"}:
        raise ValueError("Choose none, rdfs, owlrl or both")
    copied_shapes = Graph()
    copied_shapes += shapes
    shapes = copied_shapes
    if ontology is not None:
        copied_ontology = Graph()
        copied_ontology += ontology
        ontology = copied_ontology
    selection = extraction.selection
    shape_text = shapes.serialize(format="turtle")
    result = SelectionAssessment(
        selection_rows=len(selection.patterns),
        shapes_turtle=shape_text,
        ontology_turtle=ontology.serialize(format="turtle") if ontology is not None else None,
        retained_paths=selection.paths,
        inference=inference,
    )
    roots = {
        subject
        for predicate in (SH.targetClass, SH.targetNode, SH.targetSubjectsOf, SH.targetObjectsOf)
        for subject in shapes.subjects(predicate)
    } | {s for s in shapes.subjects(RDF.type, SH.NodeShape) if (s, RDF.type, RDFS.Class) in shapes}
    inactive = {s for s in roots if bool(shapes.value(s, SH.deactivated))}
    result.active_shapes = len(roots - inactive)
    result.deactivated_shapes = len(inactive)
    fields = selection._fields()
    for shape in roots - inactive:
        if bool(shapes.value(shape, SH.closed)):
            result.scope_warnings.append(
                f"{shape}: closed-shape conformance covers only extracted properties"
            )
        for cls in shapes.objects(shape, SH.targetClass):
            for prop in shapes.objects(shape, SH.property):
                path = shapes.value(prop, SH.path)
                if not isinstance(path, URIRef) or (str(cls), str(path)) not in fields:
                    result.scope_warnings.append(
                        f"{shape}: constraint path {path} is outside selected simple fields"
                    )
    digest = sha256(shape_text.encode()).hexdigest()
    dataset = selection.source.about.dataset_name or "selection"
    artifact = DeclaredArtifact(
        artifact_id="declared:" + digest,
        dataset_id=dataset,
        kind="shacl",
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        sha256=digest,
        local_path="",
        representation="serialized_graph",
        retrieval_method="supplied_graph",
    )
    result.comparisons = compare_observed_with_declared_shacl(
        dataset_id=dataset,
        patterns=selection.patterns,
        declared=project_declared_evidence(shapes, artifact),
    )
    data = Graph()
    for s, p, o, _ in extraction.to_dataset().quads((None, None, None, None)):
        data.add((s, p, o))
    try:
        if inference in {"owlrl", "both"}:
            from owlrl import OWLRL_Semantics  # type: ignore[import-untyped]

            closure = Graph()
            closure += data
            if ontology is not None:
                closure += ontology
            rules = OWLRL_Semantics(closure, axioms=False, daxioms=False, rdfs=inference == "both")
            rules.closure()
            result.ontology_errors = list(rules.error_messages)
            result.ontology_consistency = (
                "contradiction_reported"
                if result.ontology_errors
                else "no_owlrl_contradiction_reported"
            )
            result.engine["owlrl"] = version("owlrl")
        if not result.active_shapes:
            result.message = "No active targeted shapes; conformance was not checked"
            return result
        from pyshacl import validate

        result.engine["pyshacl"] = version("pyshacl")
        conforms, report, message = validate(
            data,
            shacl_graph=shapes,
            ont_graph=ontology,
            inference=inference,
            inplace=False,
            do_owl_imports=False,
            advanced=False,
            js=False,
        )
        if not isinstance(report, Graph):
            raise ValueError(str(report))
        result.conforms = bool(conforms)
        result.state = "conforms" if conforms else "violations"
        result.message = str(message)
        result.report_turtle = report.serialize(format="turtle")
        for item in report.subjects(RDF.type, SH.ValidationResult):
            values = {
                name: RdfTerm.from_rdf(term) if term is not None else None
                for name, predicate in (
                    ("focus", SH.focusNode),
                    ("path", SH.resultPath),
                    ("value", SH.value),
                    ("shape", SH.sourceShape),
                )
                for term in [report.value(item, predicate)]
            }
            result.violations.append(
                ConstraintViolation(
                    **values,
                    component=str(report.value(item, SH.sourceConstraintComponent) or ""),
                    messages=[str(m) for m in report.objects(item, SH.resultMessage)],
                )
            )
    except ImportError as error:
        raise ImportError("Install rdfsolve[validation] to assess supplied constraints") from error
    except Exception as error:
        result.state = "error"
        result.conforms = None
        result.message = f"{type(error).__name__}: {error}"
    return result
