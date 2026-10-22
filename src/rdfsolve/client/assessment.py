"""Assess selected RDF against supplied declarations without changing either."""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from hashlib import sha256
from importlib.metadata import version
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field
from rdflib import RDF, RDFS, SH, Graph, Node, URIRef

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
    focus_nodes: int = 0
    focus_counts: dict[str, int] = Field(default_factory=dict)
    violations: list[ConstraintViolation] = Field(default_factory=list)
    scope_warnings: list[str] = Field(default_factory=list)
    comparisons: list[EvidenceComparison] = Field(default_factory=list)
    retained_paths: list[NavigationPath] = Field(default_factory=list)
    inference: Inference = "none"
    ontology_consistency: str = "not_checked"
    ontology_errors: list[str] = Field(default_factory=list)
    engine: dict[str, str] = Field(default_factory=dict)
    # The declarations checked, by digest (sorted N-Triples); their Turtle only on request.
    shapes_sha256: str = ""
    ontology_sha256: str | None = None
    shapes_turtle: str | None = None
    ontology_turtle: str | None = None
    report_turtle: str = ""
    message: str = ""


def _digest(graph: Graph) -> str:
    """Return the sha256 of a graph's statements as sorted N-Triples lines."""
    lines = sorted(" ".join(term.n3() for term in triple) for triple in graph)
    return sha256("\n".join(lines).encode()).hexdigest()


def _instances(data: Graph, cls: Node) -> set[Node]:
    """Return the SHACL instances of a class in the data: typed with it or a subclass of it."""
    classes, frontier = {cls}, [cls]
    while frontier:
        for sub in data.subjects(RDFS.subClassOf, frontier.pop()):
            if sub not in classes:
                classes.add(sub)
                frontier.append(sub)
    return {s for c in classes for s in data.subjects(RDF.type, c)}


def _focus_nodes(data: Graph, shapes: Graph, shape: Node) -> set[Node]:
    """Return the focus nodes of a shape by its targets (SHACL 2.1.3), read from the data."""
    nodes: set[Node] = set(shapes.objects(shape, SH.targetNode))
    for cls in shapes.objects(shape, SH.targetClass):
        nodes |= _instances(data, cls)
    if (shape, RDF.type, RDFS.Class) in shapes:
        nodes |= _instances(data, shape)
    for prop in shapes.objects(shape, SH.targetSubjectsOf):
        nodes |= set(data.subjects(prop, None))
    for prop in shapes.objects(shape, SH.targetObjectsOf):
        nodes |= set(data.objects(None, prop))
    for target in shapes.objects(shape, SH.target):
        select = shapes.value(target, SH.select)
        if select is not None:  # a SPARQL-based target (SHACL-AF, with advanced)
            nodes |= {row[0] for row in data.query(str(select)) if isinstance(row, tuple)}
    return nodes


def assess(
    extraction: Extraction,
    shapes: Graph,
    ontology: Graph | None,
    inference: Inference,
    *,
    advanced: bool = False,
    compare: bool = True,
    keep_turtle: bool = False,
) -> SelectionAssessment:
    """Check the returned RDF union using external SHACL and optional OWL-RL rules.

    The declarations are read, not copied or changed; the result names them by digest, and
    keeps their Turtle with *keep_turtle*. *advanced* runs SHACL-AF (SPARQL-based constraints,
    targets and rules). *compare* also compares the selection's observed rows with the
    declared shapes (comparisons).
    """
    if inference not in {"none", "rdfs", "owlrl", "both"}:
        raise ValueError("Choose none, rdfs, owlrl or both")
    # The validator adds statements to the shapes graph: it reads a copy (the shapes are small;
    # the ontology is only read, and mixed into the data graph).
    copied = Graph()
    copied += shapes
    shapes = copied
    selection = extraction.selection
    result = SelectionAssessment(
        selection_rows=len(selection.patterns),
        shapes_sha256=_digest(shapes),
        ontology_sha256=_digest(ontology) if ontology is not None else None,
        shapes_turtle=shapes.serialize(format="turtle") if keep_turtle else None,
        ontology_turtle=(
            ontology.serialize(format="turtle") if keep_turtle and ontology is not None else None
        ),
        retained_paths=selection.paths,
        inference=inference,
    )
    roots = {
        subject
        for predicate in (
            SH.targetClass,
            SH.targetNode,
            SH.targetSubjectsOf,
            SH.targetObjectsOf,
            *((SH.target,) if advanced else ()),
        )
        for subject in shapes.subjects(predicate)
    } | {s for s in shapes.subjects(RDF.type, SH.NodeShape) if (s, RDF.type, RDFS.Class) in shapes}
    inactive = {s for s in roots if bool(shapes.value(s, SH.deactivated))}
    result.active_shapes = len(roots - inactive)
    result.deactivated_shapes = len(inactive)
    fields = selection._fields()
    digest = result.shapes_sha256
    dataset = selection.source.about.dataset_name or "selection"
    if compare:
        artifact = DeclaredArtifact(
            artifact_id="declared:" + digest,
            dataset_id=dataset,
            kind="shacl",
            retrieved_at=datetime.now(UTC).isoformat(),
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
            inplace=True,
            do_owl_imports=False,
            advanced=advanced,
            js=False,
        )
        if not isinstance(report, Graph):
            raise ValueError(str(report))
        # The data graph now holds the ontology and the inferred statements (inplace).
        focuses = {str(shape): _focus_nodes(data, shapes, shape) for shape in roots - inactive}
        result.focus_counts = {shape: len(nodes) for shape, nodes in focuses.items()}
        result.focus_nodes = len(set().union(*focuses.values()))
        for shape in roots - inactive:
            if not result.focus_counts.get(str(shape)):
                continue
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
        if not result.focus_nodes:
            result.message = "No focus nodes matched the active shapes; conformance was not checked"
            result.report_turtle = report.serialize(format="turtle")
            return result
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
