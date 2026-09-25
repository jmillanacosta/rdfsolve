"""Extract selected source fields with their graph witnesses."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field
from rdflib import BNode, Dataset, Graph, URIRef
from rdflib.term import Node

from rdfsolve.client.hydration import HydrationLimitError, _iri, _term
from rdfsolve.mining.query_builders import _graph_scope, _subject_type_pattern, _type_pattern
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.selection import SchemaSelection

if TYPE_CHECKING:
    from rdfsolve.client.api import Client
    from rdfsolve.client.assessment import Inference, SelectionAssessment

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


class ExtractedQuad(BaseModel):
    """One unchanged source statement and its graph."""

    model_config = ConfigDict(extra="forbid")
    subject: RdfTerm
    predicate: str
    object: RdfTerm
    graph: str | None = None


class Extraction(BaseModel):
    """A selected RDF response with source evidence and response-scoped blank nodes."""

    model_config = ConfigDict(extra="forbid")
    selection: SchemaSelection
    root_class: str
    roots: list[RdfTerm]
    quads: list[ExtractedQuad]
    query: str
    response_scope: str = Field(default_factory=lambda: uuid4().hex)
    completeness: str = "Returned response; source and endpoint limits still apply"

    def to_dataset(self) -> Dataset:
        """Restore named graphs and isolate blank nodes from other responses."""
        result = Dataset()

        def node(term: RdfTerm) -> Node:
            """Restore a term within this response."""
            return (
                BNode(self.response_scope + "_" + term.value)
                if term.kind == "bnode"
                else term.to_rdf()
            )

        for quad in self.quads:
            graph = result.graph(URIRef(quad.graph)) if quad.graph else result.default_graph
            graph.add((node(quad.subject), URIRef(quad.predicate), node(quad.object)))
        return result

    def assess(
        self, shapes: Graph, *, ontology: Graph | None = None, inference: Inference = "none"
    ) -> SelectionAssessment:
        """Check supplied SHACL and optional ontology rules on the extracted RDF."""
        from rdfsolve.client.assessment import assess

        return assess(self, shapes, ontology, inference)

    def save(self, path: str | Path) -> None:
        """Write TriG with every retained graph."""
        self.to_dataset().serialize(destination=path, format="trig")


def extraction_query(
    selection: SchemaSelection, root_class: str, roots: list[str] | None, limit: int
) -> str:
    """Compile finite selected routes and fields into one graph-preserving response."""
    _iri(root_class)
    data = selection.source.about.graph_uris or []
    context = list(
        dict.fromkeys(
            (selection.source.about.type_graph_uris or [])
            + (selection.source.about.type_context_graph_uris or [])
        )
    )
    for graph in data + context:
        _iri(graph)
    dataset, _, _ = _graph_scope(data, context)
    fields = selection._fields()
    if not any(owner == root_class for owner, _ in fields):
        raise ValueError("The root class must own a selected field")
    if any(path.steps[0].subject_class != root_class for path in selection.paths):
        raise ValueError("Select paths starting at the chosen root class")
    reaches: dict[tuple[str, str, str], None] = {(root_class, "?root", ""): None}
    for route_index, path in enumerate(selection.paths):
        prefix = ""
        node = "?root"
        for step_index, step in enumerate(path.steps):
            target = f"?r{route_index}_{step_index}"
            prefix += f" {node} {_iri(step.property_uri)} {target} ."
            if step.object_class in {"Literal", "Resource", "BlankNode"}:
                break
            prefix += " " + _type_pattern(target, _iri(step.object_class), context)
            reaches[(step.object_class, target, prefix)] = None
            node = target
    for reach_index, (owner, node, prefix) in enumerate(list(reaches)):
        for profile_index, profile in enumerate(selection.collections or []):
            if profile.subject_class != owner:
                continue
            for member_index, member_class in enumerate(profile.member_types):
                if not any(cls == member_class for cls, _ in fields):
                    continue
                member = f"?member{reach_index}_{profile_index}_{member_index}"
                route = (
                    prefix
                    + f" {node} {_iri(profile.property_uri)}/<{RDF}rest>*/<{RDF}first> {member} . "
                    + _type_pattern(member, _iri(member_class), context)
                )
                reaches[(member_class, member, route)] = None
    unreachable = {owner for owner, _ in fields} - {owner for owner, _, _ in reaches}
    if unreachable:
        raise ValueError(f"Selected fields lack a path from the root: {sorted(unreachable)}")

    def edge(body: str, graphs: list[str], *, types: bool = False) -> str:
        """Bind the graph that supplied each statement."""
        if not graphs:
            return body
        named = f"VALUES ?graph {{ {' '.join(map(_iri, graphs))} }} GRAPH ?graph {{ {body} }}"
        return f"{{ {body} }} UNION {{ {named} }}" if types and not data else named

    branches = []
    for owner, node, prefix in reaches:
        branches.append(
            "{ "
            + prefix
            + " "
            + edge(f"{node} <{RDF}type> ?o .", list(dict.fromkeys(data + context)), types=True)
            + f" BIND({node} AS ?s) BIND(<{RDF}type> AS ?p) }}"
        )
        for _, predicate in sorted(field for field in fields if field[0] == owner):
            link = f"{node} {_iri(predicate)} ?value ."
            branches.append(
                "{ "
                + prefix
                + " "
                + edge(link, data)
                + f" BIND({node} AS ?s) BIND({_iri(predicate)} AS ?p) BIND(?value AS ?o) }}"
            )
            branches.append(
                "{ "
                + prefix
                + " "
                + link
                + " "
                + edge(f"?value <{RDF}type> ?o .", list(dict.fromkeys(data + context)), types=True)
                + f" BIND(?value AS ?s) BIND(<{RDF}type> AS ?p) }}"
            )
            if any(
                p.subject_class == owner and p.property_uri == predicate
                for p in selection.collections or []
            ):
                branches.append(
                    "{ "
                    + prefix
                    + " "
                    + link
                    + f" ?value <{RDF}rest>* ?s . "
                    + edge(f"VALUES ?p {{ <{RDF}first> <{RDF}rest> }} ?s ?p ?o .", data)
                    + " }"
                )
                branches.append(
                    "{ "
                    + prefix
                    + " "
                    + link
                    + f" ?value <{RDF}rest>*/<{RDF}first> ?s . "
                    + edge(f"?s <{RDF}type> ?o .", list(dict.fromkeys(data + context)), types=True)
                    + f" BIND(<{RDF}type> AS ?p) }}"
                )
    anchor = _subject_type_pattern("?root", _iri(root_class), context)
    if roots is not None:
        anchor = (
            f"VALUES ?root {{ {' '.join(_iri(value) for value in roots)} }} " + anchor
            if roots
            else "FILTER(1 = 0) " + anchor
        )
    branches = ["{ " + anchor + " " + branch[2:] for branch in branches]
    return (
        f"SELECT DISTINCT ?root ?s ?p ?o ?graph {dataset} WHERE {{ {anchor} "
        + " OPTIONAL { "
        + " UNION ".join(branches)
        + f" }} }} LIMIT {limit + 1}"
    )


def extract(
    client: Client, selection: SchemaSelection, root_class: str, roots: list[str] | None
) -> Extraction:
    """Execute one selection without following response-scoped blank nodes later."""
    if selection.source.to_dict() != client._schema.to_dict():
        raise ValueError("Use a selection from this client's schema snapshot")
    if set(client.graph_uris) != set(selection.source.about.graph_uris or []):
        raise ValueError("Extraction requires the selection's data scope")
    query = extraction_query(selection, root_class, roots, client.max_rows)
    rows = client._select(query)
    if len(rows) > client.max_rows:
        raise HydrationLimitError(
            "Extraction exceeds max_rows; select fewer roots or raise max_rows"
        )
    found_roots: dict[str, RdfTerm] = {}
    quads: dict[str, ExtractedQuad] = {}
    for row in rows:
        root = _term(row["root"])
        found_roots[root.model_dump_json()] = root
        if "s" not in row:
            continue
        quad = ExtractedQuad(
            subject=_term(row["s"]),
            predicate=row["p"]["value"],
            object=_term(row["o"]),
            graph=row.get("graph", {}).get("value"),
        )
        quads[quad.model_dump_json()] = quad
    return Extraction(
        selection=selection.model_copy(deep=True),
        root_class=root_class,
        roots=[found_roots[key] for key in sorted(found_roots)],
        quads=[quads[key] for key in sorted(quads)],
        query=query,
    )
