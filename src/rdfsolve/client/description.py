"""Describe schema entries and exact source literal matches."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from rdflib import Literal

from rdfsolve.client.hydration import _iri, _term
from rdfsolve.client.query_fragments import Fragment
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql

if TYPE_CHECKING:
    from rdfsolve.client.api import Client


COLUMNS = [
    "Reference",
    "Kind",
    "Label",
    "Class",
    "Field",
    "Path",
    "Targets",
    "Description",
    "Ontology evidence",
    "Resource",
    "Types",
    "Predicate",
    "Literal",
    "Graph",
    "Query",
    "Basis",
]


def describe(
    client: Client,
    concept: str | Literal,
    owners: tuple[str, ...],
    targets: tuple[str, ...],
    source: bool,
) -> pd.DataFrame:
    """Combine schema matches with scoped literal evidence and ontology candidates."""
    if concept and (not concept.strip() or len(concept) > 200):
        raise ValueError("Use a nonempty phrase of at most 200 characters")
    index = client.catalogue
    rows: list[dict[str, Any]] = [
        {
            "Reference": ref,
            "Kind": f.kind,
            "Label": f.label,
            "Class": f.iri or f.owner,
            "Field": f.field_name,
            "Path": path_to_sparql(f.path) if f.path else None,
            "Targets": index.metadata[ref].get("targets", []),
            "Description": f.description,
            "Ontology evidence": index.metadata[ref].get("ontology", []),
            "Basis": "schema",
        }
        for ref in index.search(str(concept), owners=owners, targets=targets)
        for f in [index.fragments[ref]]
    ]
    coverage: dict[str, Any] = {"status": "not_requested", "basis": "schema"}
    if (isinstance(concept, Literal) or concept) and source and not targets:
        matches, coverage = literal_matches(client, concept, owners)
        rows.extend(matches)
        if client.ontology:
            for candidate in client.ontology.search(str(concept)):
                term = RdfTerm(kind="uri", value=candidate["iri"])
                ref = index._put(
                    Fragment(
                        "term",
                        candidate.get("label", concept),
                        term=term,
                        basis="external ontology candidate",
                    ),
                    ["ontology", client.ontology.provider, term.value],
                )
                index.metadata[ref] = {"ontology": [candidate]}
                rows.append(
                    {
                        "Reference": ref,
                        "Kind": "ontology",
                        "Label": candidate.get("label"),
                        "Resource": term.value,
                        "Types": [],
                        "Ontology evidence": [candidate],
                        "Basis": "external ontology candidate; source membership unverified",
                    }
                )
            coverage["ontology"] = client.ontology.diagnostics()
    table = pd.DataFrame(rows, columns=COLUMNS)
    table.attrs["coverage"] = coverage
    return table


def literal_matches(
    client: Client, concept: str | Literal, owners: tuple[str, ...]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Find literal subjects without requiring a generated model."""
    terms = (
        [concept]
        if isinstance(concept, Literal)
        else [
            Literal(value)
            for value in dict.fromkeys(
                [
                    concept.strip(),
                    concept.strip().lower(),
                    concept.strip().upper(),
                    concept.strip().title(),
                ]
            )
        ]
    )
    match = " UNION ".join(
        f"{{ ?s ?p {term.n3()} . BIND({term.n3()} AS ?text) }}" for term in terms
    )
    match = "{ " + match + " }"
    typing = "OPTIONAL { " + client._type_pattern("?s", "?type") + " }"
    if owners:
        classes = ", ".join(_iri(client.catalogue._type(owner)) for owner in owners)
        typing += f" FILTER(?type IN ({classes}))"
    if len(client.graph_uris) > 1:
        graphs = " ".join(_iri(graph) for graph in client.graph_uris)
        body = f"VALUES ?_graph {{ {graphs} }} GRAPH ?_graph {{ {match} }} {typing}"
    else:
        body = client._scope(match + " " + typing)
    with client.step("Describe literal " + str(concept)):
        bindings = client._select(
            f"SELECT DISTINCT ?s ?p ?text ?type ?_graph WHERE {{ {body} }} "
            f"LIMIT {client.max_rows + 1}"
        )
    partial = len(bindings) > client.max_rows
    query_id = len(client._records())
    matches: dict[tuple[str, str, str, str | None], dict[str, Any]] = {}
    subjects: set[str] = set()
    for binding in bindings[: client.max_rows]:
        subject, text = _term(binding["s"]), _term(binding["text"])
        if subject.value not in subjects and len(subjects) >= client.max_subjects:
            partial = True
            continue
        subjects.add(subject.value)
        predicate = binding["p"]["value"]
        graph = binding.get("_graph", {}).get("value")
        key = (subject.value, predicate, text.to_rdf().n3(), graph)
        ref = client.catalogue._put(
            Fragment("term", text.value, term=subject, basis="source literal match"),
            [
                "literal subject",
                subject.kind,
                subject.value,
                query_id if subject.kind == "bnode" else None,
            ],
        )
        row = matches.setdefault(
            key,
            {
                "Reference": ref,
                "Kind": "resource",
                "Label": text.value,
                "Resource": subject.value,
                "Types": [],
                "Predicate": predicate,
                "Literal": text.model_dump(mode="json"),
                "Graph": graph,
                "Query": query_id,
                "Basis": "source literal match",
            },
        )
        if "type" in binding and binding["type"]["value"] not in row["Types"]:
            row["Types"].append(binding["type"]["value"])
        metadata = client.catalogue.metadata.setdefault(ref, {"literal_matches": []})
        if row not in metadata["literal_matches"]:
            metadata["literal_matches"].append(row)
    return list(matches.values()), {
        "status": "partial" if partial else "complete",
        "basis": "exact RDF literals in selected data scope",
        "searched_literals": [RdfTerm.from_rdf(term).model_dump(mode="json") for term in terms],
        "query_ids": client._steps[-1]["query_ids"],
        "graph_uris": list(client.graph_uris),
    }
