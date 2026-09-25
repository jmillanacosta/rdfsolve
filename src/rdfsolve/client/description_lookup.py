"""Verify externally named classes in the selected data scope."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdflib import Literal

from rdfsolve.client.hydration import _iri
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.client.query_fragments import Fragment
from rdfsolve.schema_models.enrichment import RdfTerm

if TYPE_CHECKING:
    from rdfsolve.client.api import Client


def ontology_matches(
    client: Client,
    concept: str | Literal,
    rows: list[dict[str, Any]],
    coverage: dict[str, Any],
    candidates: list[str],
) -> list[dict[str, Any]]:
    """Keep external names, scoped literal checks and class-use witnesses."""
    from rdfsolve.client.description import literal_matches

    class_types = {
        "http://www.w3.org/2002/07/owl#Class",
        "http://www.w3.org/2000/01/rdf-schema#Class",
    }
    for row in rows:
        if row["Kind"] == "type":
            return []
        if (
            class_types.intersection(row.get("Types", []))
            and class_use(client, row["Resource"])["status"] == "observed_class"
        ):
            return []
    if client.ontology is None:
        client.ontology = OntologyLookup()
    lookup = client.ontology
    event: dict[str, Any] = {
        "strategy": "ontology_fallback",
        "concept": str(concept),
        "source_search": dict(coverage),
        "provider": lookup.provider,
        "candidates": [],
    }
    client.description_lookups.append(event)
    found: list[dict[str, Any]] = []
    event_start = len(lookup.events)
    for candidate in lookup.search(str(concept)):
        iri = candidate["iri"]
        if candidates and iri not in candidates:
            continue
        matches, label_check = literal_matches(client, concept, (), [iri])
        label_check["status"] = "found" if matches else "not_found_for_searched_literals"
        evidence = {
            **candidate,
            "label_origin": "external ontology",
            "provider": lookup.provider,
            "source_label": label_check,
            "source_use": class_use(client, iri),
        }
        event["candidates"].append(evidence)
        term = RdfTerm(kind="uri", value=iri)
        ref = client.catalogue._put(
            Fragment(
                "term",
                candidate.get("label", str(concept)),
                term=term,
                basis="external ontology label",
            ),
            ["ontology", lookup.provider, iri],
        )
        client.catalogue.metadata.setdefault(ref, {}).setdefault("ontology", []).append(evidence)
        found.append(
            {
                "Reference": ref,
                "Kind": "ontology",
                "Label": candidate.get("label"),
                "Resource": iri,
                "Types": [],
                "Ontology evidence": [evidence],
                "Basis": "external ontology label; " + evidence["source_use"]["status"],
            }
        )
    event["lookup"] = lookup.diagnostics()
    event["events"] = list(lookup.events[event_start:])
    return found


def class_use(client: Client, iri: str) -> dict[str, Any]:
    """Return one scoped class-use witness."""
    body = client._scope(client._subject_type("?instance", _iri(iri)))
    with client.step("Verify ontology class " + iri):
        used = client._select(f"SELECT ?instance WHERE {{ {body} }} LIMIT 1")
    return {
        "status": "observed_class" if used else "not_observed",
        "witnesses": used,
        "query_ids": list(client._steps[-1]["query_ids"]),
        "graph_uris": list(client.graph_uris),
        "type_context_graph_uris": list(client._schema.about.type_context_graph_uris or []),
        "type_graph_uris": list(client._schema.about.type_graph_uris or []),
    }
