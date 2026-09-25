"""Verify externally named classes in the selected data scope."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdflib import Literal

from rdfsolve.client.catalogue import same_name
from rdfsolve.client.hydration import _iri
from rdfsolve.client.ontology import OntologyLookup
from rdfsolve.client.query_fragments import Fragment
from rdfsolve.schema_models.enrichment import RdfTerm

CLASS_TYPES = {
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2000/01/rdf-schema#Class",
}

if TYPE_CHECKING:
    from rdfsolve.client.api import Client


def ontology_matches(
    client: Client,
    concept: str | Literal,
    rows: list[dict[str, Any]],
    coverage: dict[str, Any],
    candidates: list[str],
) -> list[dict[str, Any]]:
    """Consult external names unless the source already uses a class with this exact name."""
    for row in rows:
        named = same_name(str(row.get("Label") or ""), str(concept))
        iri = (
            row.get("Class")
            if row["Kind"] == "type"
            else row["Resource"]
            if CLASS_TYPES.intersection(row.get("Types") or [])
            else None
        )
        if named and iri and class_use(client, iri)["status"] == "observed_class":
            return []
    event = external_candidates(client, concept, coverage, candidates, "ontology_fallback")
    found: list[dict[str, Any]] = []
    for evidence in event["candidates"]:
        iri = evidence["iri"]
        ref = client.catalogue._put(
            Fragment(
                "term",
                evidence.get("label") or str(concept),
                term=RdfTerm(kind="uri", value=iri),
                basis="external ontology label",
            ),
            ["ontology", evidence["provider"], iri],
        )
        client.catalogue.metadata.setdefault(ref, {}).setdefault("ontology", []).append(evidence)
        found.append(
            {
                "Reference": ref,
                "Kind": "ontology",
                "Label": evidence.get("label"),
                "Resource": iri,
                "Types": [],
                "Ontology evidence": [evidence],
                "Basis": "external ontology label; " + evidence["source_use"]["status"],
            }
        )
    return found


def external_candidates(
    client: Client,
    concept: str | Literal,
    coverage: dict[str, Any],
    candidates: list[str],
    strategy: str = "external ontology names",
) -> dict[str, Any]:
    """Look up external class names and check each in the selected scope; retain the event."""
    from rdfsolve.client.description import literal_matches

    lookup = client.ontology or OntologyLookup()
    event: dict[str, Any] = {
        "strategy": strategy,
        "concept": str(concept),
        "source_search": dict(coverage),
        "provider": lookup.provider,
        "candidates": [],
    }
    client.description_lookups.append(event)
    event_start = len(lookup.events)
    try:
        for candidate in lookup.search(str(concept)):
            iri = candidate["iri"]
            if candidates and iri not in candidates:
                continue
            matches, label_check = literal_matches(client, concept, (), [iri])
            label_check["status"] = "found" if matches else "not_found_for_searched_literals"
            event["candidates"].append(
                {
                    **candidate,
                    "label_origin": "external ontology",
                    "provider": lookup.provider,
                    "source_label": label_check,
                    "source_use": class_use(client, iri),
                }
            )
    finally:
        event["lookup"] = lookup.diagnostics()
        event["events"] = list(lookup.events[event_start:])
        if lookup is not client.ontology:
            lookup.close()
    return event


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
