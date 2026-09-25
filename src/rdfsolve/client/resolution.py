"""Resolve query names with scoped source evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rdfsolve.client.catalogue import words
from rdfsolve.client.description import description_resource
from rdfsolve.client.description_lookup import class_use
from rdfsolve.client.hydration import _iri
from rdfsolve.client.query_fragments import Fragment
from rdfsolve.schema_models.enrichment import RdfTerm

if TYPE_CHECKING:
    from rdfsolve.client.api import Client


def resolve_term(
    client: Client,
    name: str,
    *,
    kind: str,
    ontology_fallback: bool,
    identifier: str | None = None,
) -> dict[str, Any]:
    """Require one eligible identity and retain every checked candidate."""
    if kind not in {"class", "resource"}:
        raise ValueError("Choose class or resource resolution")
    evidence: dict[str, Any]
    try:
        _iri(name)
        iri = name
        evidence = {"method": "supplied IRI", "candidates": []}
    except ValueError:
        if kind == "class" and identifier is None:
            return resolve_class(client, name, ontology_fallback)
        table = client.describe(name, identifier=identifier, ontology_fallback=ontology_fallback)
        iri = description_resource(client, table)
        evidence = {
            "method": "source literal",
            "candidates": table.to_dict("records"),
            "coverage": table.attrs,
        }
    use = class_use(client, iri)
    if kind == "class" and use["status"] != "observed_class":
        raise ValueError(f"No class-use witness in the selected source: {iri}")
    return retain(client, iri, kind, {**evidence, "source_use": use})


def resolve_class(client: Client, name: str, ontology_fallback: bool) -> dict[str, Any]:
    """Resolve classes by eligible source use, keeping ontology names separate."""
    candidates: dict[str, dict[str, Any]] = {}
    for ref in client.catalogue.search(name, ontology=False):
        fragment = client.catalogue.fragments[ref]
        if fragment.kind == "type" and fragment.iri and words(fragment.label) == words(name):
            candidates[fragment.iri] = {
                "method": "schema label",
                "source_use": class_use(client, fragment.iri),
            }
    if not any(c["source_use"]["status"] == "observed_class" for c in candidates.values()):
        table = client.describe(name, ontology_fallback=ontology_fallback)
        for row in table.to_dict("records"):
            if row["Kind"] == "ontology":
                item = row["Ontology evidence"][0]
                candidates[row["Resource"]] = {
                    "method": "external ontology class",
                    "source_use": item["source_use"],
                    "ontology": item,
                    "coverage": table.attrs,
                }
            elif row["Kind"] == "resource" and (
                "http://www.w3.org/2002/07/owl#Class" in row["Types"]
                or "http://www.w3.org/2000/01/rdf-schema#Class" in row["Types"]
            ):
                candidates[row["Resource"]] = {
                    "method": "source class label",
                    "source_use": class_use(client, row["Resource"]),
                }
    eligible = [
        iri for iri, item in candidates.items() if item["source_use"]["status"] == "observed_class"
    ]
    if len(eligible) != 1:
        state = "ambiguous" if eligible else "unresolved"
        raise ValueError(f"{name!r} is {state}; eligible classes: {eligible}")
    (iri,) = eligible
    return retain(client, iri, "class", {**candidates[iri], "candidates": candidates})


def retain(client: Client, iri: str, kind: str, evidence: dict[str, Any]) -> dict[str, Any]:
    """Retain a verified query constraint without changing the schema."""
    fragment = (
        Fragment("type", iri, iri=iri, basis=evidence["method"])
        if kind == "class"
        else Fragment("term", iri, term=RdfTerm(kind="uri", value=iri), basis=evidence["method"])
    )
    ref = client.catalogue._put(fragment, ["resolved", kind, iri])
    if kind == "class":
        client.catalogue.type_refs.setdefault(iri, ref)
    client.catalogue.metadata[ref] = {"fields": [], **evidence}
    return {"iri": iri, "kind": kind, "reference": ref, **evidence}
