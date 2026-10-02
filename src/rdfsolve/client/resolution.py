"""Resolve query names into class or resource constraints with scoped source evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

from rdfsolve.client.catalogue import same_name
from rdfsolve.client.description_lookup import CLASS_TYPES
from rdfsolve.client.hydration import _iri
from rdfsolve.client.query_fragments import Fragment
from rdfsolve.schema_models.enrichment import SYNONYM_PREDICATES, RdfTerm

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rdfsolve.client.api import Client

Origin = Literal[
    "supplied IRI", "registered identifier", "schema label", "source label", "external ontology"
]


class Candidate(BaseModel):
    """One IRI considered for a name, where it came from and what the source shows."""

    iri: str
    origin: Origin
    label: str | None = None
    predicate: str | None = Field(None, description="Source predicate that carried the name.")
    use: Literal["used as class", "not used as class", "present", "absent"]
    witness: str | None = Field(None, description="One member or statement in the scope.")
    query_ids: list[int] = Field(default_factory=list)


class Resolution(BaseModel):
    """A name resolved into a query constraint, or the reason it was not."""

    name: str
    kind: Literal["class", "resource"] = Field(
        description="class: nodes typed with the IRI; resource: that exact RDF term."
    )
    status: Literal["resolved", "ambiguous", "unresolved"]
    iri: str | None = None
    reference: str | None = Field(None, description="Insertable reference when resolved.")
    candidates: list[Candidate] = Field(default_factory=list)
    graph_uris: list[str] = Field(default_factory=list)
    coverage: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list, description="Meaning of using the result.")
    warnings: list[str] = Field(default_factory=list, description="Limits of the evidence.")


class ResolutionError(ValueError):
    """A name that did not resolve to exactly one eligible IRI."""

    def __init__(self, resolution: Resolution) -> None:
        """Keep the full resolution for callers that can ask the user to choose."""
        self.resolution = resolution
        choices = [c.iri for c in resolution.candidates if c.use in {"used as class", "present"}]
        super().__init__(f"{resolution.name!r} is {resolution.status}; eligible: {choices}")


def resolve_term(
    client: Client,
    name: str,
    *,
    kind: str,
    external_names: bool = False,
    identifier: str | None = None,
) -> Resolution:
    """Collect every candidate for a name, check it in scope and keep one eligible IRI."""
    if kind not in {"class", "resource"}:
        raise ValueError("Choose kind='class' (typed members) or kind='resource' (exact term)")
    found: dict[str, dict[str, Any]] = {}
    coverage: dict[str, Any] = {}
    warnings: list[str] = []
    if identifier is None and ":" in name and " " not in name:
        found, coverage = _registered(name)
    else:
        found, coverage, warnings = _named(client, name, kind, external_names, identifier)
    checks = _witnesses(client, list(found), kind)
    return _conclude(client, name, kind, found, checks, coverage, warnings)


def _registered(name: str) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Return the IRI candidates of an IRI or CURIE, with what they are based on."""
    from rdfsolve import identifiers

    try:
        iris, coverage = identifiers.candidates(name)
    except ValueError:
        iris, coverage = [], {}
    origin: Origin = (
        "supplied IRI" if coverage.get("basis") == "exact IRI" else ("registered identifier")
    )
    if not iris:
        _iri(name)
        iris, origin = [name], "supplied IRI"
    return {iri: {"origin": "supplied IRI" if iri == name else origin} for iri in iris}, coverage


def resolve_terms(
    client: Client, names: Iterable[str], *, kind: str, batch: int = 200
) -> dict[str, Resolution]:
    """Resolve many names at once: the candidates of every IRI or CURIE are checked together.

    The same decisions as :func:`resolve_term`, with one check query for each *batch* of
    candidate IRIs instead of one for each name (134 identifiers of WP4726: 134 queries
    before). A name that is not an IRI or CURIE is resolved on its own.
    """
    if kind not in {"class", "resource"}:
        raise ValueError("Choose kind='class' (typed members) or kind='resource' (exact term)")
    results: dict[str, Resolution] = {}
    pending: dict[str, tuple[dict[str, dict[str, Any]], dict[str, Any]]] = {}
    for name in dict.fromkeys(names):
        if ":" in name and " " not in name:
            pending[name] = _registered(name)
        else:
            results[name] = resolve_term(client, name, kind=kind)
    iris = sorted({iri for found, _ in pending.values() for iri in found})
    checks: dict[str, dict[str, Any]] = {}
    for start in range(0, len(iris), batch):
        checks.update(_witnesses(client, iris[start : start + batch], kind))
    for name, (found, coverage) in pending.items():
        results[name] = _conclude(client, name, kind, found, checks, coverage, [])
    return results


def _conclude(
    client: Client,
    name: str,
    kind: str,
    found: dict[str, dict[str, Any]],
    checks: dict[str, dict[str, Any]],
    coverage: dict[str, Any],
    warnings: list[str],
) -> Resolution:
    """Decide a resolution from its candidates and their checks in the source."""
    candidates = [
        Candidate(iri=iri, **item, **checks[iri]) for iri, item in found.items() if iri in checks
    ]
    if found.keys() - checks.keys():
        warnings.append("Some candidates were not checked in the source")
    eligible = [c for c in candidates if c.use in {"used as class", "present"}]
    if coverage.get("status") == "partial":
        warnings.append("The source name search was truncated; other matches may exist")
    status = "resolved" if len(eligible) == 1 else "ambiguous" if eligible else "unresolved"
    if status == "resolved" and coverage.get("status") == "partial" and kind == "resource":
        status = "unresolved"
    if coverage.get("basis") == "registered namespace candidates":
        # Absent namespace spellings stay listed in coverage, not as separate candidates.
        candidates = eligible or candidates[:1]
    result = Resolution(
        name=name,
        kind=kind,
        status=status,
        candidates=candidates,
        graph_uris=list(client.graph_uris),
        coverage=coverage,
        warnings=warnings,
    )
    if status == "resolved":
        _retain(client, result, eligible[0])
    return result


def _named(
    client: Client, name: str, kind: str, external: bool, identifier: str | None
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], list[str]]:
    """Find schema, source and optional external candidates for a name."""
    found: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    if kind == "class" and identifier is None:
        for ref in client.catalogue.search(name, ontology=False):
            fragment = client.catalogue.fragments[ref]
            if fragment.kind == "type" and fragment.iri and same_name(fragment.label, name):
                found.setdefault(fragment.iri, {"origin": "schema label", "label": fragment.label})
    table = client.describe(name, identifier=identifier)
    coverage = dict(table.attrs.get("coverage", {}))
    for row in table.to_dict("records"):
        if row["Kind"] != "resource" or (
            kind == "class" and not CLASS_TYPES.intersection(row["Types"])
        ):
            continue
        found.setdefault(
            row["Resource"],
            {"origin": "source label", "label": row["Label"], "predicate": row["Predicate"]},
        )
        if SYNONYM_PREDICATES.get(row["Predicate"]) in {"broad", "narrow", "related"}:
            warnings.append(f"{row['Resource']} carries the name as a {row['Predicate']} value")
    if external and kind == "class":
        from rdfsolve.client.description_lookup import external_candidates

        event = external_candidates(client, name, coverage, [])
        coverage["external"] = {k: v for k, v in event.items() if k != "candidates"}
        for item in event["candidates"]:
            found.setdefault(
                item["iri"], {"origin": "external ontology", "label": item.get("label")}
            )
        if any(e.get("possibly_truncated") for e in event.get("events", [])):
            warnings.append("The external name search returned its maximum; others may exist")
    return found, coverage, warnings


def _witnesses(client: Client, iris: list[str], kind: str) -> dict[str, dict[str, Any]]:
    """Return one scoped member (class) or statement (resource) for each IRI, in one query.

    The IRIs are one VALUES list and each keeps one witness (SAMPLE). Before, each IRI was a
    LIMIT 1 subquery in a UNION, which QLever answered slowly for many IRIs (66 ChEBI ids in
    batches of 200 candidates: 93 s, against 23 s one by one).
    """
    if not iris:
        return {}
    values = " ".join(_iri(iri) for iri in iris)
    body = (
        client._subject_type("?w", "?candidate")
        if kind == "class"
        else "{ ?candidate ?_p ?w } UNION { ?w ?_p ?candidate }"
    )
    query = (
        f"SELECT ?candidate (SAMPLE(?w) AS ?witness) WHERE {{ VALUES ?candidate {{ {values} }} "
        f"{client._scope(body)} }} GROUP BY ?candidate"
    )
    with client.step(f"Check {kind} candidates in scope"):
        rows = client._select(query)
    ids = list(client._steps[-1]["query_ids"])
    seen = {row["candidate"]["value"]: row["witness"]["value"] for row in rows if "witness" in row}
    used, missing = (
        ("used as class", "not used as class")
        if kind == "class"
        else (
            "present",
            "absent",
        )
    )
    return {
        iri: {"use": used if iri in seen else missing, "witness": seen.get(iri), "query_ids": ids}
        for iri in iris
    }


def _retain(client: Client, result: Resolution, chosen: Candidate) -> None:
    """Keep the chosen IRI as an insertable reference and state its meaning."""
    iri, kind = chosen.iri, result.kind
    fragment = (
        Fragment("type", chosen.label or iri, iri=iri, basis=chosen.origin)
        if kind == "class"
        else Fragment(
            "term", chosen.label or iri, term=RdfTerm(kind="uri", value=iri), basis=chosen.origin
        )
    )
    ref = client.catalogue._put(fragment, ["resolved", kind, iri])
    if kind == "class":
        client.catalogue.type_refs.setdefault(iri, ref)
        result.notes.append(
            f"Matches nodes with rdf:type <{iri}> in the selected graphs; members of its "
            "subclasses are not included unless they are typed with it."
        )
    else:
        result.notes.append(f"Matches the exact term <{iri}>, not members of a class.")
    if chosen.origin == "external ontology":
        result.notes.append("The name comes from an external ontology label, not the source.")
    client.catalogue.metadata[ref] = {"fields": [], "resolution": result.model_dump(mode="json")}
    result.iri, result.reference = iri, ref
