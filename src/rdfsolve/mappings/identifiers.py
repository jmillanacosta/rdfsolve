"""Resolve identifier candidates against caller-supplied target evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from typing import Literal

from pydantic import BaseModel, Field

from rdfsolve._uri import make_expander
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import absolute_iri

ResolutionMode = Literal["observe", "namespace"]


class IdentifierResolution(BaseModel):
    """Original RDF term, candidate namespace rules and observed target matches."""

    term: RdfTerm
    status: Literal["exact", "resolved", "ambiguous", "unresolved"] = "unresolved"
    candidates: dict[str, str] = Field(default_factory=dict)
    matches: list[str] = Field(default_factory=list)
    reason: str = ""


class IdentifierResolutionReport(BaseModel):
    """Resolution policy, source rules and the supplied target evidence identity."""

    mode: ResolutionMode
    source_name: str
    prefix: str
    aliases: list[str]
    namespaces: list[str]
    registry_version: str | None
    target_count: int
    target_sha256: str
    results: list[IdentifierResolution]


def resolve_identifiers(
    terms: Sequence[RdfTerm],
    source: SourceModel,
    *,
    target_iris: Iterable[str],
    mode: ResolutionMode = "observe",
) -> IdentifierResolutionReport:
    """Check exact IRIs or recorded namespace alternatives without changing RDF.

    Supply target IRIs observed in the intended graph/type scope. Matches prove
    membership in that supplied set, not equivalence or identifier validity.
    No graph queries or registry refreshes are performed.
    """
    if mode not in {"observe", "namespace"}:
        raise ValueError("Use observe or namespace resolution")
    targets = {absolute_iri(iri) for iri in target_iris}
    namespaces = sorted(set(source.bioregistry_uri_prefixes))
    aliases = sorted({source.bioregistry_prefix, *source.bioregistry_synonyms} - {""})
    results = []
    for term in terms:
        result = IdentifierResolution(term=term.model_copy(deep=True))
        results.append(result)
        if term.kind == "uri" and term.value in targets:
            result.status, result.matches = "exact", [term.value]
            continue
        if mode == "observe":
            result.reason = "No exact target IRI; namespace resolution was not requested"
            continue
        local = ""
        if term.kind == "uri":
            matching = [ns for ns in namespaces if term.value.startswith(ns)]
            if matching:
                local = term.value[len(max(matching, key=len)) :]
        elif (
            term.kind == "literal"
            and not term.language
            and term.datatype in {None, "http://www.w3.org/2001/XMLSchema#string"}
        ):
            prefix, separator, identifier = term.value.partition(":")
            if separator and prefix in aliases:
                local = identifier
        if not local:
            result.reason = "No identifier recognised by the supplied source rules"
            continue
        for namespace in namespaces:
            candidate = make_expander({"identifier": namespace})("identifier:" + local)
            try:
                absolute_iri(candidate)
            except ValueError:
                continue
            result.candidates[candidate] = namespace
        result.matches = sorted(result.candidates.keys() & targets)
        if len(result.matches) > 1:
            result.status = "ambiguous"
            result.reason = "Multiple recorded alternatives occur in the target evidence"
        elif result.matches:
            result.status = "resolved"
        else:
            result.reason = "No candidate occurs in the supplied target evidence"
    return IdentifierResolutionReport(
        mode=mode,
        source_name=source.name,
        prefix=source.bioregistry_prefix,
        aliases=aliases,
        namespaces=namespaces,
        registry_version=source.bioregistry_package_version,
        target_count=len(targets),
        target_sha256=hashlib.sha256(json.dumps(sorted(targets)).encode()).hexdigest(),
        results=results,
    )
