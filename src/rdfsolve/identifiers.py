"""Identifiers: how rdfsolve reads an identifier, in one place.

An identifier is a registered prefix (Bioregistry) and a local identifier, written as an IRI of
a registered URI format or as a CURIE. :func:`parse` reads either form and says whether the
local identifier is valid for its prefix (its Bioregistry pattern; None when the prefix has no
pattern). Everything else is built on it: :func:`curie`, :func:`canonical_iri`, the IRI forms
of :func:`candidates`, and :func:`resolve_identifiers`, which checks values against the
namespaces that a source records.
"""

from __future__ import annotations

import functools
import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field
from rdflib import URIRef

from rdfsolve._uri import make_expander
from rdfsolve.models.source_model import SourceModel
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import absolute_iri


@dataclass(frozen=True)
class Identifier:
    """A registered identifier: its normalized prefix, standard local identifier and validity."""

    prefix: str
    local: str
    valid: bool | None

    @property
    def curie(self) -> str:
        """Return the identifier as a CURIE of its normalized prefix."""
        return f"{self.prefix}:{self.local}"

    def iri(self) -> str | None:
        """Return the preferred IRI of the identifier, if its prefix has one."""
        import bioregistry

        return bioregistry.get_iri(self.prefix, self.local)

    def iris(self) -> list[str]:
        """Return the identifier in every registered URI format that gives a valid IRI: those of
        Bioregistry and those the source registry adds (uri_formats).
        """
        import bioregistry

        resource = bioregistry.get_resource(self.prefix)
        forms = set(resource.get_uri_formats() if resource else ()) | set(
            registry_uri_formats().get(self.prefix, ())
        )
        return sorted(
            {iri for form in forms if (iri := _valid_iri(form.replace("$1", self.local)))}
        )


@functools.cache
def registry_uri_formats() -> dict[str, tuple[str, ...]]:
    """Return the URI formats the source registry adds to each Bioregistry prefix (uri_formats
    of its entries, keyed by their bioregistry_prefix); empty without the registry file.
    """
    import yaml

    from rdfsolve.sources import DEFAULT_SOURCES_YAML

    if not DEFAULT_SOURCES_YAML.is_file():
        return {}
    found: dict[str, set[str]] = {}
    for entry in yaml.safe_load(DEFAULT_SOURCES_YAML.read_text(encoding="utf-8")) or []:
        if entry.get("uri_formats") and entry.get("bioregistry_prefix"):
            found.setdefault(entry["bioregistry_prefix"], set()).update(entry["uri_formats"])
    return {prefix: tuple(sorted(forms)) for prefix, forms in found.items()}


def _valid_iri(text: str) -> str | None:
    """Return the IRI, or None when it is not valid (registries list some formats with spaces)."""
    try:
        return absolute_iri(text)
    except ValueError:
        return None


def parse(value: Any) -> Identifier | None:
    """Read an identifier written as a registered IRI or a CURIE; None for anything else.

    An identifiers.org IRI that Bioregistry reads as idot (http://identifiers.org/mgi/101757) is
    read as namespace/identifier. The local identifier is standardized (CHEBI:15377 is chebi
    15377).
    """
    import bioregistry

    text = str(value)  # an RDFLib URIRef is a str subclass that Bioregistry does not parse
    if text.startswith(("http://", "https://", "urn:")):
        prefix, local = bioregistry.parse_iri(text) or (None, None)
        if prefix == "idot" and local and "/" in local:
            prefix, _, local = local.partition("/")
    elif ":" in text and not any(c.isspace() for c in text):
        prefix, local = bioregistry.parse_curie(text) or (None, None)
    else:
        return None
    resource = bioregistry.get_resource(prefix) if prefix else None
    if resource is None or not local:
        return None
    local = resource.standardize_identifier(local)
    valid = resource.is_valid_identifier(local) if resource.get_pattern() else None
    return Identifier(resource.prefix, local, valid)


def curie(value: Any) -> str:
    """Return the CURIE of a registered identifier, else the value as given."""
    found = parse(value)
    return found.curie if found else str(value)


def canonical_iri(iri: str) -> str:
    """Return the preferred IRI of a registered identifier, else the IRI as given."""
    absolute_iri(iri)
    found = parse(iri)
    return (found.iri() if found else None) or iri


def candidates(value: Any) -> tuple[list[str], dict[str, Any]]:
    """Return every registered IRI form of an identifier, with what the expansion is based on.

    An IRI of a registered namespace also gives the forms of its CURIE, since a source may
    write the identifier in another registered form; an IRI of no registered namespace is only
    itself. A CURIE must be registered and valid.
    """
    from importlib.metadata import version

    text = str(value)
    found = parse(text)
    if text.startswith(("http://", "https://", "urn:")):
        given = absolute_iri(text)
        if found is None or found.valid is False:
            return [given], {"input": text, "basis": "exact IRI"}
        forms = found.iris()
        if not forms:
            return [given], {"input": text, "basis": "exact IRI"}
        iris = sorted({given, *forms})
        return iris, {
            "input": text,
            "basis": "registered namespace candidates",
            "registry_version": version("bioregistry"),
            "identifier": found.curie,
            "candidates": iris,
        }
    if found is None:
        raise ValueError("Use a full IRI or a registered CURIE identifier")
    if found.valid is False:
        raise ValueError("Invalid registered identifier")
    iris = found.iris()
    if not iris:
        raise ValueError("No registered IRI formats for this identifier")
    return iris, {
        "input": text,
        "basis": "registered namespace candidates",
        "registry_version": version("bioregistry"),
        "candidates": iris,
    }


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

    def to_sparql_values(self) -> str:
        """Bind accepted results as ?resolution, ?input and ?target; retain input order."""
        rows = []
        for index, result in enumerate(self.results):
            if result.status not in {"exact", "resolved"}:
                continue
            if len(result.matches) != 1 or result.term.kind == "bnode":
                raise ValueError("Accepted resolutions need one target and an addressable input")
            if result.term.kind == "uri":
                absolute_iri(result.term.value)
            target = URIRef(absolute_iri(result.matches[0])).n3()
            rows.append(f"({index} {result.term.to_rdf().n3()} {target})")
        if not rows:
            return "FILTER(1 = 0)"
        return "VALUES (?resolution ?input ?target) {\n" + "\n".join(rows) + "\n}"


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


__all__ = [
    "Identifier",
    "IdentifierResolution",
    "IdentifierResolutionReport",
    "candidates",
    "canonical_iri",
    "curie",
    "parse",
    "resolve_identifiers",
]
