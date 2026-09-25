"""Find source resources by identifier and read their statements, without a schema."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field
from rdflib import Graph, Literal, URIRef

from rdfsolve.client.hydration import _iri, _term
from rdfsolve.schema_models.enrichment import NAME_PREDICATES

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rdfsolve.client.api import Client

BATCH = 20


class Identification(BaseModel):
    """A source resource that carries an identifier, and how the source writes it."""

    identifier: str = Field(description="The identifier as given (CURIE or IRI).")
    resource: str = Field(description="The resource that carries it.")
    predicate: str = Field(description="The property that carries it.")
    value: str = Field(description="The matched value as written in the source.")
    kind: str = Field(description="literal or uri: how the value is written.")
    intermediates: list[str] = Field(
        default_factory=list,
        description="Matched resources this one links to, such as qualified statements.",
    )


def spellings(identifier: str) -> list[Any]:
    """Every way a source may write an identifier: registered IRIs, as IRIs or strings, and ids."""
    import bioregistry

    from rdfsolve.client.ontology import identifier_candidates

    iris, _ = identifier_candidates(identifier)
    terms: list[Any] = [URIRef(iri) for iri in iris] + [Literal(iri) for iri in iris]
    if not identifier.startswith(("http://", "https://", "urn:")):
        prefix, local = identifier.split(":", 1)
        resource = bioregistry.get_resource(prefix)
        local = resource.standardize_identifier(local) if resource else local
        terms += [Literal(v) for v in (local, local.upper(), local.lower(), identifier)]
    return list(dict.fromkeys(terms))


def identify(client: Client, identifiers: Iterable[str]) -> list[Identification]:
    """Match every spelling of each identifier as an exact object term in the selected graphs."""
    wanted = list(dict.fromkeys(identifiers))
    found: list[Identification] = []
    for start in range(0, len(wanted), BATCH):
        rows = " ".join(
            f"({Literal(key).n3()} {term.n3()})"
            for key in wanted[start : start + BATCH]
            for term in spellings(key)
        )
        body = client._scope(f"VALUES (?key ?o) {{ {rows} }} ?s ?p ?o . FILTER(isIRI(?s))")
        with client.step("Identify resources"):
            bindings = client._select(f"SELECT DISTINCT ?key ?s ?p ?o WHERE {{ {body} }}")
        for row in bindings:
            value = _term(row["o"])
            found.append(
                Identification(
                    identifier=row["key"]["value"],
                    resource=row["s"]["value"],
                    predicate=row["p"]["value"],
                    value=value.value,
                    kind=value.kind,
                )
            )
    return _without_intermediates(client, found)


def _without_intermediates(client: Client, found: list[Identification]) -> list[Identification]:
    """Keep a matched resource that links to another match; report the other as intermediate."""
    resources = sorted({m.resource for m in found})
    if len(resources) < 2:
        return found
    values = " ".join(_iri(r) for r in resources)
    body = client._scope(f"VALUES ?a {{ {values} }} VALUES ?b {{ {values} }} ?a ?link ?b .")
    with client.step("Link identified resources"):
        rows = client._select(f"SELECT DISTINCT ?a ?b WHERE {{ {body} }}")
    links = {(row["a"]["value"], row["b"]["value"]) for row in rows if row["a"] != row["b"]}
    matched = {(m.identifier, m.resource) for m in found}
    kept = []
    for match in found:
        parents = [a for a, b in links if b == match.resource and (match.identifier, a) in matched]
        if parents:
            continue  # reached from another resource with the same identifier
        match.intermediates = sorted(
            b for a, b in links if a == match.resource and (match.identifier, b) in matched
        )
        kept.append(match)
    return kept


def statements(client: Client, iris: Iterable[str], languages: Iterable[str] = ()) -> Graph:
    """Read the resources' direct statements and the names of the resources they point to."""
    langs = [lang.lower() for lang in languages]
    keep = (
        'FILTER(!isLiteral(?o) || LANG(?o) = "" || LCASE(LANG(?o)) IN ('
        + ", ".join(Literal(lang).n3() for lang in langs)
        + "))"
        if langs
        else ""
    )
    graph = Graph()
    subjects = list(dict.fromkeys(iris))
    names = " ".join(_iri(p) for p in NAME_PREDICATES)
    for start in range(0, len(subjects), BATCH):
        values = " ".join(_iri(s) for s in subjects[start : start + BATCH])
        body = client._scope(f"VALUES ?s {{ {values} }} ?s ?p ?o . {keep}")
        with client.step("Read statements"):
            rows = client._select(f"SELECT ?s ?p ?o WHERE {{ {body} }}", exhaustive=True)
        for row in rows:
            graph.add(
                (URIRef(row["s"]["value"]), URIRef(row["p"]["value"]), _term(row["o"]).to_rdf())
            )
    objects = sorted({str(o) for o in graph.objects() if isinstance(o, URIRef)})
    label_filter = keep.replace("?o", "?name")
    for start in range(0, len(objects), BATCH * 5):
        values = " ".join(_iri(o) for o in objects[start : start + BATCH * 5])
        body = client._scope(
            f"VALUES ?o {{ {values} }} VALUES ?p {{ {names} }} ?o ?p ?name . {label_filter}"
        )
        with client.step("Read names"):
            rows = client._select(f"SELECT ?o ?p ?name WHERE {{ {body} }}", exhaustive=True)
        for row in rows:
            graph.add(
                (URIRef(row["o"]["value"]), URIRef(row["p"]["value"]), _term(row["name"]).to_rdf())
            )
    return graph
