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
    method: str = Field(
        "any property",
        description="schema property: asked only properties whose examples carry this "
        "identifier type; any property: every property was searched.",
    )
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


def carriers(client: Client, prefix: str) -> list[str]:
    """Properties whose example values in the schema are identifiers of this registered type."""
    import bioregistry

    resource = bioregistry.get_resource(prefix)
    if resource is None:
        return []
    found = []
    for example in client._schema.enrichment.examples:
        value = example.value.value
        parsed = bioregistry.parse_iri(value) if value.startswith(("http://", "https://")) else None
        if (parsed and bioregistry.normalize_prefix(parsed[0]) == resource.prefix) or (
            not parsed and resource.is_valid_identifier(value)
        ):
            found.append(example.property_uri)
    return sorted(set(found))


def identify(client: Client, identifiers: Iterable[str]) -> list[Identification]:
    """Match every spelling of each identifier as an exact object term in the selected graphs.

    When the schema's examples show which properties carry an identifier type, only those
    properties are asked (an index lookup); otherwise every property is searched.
    """
    wanted = list(dict.fromkeys(identifiers))
    found: list[Identification] = []
    for start in range(0, len(wanted), BATCH):
        batch = wanted[start : start + BATCH]
        groups: dict[tuple[str, ...], list[str]] = {}
        for key in batch:
            prefix = "" if key.startswith(("http://", "https://", "urn:")) else key.split(":", 1)[0]
            groups.setdefault(tuple(carriers(client, prefix)) if prefix else (), []).append(key)
        for predicates, keys in groups.items():
            rows = " ".join(f"({Literal(k).n3()} {t.n3()})" for k in keys for t in spellings(k))
            values = f"VALUES (?key ?o) {{ {rows} }}"
            if predicates:
                pattern = " UNION ".join(
                    f"{{ {values} ?s {_iri(p)} ?o . BIND({_iri(p)} AS ?p) }}" for p in predicates
                )
            else:
                pattern = f"{values} ?s ?p ?o ."
            body = client._scope(f"{pattern} FILTER(isIRI(?s))")
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
                        method="schema property" if predicates else "any property",
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
