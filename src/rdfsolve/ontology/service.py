"""Ontologies: answers about ontology terms from an ontology provider, cached, with provenance.

OLS4 (default) or Ontobee: a term with its labels, definitions and scoped synonyms, a search
by name, and the direct named parents. Each request is cached (one day, or always offline),
bounded, and recorded as an event, so that ontology requests stay apart from the queries of a
dataset.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, cast
from urllib.parse import quote

import requests
from rdflib import Literal

from rdfsolve.identifiers import canonical_iri
from rdfsolve.ontology.terms import Term
from rdfsolve.ontology.ubergraph import UberGraph
from rdfsolve.ontology.vocabulary import DEFINITIONS, DEPRECATED, LABEL, SUBCLASS_OF
from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, NAME_PREDICATES, SYNONYM_PREDICATES
from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError

logger = logging.getLogger(__name__)
OLS = "https://www.ebi.ac.uk/ols4/api"
ONTOBEE = "https://sparql.hegroup.org/sparql/"
PARENT = SUBCLASS_OF


def synonym_evidence(value: dict[str, Any]) -> list[dict[str, str]]:
    """Retain declared synonym scope from OLS annotations and OBO metadata."""
    annotations = value.get("annotation") or {}
    evidence: list[dict[str, str]] = []
    for predicate, scope in SYNONYM_PREDICATES.items():
        local = predicate.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        keys = [predicate, local]
        if local == "IAO_0000118":
            keys += ["alternative term", "alternative_term"]
        texts = [text for key in keys for text in annotations.get(key) or []]
        texts += [
            s["name"]
            for s in value.get("obo_synonym") or []
            if s.get("scope") in {predicate, local}
        ]
        if scope == "exact":
            texts += value.get("exact_synonyms") or []
        evidence.extend(
            {"text": text, "predicate": predicate, "scope": scope}
            for text in dict.fromkeys(texts)
            if isinstance(text, str)
        )
    return evidence


SEARCH_ROWS = 8


class Ontologies:
    """Answers about ontology terms, with one bounded and cached request log.

    Terms, names and direct parents come from the provider (OLS or Ontobee); ancestors,
    descendants, relations and Biolink categories come from *ubergraph* (rdfsolve.ontology.
    ubergraph.UberGraph; the public endpoint by default).
    """

    def __init__(
        self,
        provider: str = "ols",
        *,
        cache: str | Path | None = None,
        offline: bool = False,
        timeout: float = 8,
        max_requests: int = 40,
        ubergraph: UberGraph | None = None,
    ) -> None:
        """Configure a provider; defer requests until evidence is needed."""
        if provider not in {"ols", "ontobee"}:
            raise ValueError("Use ols or ontobee as the ontology provider")
        self.provider, self.offline = provider, offline
        self.timeout, self.max_requests = timeout, max_requests
        self.path = Path(cache).expanduser().resolve() if cache else None
        self.cache: dict[str, Any] = (
            json.loads(self.path.read_text()) if self.path and self.path.exists() else {}
        )
        self.events: list[dict[str, Any]] = []
        self.requests = 0
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "rdfsolve ontologies"
        self.helper: SparqlHelper | None = None
        self.ubergraph = ubergraph

    def close(self) -> None:
        """Release provider connections."""
        if self.ubergraph is not None:
            self.ubergraph.close()
        self.http.close()
        if self.helper:
            self.helper.close()

    def cached(self, iri: str) -> bool:
        """Check whether exact vocabulary evidence is already available locally."""
        key = json.dumps([self.provider, "term", canonical_iri(iri)])
        stored = self.cache.get(key)
        return bool(
            stored
            and stored["data"]
            and (self.offline or time.time() - stored["fetched_at"] < 86400)
        )

    def _request(
        self, operation: str, value: Any, fetch: Callable[[], Any], *, provider: str | None = None
    ) -> Any:
        provider = provider or self.provider
        key = json.dumps([provider, operation, value], sort_keys=True)
        started = time.monotonic()
        stored = self.cache.get(key)
        event: dict[str, Any] = {
            "provider": provider,
            "operation": operation,
            "value": value,
            "cached": False,
        }
        if stored and (self.offline or time.time() - stored["fetched_at"] < 86400):
            event.update(cached=True, status="matched" if stored["data"] else "not_found")
            data = stored["data"]
        elif self.offline or self.requests >= self.max_requests:
            data = None
            event["status"] = "cache_miss" if self.offline else "budget_exhausted"
        else:
            try:
                self.requests += 1
                data = fetch()
                event["status"] = "matched" if data else "not_found"
                self.cache[key] = {"fetched_at": time.time(), "data": data}
                if self.path:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    with NamedTemporaryFile(mode="w", dir=self.path.parent, delete=False) as stream:
                        json.dump(self.cache, stream, ensure_ascii=False)
                        temporary = Path(stream.name)
                    temporary.replace(self.path)
            except (requests.RequestException, SparqlHelperError, ValueError, OSError) as exc:
                data = None
                event.update(status="unavailable", error=f"{type(exc).__name__}: {exc}")
        if key in self.cache:
            event["fetched_at"] = self.cache[key]["fetched_at"]
        event["seconds"] = round(time.monotonic() - started, 4)
        self.events.append(event)
        log = logger.debug if event["cached"] else logger.info
        log(
            "Ontology %s %s: %s; cached=%s; %.3fs",
            operation,
            value,
            event["status"],
            event["cached"],
            event["seconds"],
        )
        return data

    def _json(self, path: str, **params: Any) -> Any:
        with self.http.get(
            OLS + path, params=params, timeout=self.timeout, stream=True
        ) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_content(65536):
                content.extend(chunk)
                if len(content) > 2_000_000:
                    raise ValueError("Ontology response exceeded two megabytes")
            return json.loads(content)

    @staticmethod
    def _term(value: dict[str, Any]) -> Term:
        names = synonym_evidence(value)
        return {
            "iri": value["iri"],
            "label": value.get("label") or "",
            "description": value.get("description") or [],
            "synonyms": list(
                dict.fromkeys(n["text"] for n in names if n["scope"] in {"exact", "alternative"})
            ),
            "synonym_evidence": names,
            "ontology": value.get("ontology_name") or "",
            "namespace": (value.get("annotation") or {}).get("has_obo_namespace") or [],
            "obsolete": value.get("is_obsolete", False),
        }

    def lookup(self, iri: str, *, hierarchy: bool = True) -> Term | None:
        """Read one term, its direct named parents and exact lookup provenance."""
        canonical = canonical_iri(iri)
        if self.provider == "ols":

            def fetch() -> Term | None:
                """Read the term from OLS."""
                terms = (
                    self._json("/terms", iri=canonical, size=100)
                    .get("_embedded", {})
                    .get("terms", [])
                )
                terms = [t for t in terms if t.get("iri") == canonical and not t.get("is_obsolete")]
                terms.sort(
                    key=lambda t: (not t.get("is_defining_ontology"), t.get("ontology_name", ""))
                )
                return self._term(terms[0]) if terms else None
        else:

            def fetch() -> Term | None:
                """Read the term from Ontobee."""
                predicates = " ".join(
                    f"<{p}>" for p in [*NAME_PREDICATES, *DEFINITIONS, DEPRECATED]
                )
                values = self._sparql(
                    f'SELECT DISTINCT ?p ?value WHERE {{ VALUES ?p {{ {predicates} }} <{canonical}> ?p ?value . FILTER(isLiteral(?value) && (LANG(?value) = "" || LANGMATCHES(LANG(?value), "en"))) }} LIMIT 100'
                )
                if any(
                    r["p"]["value"] == DEPRECATED and r["value"]["value"] in {"true", "1"}
                    for r in values
                ):
                    return None
                labels = [
                    r["value"]["value"]
                    for p in LABEL_PREDICATES
                    for r in values
                    if r["p"]["value"] == p
                ]
                names = [
                    {
                        "text": r["value"]["value"],
                        "predicate": r["p"]["value"],
                        "scope": SYNONYM_PREDICATES[r["p"]["value"]],
                    }
                    for r in values
                    if r["p"]["value"] in SYNONYM_PREDICATES
                ]
                aliases = [n["text"] for n in names if n["scope"] in {"exact", "alternative"}]
                if not labels:
                    labels = aliases
                if not labels and not names:
                    return None
                return {
                    "iri": canonical,
                    "label": labels[0] if labels else "",
                    "description": [
                        r["value"]["value"] for r in values if r["p"]["value"] in DEFINITIONS
                    ],
                    "synonyms": aliases,
                    "synonym_evidence": names,
                    "ontology": "",
                    "namespace": [],
                    "obsolete": False,
                }

        data = self._request("term", canonical, fetch)
        if data is None:
            return None
        data = cast(
            Term,
            {
                **data,
                "description": list(dict.fromkeys(data["description"])),
                "synonyms": list(dict.fromkeys(data["synonyms"])),
            },
        )
        parents = self.parents(data) if hierarchy else []
        return {
            **data,
            "local_iri": iri,
            "provider": self.provider,
            "source": OLS + "/terms?iri=" + quote(canonical, safe="")
            if self.provider == "ols"
            else ONTOBEE,
            "match": "exact_iri" if canonical == iri else "registry_identifier",
            "parents": parents,
            "fetched_at": self.cache[json.dumps([self.provider, "term", canonical])]["fetched_at"],
        }  # type: ignore[return-value]

    def search(self, text: str, *, ontology: str | None = None, exact: bool = True) -> list[Term]:
        """Return ontology candidates; membership in a dataset requires client verification.

        With ``exact=False`` the provider ranks terms whose names contain the words, for
        example to review candidate IRIs for a phrase.
        """
        if not text.strip() or len(text) > 200:
            raise ValueError("Use a vocabulary phrase of at most 200 characters")
        text = " ".join(text.split()).casefold()

        def fetch() -> list[Term]:
            """Search exact names at the configured provider."""
            if self.provider == "ols":
                params: dict[str, Any] = {
                    "q": text,
                    "queryFields": "label,synonym",
                    "rows": SEARCH_ROWS,
                    "exact": "true" if exact else "false",
                    "obsoletes": "false",
                    "local": "true",
                    "groupField": "iri",
                }
                if ontology:
                    params["ontology"] = ontology
                docs = self._json("/search", **params).get("response", {}).get("docs", [])
                return [self._term(t) for t in docs]
            literal = Literal(text).n3()
            predicates = " ".join(f"<{p}>" for p in NAME_PREDICATES)
            match = f"LCASE(STR(?value)) = LCASE({literal})"
            if not exact:
                match = f"CONTAINS(LCASE(STR(?value)), LCASE({literal}))"
            rows = self._sparql(
                f"SELECT DISTINCT ?iri ?p ?value WHERE {{ VALUES ?p {{ {predicates} }} ?iri ?p ?value . FILTER(isIRI(?iri) && isLiteral(?value) && {match}) }} LIMIT {SEARCH_ROWS}"
            )
            return [
                {
                    "iri": r["iri"]["value"],
                    "label": r["value"]["value"] if r["p"]["value"] in LABEL_PREDICATES else "",
                    "name_match": {
                        "text": r["value"]["value"],
                        "predicate": r["p"]["value"],
                        "scope": SYNONYM_PREDICATES.get(r["p"]["value"], "label"),
                    },
                }
                for r in rows
            ]

        key = [text, ontology] if exact else [text, ontology, "candidates"]
        found: list[Term] | None = self._request("search_names", key, fetch)
        if found is not None:
            self.events[-1]["possibly_truncated"] = len(found) >= SEARCH_ROWS
            return found
        # The provider was not asked: fall back to terms already retained, never complete.
        matches = [
            entry["data"]
            for key, entry in self.cache.items()
            if json.loads(key)[:2] == [self.provider, "term"]
            and entry["data"]
            and (self.offline or time.time() - entry["fetched_at"] < 86400)
            and (not ontology or entry["data"].get("ontology") == ontology)
            and any(
                text == name or (not exact and text in name)
                for name in {
                    v.casefold()
                    for v in [
                        entry["data"]["label"],
                        *entry["data"]["synonyms"],
                        *[n["text"] for n in entry["data"].get("synonym_evidence", [])],
                    ]
                }
            )
        ]
        if matches:
            self.events.append(
                {
                    "provider": self.provider,
                    "operation": "search_names",
                    "value": [text, ontology],
                    "cached": True,
                    "status": "matched",
                    "basis": "retained names and scoped synonyms",
                    "seconds": 0,
                    "possibly_truncated": True,
                }
            )
            logger.debug("Ontology search %s: retained names", text)
            return matches[:SEARCH_ROWS]
        return []

    def parents(self, term: Term) -> list[Term]:
        """Retrieve a bounded direct hierarchy for explanation; leave query paths unchanged."""
        if self.provider == "ols" and not term.get("ontology"):
            return []

        def fetch() -> list[Term]:
            """Read direct named parents."""
            if self.provider == "ols":
                name, iri = (
                    quote(term["ontology"], safe=""),
                    quote(quote(term["iri"], safe=""), safe=""),
                )
                values = (
                    self._json(f"/ontologies/{name}/terms/{iri}/parents", size=12)
                    .get("_embedded", {})
                    .get("terms", [])
                )
                return [{"iri": t["iri"], "label": t.get("label", "")} for t in values[:12]]
            rows = self._sparql(
                f"SELECT DISTINCT ?iri ?label WHERE {{ <{term['iri']}> <{PARENT}> ?iri . FILTER(isIRI(?iri)) OPTIONAL {{ ?iri <{LABEL}> ?label . FILTER(LANG(?label) = '' || LANGMATCHES(LANG(?label), 'en')) }} }} LIMIT 12"
            )
            return [
                {"iri": r["iri"]["value"], "label": r.get("label", {}).get("value", "")}
                for r in rows
            ]

        parents = self._request("parents", [term["iri"], term.get("ontology")], fetch) or []
        named: dict[str, Term] = {}
        for parent in parents:
            named.setdefault(parent["iri"], parent)
        return list(named.values())

    def _ubergraph(self) -> UberGraph:
        if self.ubergraph is None:
            self.ubergraph = UberGraph(timeout=max(self.timeout, 30))
        return self.ubergraph

    def _closure(self, operation: str, terms: list[str], read: Callable[[], Any]) -> Any:
        return self._request(operation, sorted(set(terms)), read, provider="ubergraph")

    def _held(self, terms: list[str]) -> tuple[dict[str, str], set[str]]:
        """Return the canonical IRI of each term and the canonical IRIs that UberGraph holds.

        Another IRI form of an OBO term (identifiers.org) is asked by its PURL.
        """
        canonical = {term: canonical_iri(term) for term in terms}
        held = self._closure(
            "known",
            list(canonical.values()),
            lambda: sorted(self._ubergraph().known(canonical.values())),
        )
        return canonical, set(held or [])

    def ancestors(self, terms: list[str]) -> dict[str, list[str] | None]:
        """Return all named superclasses of each term, or None for a term that no source knows.

        UberGraph's closure for the terms it holds; OLS for the others (EFO, EDAM, ...). An
        empty list is a term without superclasses; None is not that.
        """
        canonical, held = self._held(terms)
        inside = sorted({canonical[t] for t in terms if canonical[t] in held})
        closure = (
            self._closure(
                "ancestors",
                inside,
                lambda: {t: sorted(a) for t, a in self._ubergraph().ancestors(inside).items()},
            )
            if inside
            else {}
        ) or {}
        out: dict[str, list[str] | None] = {}
        for term in terms:
            if canonical[term] in held:
                out[term] = closure.get(canonical[term])
            else:
                out[term] = self._ols_related(term, "ancestors")
        return out

    def descendants(self, term: str) -> list[str] | None:
        """Return all named subclasses of a term, or None when no source knows the term."""
        canonical, held = self._held([term])
        if canonical[term] not in held:
            return self._ols_related(term, "descendants")
        found: list[str] | None = self._closure(
            "descendants",
            [canonical[term]],
            lambda: sorted(self._ubergraph().descendants(canonical[term])),
        )
        return found

    def relations(self, terms: list[str]) -> dict[str, list[list[str]] | None]:
        """Return (property, filler) of each term for X SubClassOf (property some filler).

        Only UberGraph gives relations; a term it does not hold gets None.
        """
        canonical, held = self._held(terms)
        inside = sorted({canonical[t] for t in terms if canonical[t] in held})
        found = (
            self._closure(
                "relations", inside, lambda: [list(r) for r in self._ubergraph().relations(inside)]
            )
            if inside
            else []
        ) or []
        by_term: dict[str, list[list[str]]] = {}
        for subject, prop, filler in found:
            by_term.setdefault(subject, []).append([prop, filler])
        return {t: by_term.get(canonical[t], []) if canonical[t] in held else None for t in terms}

    def categories(self, terms: list[str]) -> dict[str, list[str] | None]:
        """Return the most specific Biolink categories of each term (the kind of a term).

        UberGraph categorizes the terms it holds; a term it does not hold gets None.
        """
        canonical, held = self._held(terms)
        inside = sorted({canonical[t] for t in terms if canonical[t] in held})
        found = (
            self._closure(
                "categories",
                inside,
                lambda: {t: sorted(c) for t, c in self._ubergraph().categories(inside).items()},
            )
            if inside
            else {}
        ) or {}
        return {t: found.get(canonical[t], []) if canonical[t] in held else None for t in terms}

    def variants(self, terms: list[str]) -> list[tuple[str, str]]:
        """Return the pairs of terms that name one compound in another form.

        Tautomers and conjugate acids and bases (rdfsolve.ontology.vocabulary.
        CHEMICAL_VARIANT_PREDICATES), from UberGraph's closure: L-serine and L-serine
        zwitterion are tautomers. Terms it does not hold have no variants here.
        """
        from rdfsolve.ontology.vocabulary import CHEMICAL_VARIANT_PREDICATES

        canonical, held = self._held(terms)
        inside = sorted({canonical[t] for t in terms if canonical[t] in held})
        found = (
            self._closure(
                "variants",
                inside,
                lambda: [
                    [s, o]
                    for s, _, o in self._ubergraph().between(inside, CHEMICAL_VARIANT_PREDICATES)
                ],
            )
            if len(inside) > 1
            else []
        ) or []
        back = {canonical[t]: t for t in terms}
        return sorted({(back.get(s, s), back.get(o, o)) for s, o in found})

    def _ols_related(self, iri: str, relation: str) -> list[str] | None:
        """Return the ancestors or descendants of a term from OLS, or None when OLS lacks it."""
        if self.provider != "ols":
            return None
        term = self.lookup(iri, hierarchy=False)
        if term is None or not term.get("ontology"):
            return None
        name = quote(term["ontology"], safe="")
        encoded = quote(quote(term["iri"], safe=""), safe="")

        def fetch() -> list[str]:
            """Read every page of the related terms."""
            found: list[str] = []
            page = 0
            while True:
                answer = self._json(
                    f"/ontologies/{name}/terms/{encoded}/{relation}", size=500, page=page
                )
                found += [t["iri"] for t in answer.get("_embedded", {}).get("terms", [])]
                pages = answer.get("page", {}).get("totalPages", 1)
                page += 1
                if page >= pages:
                    return sorted(set(found))

        found: list[str] | None = self._request(relation, [term["iri"], term["ontology"]], fetch)
        return found

    def _sparql(self, query: str) -> list[dict[str, Any]]:
        if self.helper is None:
            self.helper = SparqlHelper(ONTOBEE, timeout=self.timeout, max_retries=1)
            self.helper.enable_query_collection(include_results=True)
        rows: list[dict[str, Any]] = self.helper.select_with_fallback(
            query, purpose="ontology grounding"
        )["results"]["bindings"]
        return rows

    def diagnostics(self) -> dict[str, Any]:
        """Separate ontology requests from source-dataset queries."""
        return {
            "provider": self.provider,
            "offline": self.offline,
            "requests": sum(
                not e["cached"] and e["status"] not in {"cache_miss", "budget_exhausted"}
                for e in self.events
            ),
            "cache_hits": sum(e["cached"] for e in self.events),
            "seconds": round(sum(e["seconds"] for e in self.events), 4),
            "unavailable": sum(
                e["status"] in {"unavailable", "cache_miss", "budget_exhausted"}
                for e in self.events
            ),
        }


__all__ = ["Ontologies", "synonym_evidence"]
