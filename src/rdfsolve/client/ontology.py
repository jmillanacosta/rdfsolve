"""Retrieve source-labelled ontology evidence without changing mined schemas."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import quote

import bioregistry
import requests
from rdflib import Literal

from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, NAME_PREDICATES, SYNONYM_PREDICATES
from rdfsolve.schema_models.paths import absolute_iri
from rdfsolve.sparql_helper import SparqlHelper, SparqlHelperError

logger = logging.getLogger(__name__)
# One ontology term or provider response, as retained in the JSON cache.
Term = dict[str, Any]
OLS = "https://www.ebi.ac.uk/ols4/api"
ONTOBEE = "https://sparql.hegroup.org/sparql/"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
DEPRECATED = "http://www.w3.org/2002/07/owl#deprecated"
PARENT = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
DEFINITIONS = (
    "http://purl.obolibrary.org/obo/IAO_0000115",
    "http://www.w3.org/2004/02/skos/core#definition",
)


def synonym_evidence(value: Term) -> list[dict[str, str]]:
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


def term_key(iri: str) -> str:
    """Use an exact IRI or a registered namespace/identifier correspondence."""
    prefix, identifier = bioregistry.parse_iri(iri)
    return f"{prefix}:{identifier}" if prefix and identifier else iri


def canonical_iri(iri: str) -> str:
    """Resolve registered IRI formats without guessing local namespaces."""
    absolute_iri(iri)
    prefix, identifier = bioregistry.parse_iri(iri)
    return bioregistry.get_iri(prefix, identifier) or iri if prefix and identifier else iri


class OntologyLookup:
    """Bounded OLS or Ontobee access with a reusable response cache."""

    def __init__(
        self,
        provider: str = "ols",
        *,
        cache: str | Path | None = None,
        offline: bool = False,
        timeout: float = 8,
        max_requests: int = 40,
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
        self.http.headers["User-Agent"] = "rdfsolve ontology lookup"
        self.helper: SparqlHelper | None = None

    def close(self) -> None:
        """Release provider connections."""
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

    def _request(self, operation: str, value: Any, fetch: Callable[[], Any]) -> Any:
        key = json.dumps([self.provider, operation, value], sort_keys=True)
        started = time.monotonic()
        stored = self.cache.get(key)
        event: dict[str, Any] = {
            "provider": self.provider,
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
    def _term(value: Term) -> Term:
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
        data = {
            **data,
            "description": list(dict.fromkeys(data["description"])),
            "synonyms": list(dict.fromkeys(data["synonyms"])),
        }
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
        }

    def search(self, text: str, *, ontology: str | None = None) -> list[Term]:
        """Return ontology candidates; membership in a dataset requires client verification."""
        if not text.strip() or len(text) > 200:
            raise ValueError("Use a vocabulary phrase of at most 200 characters")
        text = " ".join(text.split()).casefold()

        matches = [
            entry["data"]
            for key, entry in self.cache.items()
            if json.loads(key)[:2] == [self.provider, "term"]
            and entry["data"]
            and (self.offline or time.time() - entry["fetched_at"] < 86400)
            and (not ontology or entry["data"].get("ontology") == ontology)
            and text.casefold()
            in {
                v.casefold()
                for v in [
                    entry["data"]["label"],
                    *entry["data"]["synonyms"],
                    *[n["text"] for n in entry["data"].get("synonym_evidence", [])],
                ]
            }
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
                }
            )
            logger.debug("Ontology search %s: retained names", text)
            return matches[:8]

        def fetch() -> list[Term]:
            """Search exact names at the configured provider."""
            if self.provider == "ols":
                params: dict[str, Any] = {
                    "q": text,
                    "queryFields": "label,synonym",
                    "rows": 8,
                    "exact": "true",
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
            rows = self._sparql(
                f"SELECT DISTINCT ?iri ?p ?value WHERE {{ VALUES ?p {{ {predicates} }} ?iri ?p ?value . FILTER(isIRI(?iri) && isLiteral(?value) && LCASE(STR(?value)) = LCASE({literal})) }} LIMIT 8"
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

        found: list[Term] = self._request("search_names", [text, ontology], fetch) or []
        return found

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
