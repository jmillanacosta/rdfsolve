"""Endpoint discovery and management service."""

from __future__ import annotations

import requests

from rdfsolve.backend.database import Database


class EndpointService:
    """Manages known SPARQL endpoints (auto-discovered + manual)."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def get_all_endpoints(self) -> list[dict]:
        """Merge schema-discovered + manually registered endpoints.

        De-duplicates by endpoint URL.
        """
        seen: set[str] = set()
        result: list[dict] = []

        sources = (
            self.db.get_schema_endpoints()
            + self.db.list_endpoints()
        )
        for ep in sources:
            url = ep["endpoint"]
            if url not in seen:
                seen.add(url)
                result.append({
                    "name": ep.get("name", "unknown"),
                    "endpoint": url,
                    "graph": ep.get("graph"),
                })
        return result

    def add_manual_endpoint(
        self,
        name: str,
        endpoint: str,
        graph: str | None = None,
    ) -> int:
        """Register a manually provided endpoint."""
        return self.db.add_endpoint(
            name=name, endpoint=endpoint, graph=graph,
        )

    def check_health(self) -> list[dict]:
        """Ping each endpoint with ``ASK {}`` and report status."""
        results = []
        for ep in self.get_all_endpoints():
            status = "unknown"
            try:
                resp = requests.get(
                    ep["endpoint"],
                    params={"query": "ASK {}", "format": "json"},
                    headers={
                        "Accept": "application/sparql-results+json",
                    },
                    timeout=5,
                )
                status = (
                    "ok" if resp.ok else f"http_{resp.status_code}"
                )
            except requests.exceptions.Timeout:
                status = "timeout"
            except requests.exceptions.ConnectionError:
                status = "unreachable"
            except Exception as exc:
                status = f"error: {str(exc)[:80]}"

            results.append({**ep, "status": status})
        return results

    def to_known_sources_jsonld(self) -> dict:
        """Build a KnownSourcesJSONLD document."""
        graph = []
        for ep in self.get_all_endpoints():
            safe = ep["name"].lower().replace(" ", "_")
            graph.append({
                "@id": f"urn:source:{safe}",
                "@type": ["sd:Service", "void:Dataset"],
                "dcterms:title": ep["name"],
                "sd:endpoint": {"@id": ep["endpoint"]},
                "void:sparqlEndpoint": {"@id": ep["endpoint"]},
            })

        return {
            "@context": {
                "dcterms": "http://purl.org/dc/terms/",
                "sd": (
                    "http://www.w3.org/ns/"
                    "sparql-service-description#"
                ),
                "void": "http://rdfs.org/ns/void#",
            },
            "@graph": graph,
        }
