"""Run the existing mining queries on downloaded RDF."""

from __future__ import annotations

import json
from typing import Any

from rdflib import Dataset

from rdfsolve.local_rdf import LocalBackend, LocalRdf
from rdfsolve.sparql_helper import SparqlHelper


class LocalGraphHelper(SparqlHelper):
    """Query explicitly bounded local graphs without HTTP requests."""

    def __init__(
        self, endpoint_url: str, dataset: Dataset, *, backend: LocalBackend = "oxigraph"
    ) -> None:
        """Bind the local dataset and retain source identity."""
        self.local = LocalRdf(dataset, backend=backend)
        super().__init__(endpoint_url, sparql_engine=self.local.backend)
        self.dataset = dataset

    def select(self, query: str, purpose: str = "") -> dict[str, Any]:
        """Return SPARQL JSON bindings from the local dataset."""
        data = self.local.query(query).serialize(format="json")
        if data is None:
            raise RuntimeError("RDFLib returned no serialized query result")
        value: dict[str, Any] = json.loads(data)
        return value

    def construct(self, query: str) -> str:
        """Return local query results as Turtle."""
        data = self.local.query(query).serialize(format="turtle")
        if data is None:
            raise RuntimeError("RDFLib returned no serialized query result")
        return data.decode()

    def ask(self, query: str) -> bool:
        """Evaluate a local existence query."""
        return bool(self.local.query(query).askAnswer)
