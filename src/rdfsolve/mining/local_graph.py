"""Run the existing mining queries on downloaded RDF."""

from __future__ import annotations

import json
from functools import cached_property
from typing import Any

import pyoxigraph as ox
from rdflib import Dataset

from rdfsolve.local_rdf import LocalBackend, LocalRdf, to_rdflib
from rdfsolve.sparql_helper import SparqlHelper


class LocalGraphHelper(SparqlHelper):
    """Query explicitly bounded local graphs without HTTP requests."""

    def __init__(
        self,
        endpoint_url: str,
        dataset: Dataset | ox.Dataset | ox.Store,
        *,
        backend: LocalBackend = "oxigraph",
    ) -> None:
        """Bind the local dataset and retain source identity."""
        self.local = LocalRdf(dataset, backend=backend)
        super().__init__(endpoint_url, sparql_engine=self.local.backend)
        self._source = dataset

    @cached_property
    def dataset(self) -> Dataset | None:
        """Return the data as an RDFLib dataset for the walk over RDF lists.

        Oxigraph data is copied to RDFLib only when it contains RDF lists.
        """
        if isinstance(self._source, Dataset):
            return self._source
        first = "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?o"
        if not self.ask(f"ASK {{ {{ {first} }} UNION {{ GRAPH ?g {{ {first} }} }} }}"):
            return None
        return to_rdflib(self._source)

    def select(self, query: str, purpose: str = "") -> dict[str, Any]:
        """Return SPARQL JSON bindings from the local dataset."""
        return self.local.select_json(query)

    def construct(self, query: str) -> str:
        """Return local query results as Turtle."""
        data = self.local.query(query).serialize(format="turtle")
        if data is None:
            raise RuntimeError("RDFLib returned no serialized query result")
        return data.decode()

    def ask(self, query: str) -> bool:
        """Evaluate a local existence query."""
        return bool(self.local.query(query).askAnswer)
