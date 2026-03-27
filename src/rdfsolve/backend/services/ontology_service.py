"""Service layer for ontology index operations.

Wraps :class:`~rdfsolve.backend.database.Database` ontology CRUD and
:func:`~rdfsolve.ontology.index.build_ontology_index` / db-backed
save/load helpers.

Typical usage::

    from rdfsolve.backend.services.ontology_service import OntologyService

    svc = OntologyService(db)
    if not svc.has_index():
        idx = svc.build_and_save(cache_dir="/tmp/ols_cache")
    else:
        idx = svc.load()

    classes = idx.lookup("aspirin")
"""

from __future__ import annotations

import logging
from typing import Any

from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)

__all__ = ["OntologyService"]


class OntologyService:
    """Business logic for ontology index persistence.

    Parameters
    ----------
    db:
        Open :class:`~rdfsolve.backend.database.Database` instance.
    """

    def __init__(self, db: Database) -> None:
        """Initialise the service with a database handle."""
        self._db = db

    def has_index(self) -> bool:
        """Return True if an ontology index is already stored in the database."""
        return self._db.has_ontology_index()

    def stats(self) -> dict[str, int]:
        """Return ontology table row counts.

        Returns
        -------
        dict[str, int]
            Keys are table names; values are row counts.
        """
        return self._db.ontology_stats()

    def build_and_save(
        self,
        schema_class_uris: set[str] | None = None,
        *,
        cache_dir: str | None = None,
        ontology_ids: list[str] | None = None,
    ) -> Any:
        """Build an OntologyIndex from OLS4, save it to the database, and return it.

        Parameters
        ----------
        schema_class_uris:
            Optional set of class IRIs to restrict which ontologies are
            fully indexed.  Passed to
            :func:`~rdfsolve.ontology.index.build_ontology_index`.
        cache_dir:
            Optional directory for the OLS HTTP-response disk cache.
        ontology_ids:
            Optional explicit list of OLS4 ontology IDs to index.

        Returns
        -------
        OntologyIndex
            The freshly built and persisted index.
        """
        from rdfsolve.ontology.index import build_ontology_index, save_ontology_index_to_db

        idx = build_ontology_index(
            schema_class_uris=schema_class_uris,
            cache_dir=cache_dir,
            ontology_ids=ontology_ids,
        )
        save_ontology_index_to_db(idx, self._db)
        logger.info("Ontology index built and saved to DB: %s", idx.stats())
        return idx

    def load(self) -> Any:
        """Load the stored OntologyIndex from the database.

        Returns
        -------
        OntologyIndex
            Reconstructed index.

        Raises
        ------
        RuntimeError
            If no ontology index is found in the database.
        """
        from rdfsolve.ontology.index import load_ontology_index_from_db

        return load_ontology_index_from_db(self._db)

    def lookup(self, term: str) -> list[str]:
        """Return class IRIs matching *term* from the stored index.

        Parameters
        ----------
        term:
            Natural-language label or synonym.

        Returns
        -------
        list[str]
            Matching class IRIs, or an empty list.
        """
        idx = self.load()
        result: list[str] = idx.lookup(term)
        return result
