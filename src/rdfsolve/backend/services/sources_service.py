"""Service layer for data-source registry operations.

Wraps :class:`~rdfsolve.backend.database.Database` sources CRUD and
:class:`~rdfsolve.models.source_model.SourcesRegistry` YAML loading.

Typical usage::

    from rdfsolve.backend.services.sources_service import SourcesService

    svc = SourcesService(db)
    svc.seed_from_yaml("data/sources.yaml")
    entries = svc.list_sources(domain="chemical")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)

__all__ = ["SourcesService"]


class SourcesService:
    """Business logic for the sources registry.

    Parameters
    ----------
    db:
        Open :class:`~rdfsolve.backend.database.Database` instance.
    """

    def __init__(self, db: Database) -> None:
        """Initialise the service with a database handle."""
        self._db = db

    def seed_from_yaml(
        self,
        yaml_path: str | Path,
        *,
        overwrite: bool = False,
    ) -> int:
        """Load sources from *yaml_path* and upsert them into the database.

        Parameters
        ----------
        yaml_path:
            Path to ``sources.yaml``.
        overwrite:
            When ``False`` (default), skip seeding if the database already
            contains source entries.  When ``True``, always upsert all
            entries (idempotent update).

        Returns
        -------
        int
            Number of entries saved (0 if skipped).
        """
        from rdfsolve.models.source_model import SourcesRegistry

        if not overwrite and self._db.count_sources() > 0:
            logger.debug("Sources table already populated; skipping YAML seed.")
            return 0

        registry = SourcesRegistry.from_yaml(yaml_path)
        rows = [s.to_db_dict() for s in registry.sources]
        count = self._db.save_sources_bulk(rows)
        logger.info("Seeded %d source entries from %s", count, yaml_path)
        return count

    def list_sources(
        self,
        domain: str | None = None,
        bioregistry_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all source entries from the database.

        Parameters
        ----------
        domain:
            Optional bioregistry domain filter (e.g. ``"chemical"``).
        bioregistry_prefix:
            Optional Bioregistry prefix filter.

        Returns
        -------
        list[dict]
            Source entry dicts as stored in the database.
        """
        return self._db.list_sources(domain=domain, bioregistry_prefix=bioregistry_prefix)

    def get_source(self, name: str) -> dict[str, Any] | None:
        """Return a single source entry by name.

        Parameters
        ----------
        name:
            Source name (primary key).

        Returns
        -------
        dict or None
        """
        return self._db.get_source(name)

    def save_source(self, entry: dict[str, Any]) -> str:
        """Validate and upsert a single source entry.

        Parameters
        ----------
        entry:
            Raw dict (e.g. from the API request body).  Validated via
            :class:`~rdfsolve.models.source_model.SourceModel`.

        Returns
        -------
        str
            Source name (primary key).

        Raises
        ------
        pydantic.ValidationError
            If *entry* fails model validation.
        """
        from rdfsolve.models.source_model import SourceModel

        model = SourceModel.model_validate(entry)
        return self._db.save_source(model.to_db_dict())

    def delete_source(self, name: str) -> bool:
        """Delete a source entry by name.

        Parameters
        ----------
        name:
            Source name.

        Returns
        -------
        bool
            True if the entry existed and was deleted.
        """
        return self._db.delete_source(name)

    def count(self) -> int:
        """Return the number of source entries in the database."""
        return self._db.count_sources()
