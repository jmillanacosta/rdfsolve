"""Instance mapping service — thin Flask wrapper around the probe API.

Mappings are stored in the **existing** ``schemas`` table with
``strategy`` set to one of:
  - ``'instance_matcher'`` — probed from SPARQL endpoints
  - ``'semra_import'``     — imported from a SeMRA source
  - ``'inferenced'``       — produced by the inference pipeline

No schema migration is required.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)

# All strategy values that identify a mapping (not a schema)
_MAPPING_STRATEGIES = {"instance_matcher", "semra_import", "inferenced", "sssom_import"}


class MappingService:
    """Manage instance mappings via the shared ``schemas`` table.

    Mappings are distinguished from mined schemas by
    ``strategy = 'instance_matcher'``.  The JSON-LD format is fully
    compatible with the mined-schema format, so the existing
    ``SchemaService.import_from_directory()`` and Flask startup flow
    can also handle mapping files placed in ``docker/schemas/``.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    # ── read ──────────────────────────────────────────────────────────

    def list_mappings(self) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored mapping."""
        all_schemas = self.db.list_schemas()
        return [
            s for s in all_schemas
            if s.get("strategy") in _MAPPING_STRATEGIES
        ]

    def get_mapping(self, mapping_id: str) -> dict[str, Any] | None:
        """Return the full JSON-LD for a mapping, or ``None``."""
        return self.db.get_schema(mapping_id)

    # ── write ─────────────────────────────────────────────────────────

    def save_mapping(
        self,
        prefix: str,
        data: dict[str, Any],
        strategy: str = "instance_matcher",
    ) -> str:
        """Persist a mapping JSON-LD document.  Returns the mapping id."""
        about = data.get("@about", {})
        detected_strategy = about.get("strategy", strategy)
        mapping_id = about.get("dataset_name", f"{prefix}_mapping")
        self.db.save_schema(
            schema_id=mapping_id,
            name=about.get("dataset_name", mapping_id),
            data=data,
            endpoint="",
            pattern_count=about.get("pattern_count", 0),
            generated_at=about.get("generated_at", ""),
            strategy=detected_strategy,
        )
        return mapping_id

    def delete_mapping(self, mapping_id: str) -> bool:
        """Delete a mapping. Returns ``True`` if a row was removed."""
        return self.db.delete_schema(mapping_id)

    # ── probe (delegates to API) ──────────────────────────────────────

    def _datasources_from_db(self) -> "pd.DataFrame":
        """Build a datasources DataFrame from schemas stored in the DB.

        Reads ``name`` and ``endpoint`` from every row in the ``schemas``
        table whose strategy is ``'miner'``, mirroring the columns expected
        by :func:`rdfsolve.instance_matcher.probe_resource`.
        """
        import pandas as pd

        rows = self.db._conn.execute(
            "SELECT name, endpoint FROM schemas WHERE strategy = 'miner'"
        ).fetchall()
        return pd.DataFrame(
            [{"dataset_name": r["name"], "endpoint_url": r["endpoint"]}
             for r in rows]
        )

    def probe(
        self,
        prefix: str,
        predicate: str = "http://www.w3.org/2004/02/skos/core#narrowMatch",
        dataset_names: Optional[list[str]] = None,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Run the instance matcher for *prefix* and return JSON-LD.

        Datasources are loaded from the database (miner schemas), so no
        CSV file is needed inside the container.

        Args:
            prefix: Bioregistry prefix (e.g. ``"ensembl"``).
            predicate: Mapping predicate URI.
            dataset_names: Optional subset of datasets to probe.
            timeout: SPARQL request timeout in seconds.

        Returns:
            JSON-LD dict ready for :meth:`save_mapping`.
        """
        import pandas as pd
        from rdfsolve.instance_matcher import probe_resource

        datasources: pd.DataFrame = self._datasources_from_db()
        if datasources.empty:
            raise ValueError(
                "No miner schemas found in the database. "
                "Seed schemas first (SchemaService.import_from_directory)."
            )

        mapping = probe_resource(
            prefix=prefix,
            datasources=datasources,
            predicate=predicate,
            dataset_names=dataset_names,
            timeout=timeout,
        )
        return mapping.to_jsonld()
