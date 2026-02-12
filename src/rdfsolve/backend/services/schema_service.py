"""Schema generation, storage, and retrieval service."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)


class SchemaService:
    """Manages JSON-LD schemas: list, get, generate, upload, delete."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ── listing / retrieval ───────────────────────────────────────────

    def list_schemas(self) -> list[dict[str, Any]]:
        """Return lightweight metadata for all stored schemas."""
        return self.db.list_schemas()

    def get_schema(self, schema_id: str) -> dict[str, Any] | None:
        """Load a full JSON-LD schema by *schema_id*."""
        return self.db.get_schema(schema_id)

    # ── persistence ───────────────────────────────────────────────────

    def save_schema(
        self,
        dataset_name: str,
        schema: dict[str, Any],
    ) -> str:
        """Persist a JSON-LD schema to the database.

        Returns the schema_id.
        """
        safe = (
            dataset_name
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        schema_id = f"{safe}_schema"
        about = schema.get("@about", schema.get("@metadata", {}))
        self.db.save_schema(
            schema_id=schema_id,
            name=about.get("dataset_name", dataset_name),
            data=schema,
            endpoint=about.get("endpoint", ""),
            pattern_count=about.get("pattern_count", 0),
            generated_at=about.get("generated_at", ""),
            strategy=about.get("strategy", "miner"),
        )
        return schema_id

    def delete_schema(self, schema_id: str) -> bool:
        """Delete a schema by id."""
        return self.db.delete_schema(schema_id)

    # ── generation via rdfsolve ───────────────────────────────────────

    def generate_schema(
        self,
        endpoint: str,
        dataset_name: str,
        strategy: str = "miner",
        graph: str | None = None,
    ) -> dict[str, Any]:
        """Generate a JSON-LD schema from a live SPARQL endpoint.

        Calls into the ``rdfsolve`` package's :func:`mine_schema` API.
        """
        from rdfsolve.api import mine_schema

        graph_uris = [graph] if graph else None

        schema_jsonld = mine_schema(
            endpoint_url=endpoint,
            dataset_name=dataset_name,
            graph_uris=graph_uris,
        )

        # Ensure @about section exists
        if "@about" not in schema_jsonld:
            schema_jsonld["@about"] = {}

        about = schema_jsonld["@about"]
        about.setdefault("generated_by", "rdfsolve")
        about.setdefault(
            "generated_at",
            datetime.now(timezone.utc).isoformat(),
        )
        about.setdefault("endpoint", endpoint)
        about.setdefault("dataset_name", dataset_name)
        about.setdefault("strategy", strategy)

        return schema_jsonld

    # ── bulk import from existing JSON-LD files on disk ───────────────

    def import_from_directory(self, directory: str | Path) -> int:
        """Scan a directory for ``*.jsonld`` files and import them.

        Returns the number of schemas imported.
        """
        imported_ids: set[str] = set()
        count = 0
        for path in sorted(Path(directory).glob("*.jsonld")):
            try:
                data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Skipping %s: %s", path, exc)
                continue
            about = data.get("@about", data.get("@metadata", {}))
            name = about.get("dataset_name", path.stem)
            sid = self.save_schema(name, data)
            imported_ids.add(sid)
            count += 1

        # Remove DB entries whose source files no longer exist
        existing = {s["id"] for s in self.list_schemas()}
        stale = existing - imported_ids
        for sid in sorted(stale):
            logger.info("Removing stale schema %s (no file on disk)", sid)
            self.delete_schema(sid)

        return count
