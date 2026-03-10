"""SQLite database layer for rdfsolve backend.

Stores schemas (JSON-LD documents) and manually registered endpoints
in a local SQLite database.  Thread-safe via ``check_same_thread=False``.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _json_or_none(value: Any) -> str | None:
    """Serialise *value* to a JSON string, or return None if falsy."""
    if not value:
        return None
    return json.dumps(value)


_DB_INIT_SQL = """
CREATE TABLE IF NOT EXISTS schemas (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    endpoint    TEXT NOT NULL DEFAULT '',
    pattern_count INTEGER NOT NULL DEFAULT 0,
    generated_at TEXT NOT NULL DEFAULT '',
    strategy    TEXT NOT NULL DEFAULT 'miner',
    data        TEXT NOT NULL,           -- full JSON-LD (json string)
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS endpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    endpoint    TEXT NOT NULL UNIQUE,
    graph       TEXT,
    manual      INTEGER NOT NULL DEFAULT 1,   -- 1 = manually added
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS reports (
    id           TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    endpoint_url TEXT NOT NULL DEFAULT '',
    strategy     TEXT NOT NULL DEFAULT 'unknown',
    rdfsolve_version TEXT NOT NULL DEFAULT '',
    started_at   TEXT NOT NULL DEFAULT '',
    finished_at  TEXT,
    total_duration_s REAL,
    pattern_count INTEGER NOT NULL DEFAULT 0,
    class_count   INTEGER NOT NULL DEFAULT 0,
    property_count INTEGER NOT NULL DEFAULT 0,
    graphs_found  INTEGER,
    partitions_found INTEGER,
    abort_reason  TEXT,
    authors       TEXT,
    dataset_metadata TEXT,
    data          TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS void_catalogs (
    id           TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    endpoint_url TEXT NOT NULL DEFAULT '',
    strategy     TEXT NOT NULL DEFAULT 'unknown',
    rdfsolve_version TEXT NOT NULL DEFAULT '',
    generated_at TEXT NOT NULL DEFAULT '',
    pattern_count INTEGER NOT NULL DEFAULT 0,
    class_count   INTEGER NOT NULL DEFAULT 0,
    property_count INTEGER NOT NULL DEFAULT 0,
    authors       TEXT,
    dataset_metadata TEXT,
    data          TEXT NOT NULL,
    schema_id     TEXT REFERENCES schemas(id),
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS linkml_schemas (
    id           TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    endpoint_url TEXT NOT NULL DEFAULT '',
    strategy     TEXT NOT NULL DEFAULT 'unknown',
    rdfsolve_version TEXT NOT NULL DEFAULT '',
    generated_at TEXT NOT NULL DEFAULT '',
    authors       TEXT,
    dataset_metadata TEXT,
    data          TEXT NOT NULL,
    schema_id     TEXT REFERENCES schemas(id),
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Migration: add new columns to schemas if they do not yet exist.
_SCHEMAS_MIGRATIONS = [
    "ALTER TABLE schemas ADD COLUMN started_at TEXT",
    "ALTER TABLE schemas ADD COLUMN rdfsolve_version TEXT",
    "ALTER TABLE schemas ADD COLUMN authors TEXT",
    "ALTER TABLE schemas ADD COLUMN dataset_metadata TEXT",
    "ALTER TABLE schemas ADD COLUMN full_strategy TEXT",
]


class Database:
    """Simple SQLite wrapper for schema & endpoint persistence."""

    def __init__(self, db_path: str | Path = "rdfsolve.db") -> None:
        self._db_path = str(db_path)
        self._is_memory = self._db_path in (":memory:", "")
        self._lock = threading.Lock()

        if self._is_memory:
            # For in-memory databases, use a single shared connection
            # (thread-safety via the lock).
            self._shared_conn = sqlite3.connect(
                ":memory:", check_same_thread=False,
            )
            self._shared_conn.row_factory = sqlite3.Row
        else:
            self._shared_conn = None

        self._local = threading.local()
        self._init_db()

    # -- connection management ------------------------------------------

    @property
    def _conn(self) -> sqlite3.Connection:
        """Return a connection — shared for :memory:, per-thread otherwise."""
        if self._shared_conn is not None:
            return self._shared_conn

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self._db_path, check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        self._conn.executescript(_DB_INIT_SQL)
        self._conn.commit()
        # Run idempotent column migrations (ignore "duplicate column" errors).
        for stmt in _SCHEMAS_MIGRATIONS:
            try:
                self._conn.execute(stmt)
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # Column already exists — safe to ignore.

    def close(self) -> None:
        if self._shared_conn is not None:
            # Don't actually close the shared in-memory conn here;
            # it would destroy all data.
            return
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- schema operations ----------------------------------------------

    def list_schemas(self, strategy: str | None = None) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored schema.

        If *strategy* is provided, only rows with that strategy value are
        returned.  *strategy* may be a comma-separated list of values, in
        which case rows matching any of the values are returned.
        """
        if strategy:
            values = [s.strip() for s in strategy.split(",") if s.strip()]
            placeholders = ",".join("?" * len(values))
            rows = self._conn.execute(
                "SELECT id, name, endpoint, pattern_count, "
                "generated_at, strategy, data FROM schemas "
                f"WHERE strategy IN ({placeholders}) ORDER BY name",
                values,
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id, name, endpoint, pattern_count, "
                "generated_at, strategy, data FROM schemas ORDER BY name"
            ).fetchall()
        results = []
        for r in rows:
            item = dict(r)
            # Extract @about from the data blob without loading the full graph
            data_blob = item.pop("data", None)
            if data_blob:
                try:
                    doc = json.loads(data_blob)
                    item["about"] = doc.get("@about", {})
                except Exception:
                    item["about"] = {}
            results.append(item)
        return results

    def get_schema(self, schema_id: str) -> dict[str, Any] | None:
        """Load a full JSON-LD schema by *schema_id*."""
        row = self._conn.execute(
            "SELECT data FROM schemas WHERE id = ?", (schema_id,),
        ).fetchone()
        if row is None:
            # Try normalised variant (hyphens → underscores)
            norm = schema_id.replace("-", "_")
            row = self._conn.execute(
                "SELECT data FROM schemas WHERE id = ?", (norm,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["data"])

    def save_schema(
        self,
        schema_id: str,
        name: str,
        data: dict[str, Any],
        endpoint: str = "",
        pattern_count: int = 0,
        generated_at: str = "",
        strategy: str = "miner",
    ) -> str:
        """Insert or replace a schema. Returns the schema_id."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO schemas
                (id, name, endpoint, pattern_count,
                 generated_at, strategy, data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                endpoint=excluded.endpoint,
                pattern_count=excluded.pattern_count,
                generated_at=excluded.generated_at,
                strategy=excluded.strategy,
                data=excluded.data,
                updated_at=excluded.updated_at
            """,
            (
                schema_id, name, endpoint, pattern_count,
                generated_at, strategy, json.dumps(data),
                now, now,
            ),
        )
        self._conn.commit()
        return schema_id

    def delete_schema(self, schema_id: str) -> bool:
        """Delete a schema. Returns True if a row was deleted."""
        cur = self._conn.execute(
            "DELETE FROM schemas WHERE id = ?", (schema_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- endpoint operations --------------------------------------------

    def list_endpoints(self) -> list[dict[str, Any]]:
        """Return all manually registered endpoints."""
        rows = self._conn.execute(
            "SELECT id, name, endpoint, graph, manual "
            "FROM endpoints ORDER BY name",
        ).fetchall()
        return [dict(r) for r in rows]

    def add_endpoint(
        self,
        name: str,
        endpoint: str,
        graph: str | None = None,
    ) -> int:
        """Add a manual endpoint. Returns the row id."""
        cur = self._conn.execute(
            """
            INSERT INTO endpoints (name, endpoint, graph, manual)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(endpoint) DO UPDATE SET
                name=excluded.name,
                graph=excluded.graph
            """,
            (name, endpoint, graph),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def delete_endpoint(self, endpoint_url: str) -> bool:
        """Delete an endpoint by URL. Returns True if a row was deleted."""
        cur = self._conn.execute(
            "DELETE FROM endpoints WHERE endpoint = ?",
            (endpoint_url,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- schema-discovered endpoints ------------------------------------

    def get_schema_endpoints(self) -> list[dict[str, Any]]:
        """Extract endpoints from stored schemas' @about sections."""
        eps: list[dict[str, Any]] = []
        rows = self._conn.execute(
            "SELECT id, name, data FROM schemas",
        ).fetchall()
        for row in rows:
            try:
                data = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                continue
            about = data.get("@about", data.get("@metadata"))
            if not about or not isinstance(about, dict):
                continue
            ds_name = about.get("dataset_name", row["name"])
            ep = about.get("endpoint", "")
            if isinstance(ep, str) and ep:
                eps.append({
                    "name": ds_name,
                    "endpoint": ep,
                    "graph": None,
                })
            for ep_url in about.get("endpoints", []):
                if isinstance(ep_url, str) and ep_url:
                    eps.append({
                        "name": ds_name,
                        "endpoint": ep_url,
                        "graph": None,
                    })
        return eps

    # -- report operations ----------------------------------------------

    def save_report(
        self, report_id: str, data: dict[str, Any],
    ) -> str:
        """Insert or replace a run report. Returns report_id."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO reports
                (id, dataset_name, endpoint_url, strategy,
                 rdfsolve_version, started_at, finished_at,
                 total_duration_s, pattern_count, class_count,
                 property_count, graphs_found, partitions_found,
                 abort_reason, authors, dataset_metadata,
                 data, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                dataset_name=excluded.dataset_name,
                endpoint_url=excluded.endpoint_url,
                strategy=excluded.strategy,
                rdfsolve_version=excluded.rdfsolve_version,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                total_duration_s=excluded.total_duration_s,
                pattern_count=excluded.pattern_count,
                class_count=excluded.class_count,
                property_count=excluded.property_count,
                graphs_found=excluded.graphs_found,
                partitions_found=excluded.partitions_found,
                abort_reason=excluded.abort_reason,
                authors=excluded.authors,
                dataset_metadata=excluded.dataset_metadata,
                data=excluded.data
            """,
            (
                report_id,
                data.get("dataset_name", ""),
                data.get("endpoint_url", ""),
                data.get("strategy", "unknown"),
                data.get("rdfsolve_version", ""),
                data.get("started_at", ""),
                data.get("finished_at"),
                data.get("total_duration_s"),
                data.get("pattern_count", 0),
                data.get("class_count", 0),
                data.get("property_count", 0),
                data.get("graphs_found"),
                data.get("partitions_found"),
                data.get("abort_reason"),
                _json_or_none(data.get("authors")),
                _json_or_none(data.get("dataset_metadata")),
                json.dumps(data),
                now,
            ),
        )
        self._conn.commit()
        return report_id

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        """Load a full report by id."""
        row = self._conn.execute(
            "SELECT data FROM reports WHERE id = ?", (report_id,),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["data"])

    def list_reports(
        self,
        dataset_name: str | None = None,
        strategy: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored report."""
        conditions: list[str] = []
        params: list[Any] = []
        if dataset_name:
            conditions.append("dataset_name = ?")
            params.append(dataset_name)
        if strategy:
            conditions.append("strategy = ?")
            params.append(strategy)
        where = (
            "WHERE " + " AND ".join(conditions) if conditions else ""
        )
        rows = self._conn.execute(
            f"SELECT id, dataset_name, endpoint_url, strategy, "
            f"rdfsolve_version, started_at, finished_at, "
            f"total_duration_s, pattern_count, class_count, "
            f"property_count, abort_reason, authors "
            f"FROM reports {where} ORDER BY started_at DESC",
            params,
        ).fetchall()
        results = []
        for r in rows:
            item = dict(r)
            if item.get("authors"):
                try:
                    item["authors"] = json.loads(item["authors"])
                except Exception:
                    pass
            results.append(item)
        return results

    def delete_report(self, report_id: str) -> bool:
        """Delete a report. Returns True if a row was deleted."""
        cur = self._conn.execute(
            "DELETE FROM reports WHERE id = ?", (report_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- void_catalog operations ----------------------------------------

    def save_void_catalog(
        self,
        catalog_id: str,
        data: dict[str, Any],
        turtle: str,
    ) -> str:
        """Insert or replace a VoID catalog. Returns catalog_id."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO void_catalogs
                (id, dataset_name, endpoint_url, strategy,
                 rdfsolve_version, generated_at, pattern_count,
                 class_count, property_count, authors,
                 dataset_metadata, data, schema_id,
                 created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                dataset_name=excluded.dataset_name,
                endpoint_url=excluded.endpoint_url,
                strategy=excluded.strategy,
                rdfsolve_version=excluded.rdfsolve_version,
                generated_at=excluded.generated_at,
                pattern_count=excluded.pattern_count,
                class_count=excluded.class_count,
                property_count=excluded.property_count,
                authors=excluded.authors,
                dataset_metadata=excluded.dataset_metadata,
                data=excluded.data,
                schema_id=excluded.schema_id,
                updated_at=excluded.updated_at
            """,
            (
                catalog_id,
                data.get("dataset_name", ""),
                data.get("endpoint_url", ""),
                data.get("strategy", "unknown"),
                data.get("rdfsolve_version", ""),
                data.get("generated_at", ""),
                data.get("pattern_count", 0),
                data.get("class_count", 0),
                data.get("property_count", 0),
                _json_or_none(data.get("authors")),
                _json_or_none(data.get("dataset_metadata")),
                turtle,
                data.get("schema_id"),
                now,
                now,
            ),
        )
        self._conn.commit()
        return catalog_id

    def get_void_catalog(
        self, catalog_id: str,
    ) -> dict[str, Any] | None:
        """Load a VoID catalog by id. Returns meta + turtle."""
        row = self._conn.execute(
            "SELECT * FROM void_catalogs WHERE id = ?",
            (catalog_id,),
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        for field in ("authors", "dataset_metadata"):
            if item.get(field):
                try:
                    item[field] = json.loads(item[field])
                except Exception:
                    pass
        return item

    def list_void_catalogs(
        self, dataset_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored VoID catalog."""
        if dataset_name:
            rows = self._conn.execute(
                "SELECT id, dataset_name, endpoint_url, strategy, "
                "rdfsolve_version, generated_at, pattern_count, "
                "class_count, property_count "
                "FROM void_catalogs WHERE dataset_name = ? "
                "ORDER BY generated_at DESC",
                (dataset_name,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id, dataset_name, endpoint_url, strategy, "
                "rdfsolve_version, generated_at, pattern_count, "
                "class_count, property_count "
                "FROM void_catalogs ORDER BY generated_at DESC",
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_void_catalog(self, catalog_id: str) -> bool:
        """Delete a VoID catalog. Returns True if deleted."""
        cur = self._conn.execute(
            "DELETE FROM void_catalogs WHERE id = ?", (catalog_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- linkml_schema operations ---------------------------------------

    def save_linkml_schema(
        self,
        schema_id: str,
        data: dict[str, Any],
        yaml_str: str,
    ) -> str:
        """Insert or replace a LinkML schema. Returns schema_id."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO linkml_schemas
                (id, dataset_name, endpoint_url, strategy,
                 rdfsolve_version, generated_at, authors,
                 dataset_metadata, data, schema_id,
                 created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                dataset_name=excluded.dataset_name,
                endpoint_url=excluded.endpoint_url,
                strategy=excluded.strategy,
                rdfsolve_version=excluded.rdfsolve_version,
                generated_at=excluded.generated_at,
                authors=excluded.authors,
                dataset_metadata=excluded.dataset_metadata,
                data=excluded.data,
                schema_id=excluded.schema_id,
                updated_at=excluded.updated_at
            """,
            (
                schema_id,
                data.get("dataset_name", ""),
                data.get("endpoint_url", ""),
                data.get("strategy", "unknown"),
                data.get("rdfsolve_version", ""),
                data.get("generated_at", ""),
                _json_or_none(data.get("authors")),
                _json_or_none(data.get("dataset_metadata")),
                yaml_str,
                data.get("schema_id"),
                now,
                now,
            ),
        )
        self._conn.commit()
        return schema_id

    def get_linkml_schema(
        self, schema_id: str,
    ) -> dict[str, Any] | None:
        """Load a LinkML schema by id. Returns meta + yaml."""
        row = self._conn.execute(
            "SELECT * FROM linkml_schemas WHERE id = ?",
            (schema_id,),
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        for field in ("authors", "dataset_metadata"):
            if item.get(field):
                try:
                    item[field] = json.loads(item[field])
                except Exception:
                    pass
        return item

    def list_linkml_schemas(
        self, dataset_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored LinkML schema."""
        if dataset_name:
            rows = self._conn.execute(
                "SELECT id, dataset_name, endpoint_url, strategy, "
                "rdfsolve_version, generated_at "
                "FROM linkml_schemas WHERE dataset_name = ? "
                "ORDER BY generated_at DESC",
                (dataset_name,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id, dataset_name, endpoint_url, strategy, "
                "rdfsolve_version, generated_at "
                "FROM linkml_schemas ORDER BY generated_at DESC",
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_linkml_schema(self, schema_id: str) -> bool:
        """Delete a LinkML schema. Returns True if deleted."""
        cur = self._conn.execute(
            "DELETE FROM linkml_schemas WHERE id = ?", (schema_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0
