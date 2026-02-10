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
"""


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

    def list_schemas(self) -> list[dict[str, Any]]:
        """Return lightweight metadata for every stored schema."""
        rows = self._conn.execute(
            "SELECT id, name, endpoint, pattern_count, "
            "generated_at, strategy FROM schemas ORDER BY name"
        ).fetchall()
        return [dict(r) for r in rows]

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
