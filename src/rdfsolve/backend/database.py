"""SQLite database layer for rdfsolve backend.

Stores schemas (JSON-LD documents) and manually registered endpoints
in a local SQLite database.  Thread-safe via ``check_same_thread=False``.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


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

-- ── sources registry ──────────────────────────────────────────────────────
-- One row per entry from sources.yaml.  List-valued fields are stored as
-- JSON arrays; optional numeric fields use NULL when absent.
CREATE TABLE IF NOT EXISTS sources (
    name              TEXT PRIMARY KEY,
    endpoint          TEXT NOT NULL DEFAULT '',
    void_iri          TEXT NOT NULL DEFAULT '',
    graph_uris        TEXT NOT NULL DEFAULT '[]',   -- JSON array
    use_graph         INTEGER NOT NULL DEFAULT 0,
    two_phase         INTEGER NOT NULL DEFAULT 1,
    chunk_size        INTEGER,
    class_batch_size  INTEGER,
    class_chunk_size  INTEGER,
    timeout           REAL,
    delay             REAL,
    counts            INTEGER NOT NULL DEFAULT 0,
    unsafe_paging     INTEGER NOT NULL DEFAULT 0,
    notes             TEXT NOT NULL DEFAULT '',
    local_provider    TEXT NOT NULL DEFAULT '',
    download_ttl      TEXT NOT NULL DEFAULT '[]',   -- JSON array
    -- Bioregistry-derived fields (optional — populated by enrichment)
    bioregistry_prefix      TEXT NOT NULL DEFAULT '',
    bioregistry_name        TEXT NOT NULL DEFAULT '',
    bioregistry_description TEXT NOT NULL DEFAULT '',
    bioregistry_homepage    TEXT NOT NULL DEFAULT '',
    bioregistry_license     TEXT NOT NULL DEFAULT '',
    bioregistry_domain      TEXT NOT NULL DEFAULT '',
    bioregistry_keywords    TEXT NOT NULL DEFAULT '[]',     -- JSON array
    bioregistry_publications TEXT NOT NULL DEFAULT '[]',    -- JSON array
    bioregistry_uri_prefix  TEXT NOT NULL DEFAULT '',
    bioregistry_uri_prefixes TEXT NOT NULL DEFAULT '[]',    -- JSON array
    bioregistry_synonyms    TEXT NOT NULL DEFAULT '[]',     -- JSON array
    bioregistry_mappings    TEXT NOT NULL DEFAULT '{}',     -- JSON object
    bioregistry_logo        TEXT NOT NULL DEFAULT '',
    bioregistry_extra_providers TEXT NOT NULL DEFAULT '[]', -- JSON array
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ── ontology index tables ──────────────────────────────────────────────────
-- Flat relational representation of OntologyIndex, replacing pkl.gz + graphml.

-- term label/synonym → one row per (term, class_iri) pair
CREATE TABLE IF NOT EXISTS ontology_terms (
    term       TEXT NOT NULL,   -- normalised (lower-cased, stripped)
    class_iri  TEXT NOT NULL,
    PRIMARY KEY (term, class_iri)
);

-- class IRI → ontology
CREATE TABLE IF NOT EXISTS ontology_classes (
    class_iri    TEXT PRIMARY KEY,
    ontology_id  TEXT NOT NULL
);

-- ancestor chains: one row per (class_iri, position, ancestor_iri)
CREATE TABLE IF NOT EXISTS ontology_ancestors (
    class_iri    TEXT NOT NULL,
    position     INTEGER NOT NULL,   -- 0 = nearest ancestor
    ancestor_iri TEXT NOT NULL,
    PRIMARY KEY (class_iri, position)
);

-- base URI → ontology mapping
CREATE TABLE IF NOT EXISTS ontology_base_uris (
    base_uri    TEXT PRIMARY KEY,
    ontology_id TEXT NOT NULL
);

-- ontology graph nodes
CREATE TABLE IF NOT EXISTS ontology_graph_nodes (
    ontology_id      TEXT PRIMARY KEY,
    preferred_prefix TEXT NOT NULL DEFAULT '',
    base_uris        TEXT NOT NULL DEFAULT '[]',  -- JSON array
    domain           TEXT NOT NULL DEFAULT '',
    n_classes        INTEGER NOT NULL DEFAULT 0
);

-- ontology graph edges (importsFrom / exportsTo)
CREATE TABLE IF NOT EXISTS ontology_graph_edges (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    rel       TEXT NOT NULL DEFAULT 'imports',
    PRIMARY KEY (source_id, target_id)
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

# Migration: create new tables introduced after initial schema.
# Each statement is idempotent (CREATE TABLE IF NOT EXISTS).
_NEW_TABLE_MIGRATIONS = [
    """CREATE TABLE IF NOT EXISTS sources (
    name              TEXT PRIMARY KEY,
    endpoint          TEXT NOT NULL DEFAULT '',
    void_iri          TEXT NOT NULL DEFAULT '',
    graph_uris        TEXT NOT NULL DEFAULT '[]',
    use_graph         INTEGER NOT NULL DEFAULT 0,
    two_phase         INTEGER NOT NULL DEFAULT 1,
    chunk_size        INTEGER,
    class_batch_size  INTEGER,
    class_chunk_size  INTEGER,
    timeout           REAL,
    delay             REAL,
    counts            INTEGER NOT NULL DEFAULT 0,
    unsafe_paging     INTEGER NOT NULL DEFAULT 0,
    notes             TEXT NOT NULL DEFAULT '',
    local_provider    TEXT NOT NULL DEFAULT '',
    download_ttl      TEXT NOT NULL DEFAULT '[]',
    bioregistry_prefix      TEXT NOT NULL DEFAULT '',
    bioregistry_name        TEXT NOT NULL DEFAULT '',
    bioregistry_description TEXT NOT NULL DEFAULT '',
    bioregistry_homepage    TEXT NOT NULL DEFAULT '',
    bioregistry_license     TEXT NOT NULL DEFAULT '',
    bioregistry_domain      TEXT NOT NULL DEFAULT '',
    bioregistry_keywords    TEXT NOT NULL DEFAULT '[]',
    bioregistry_publications TEXT NOT NULL DEFAULT '[]',
    bioregistry_uri_prefix  TEXT NOT NULL DEFAULT '',
    bioregistry_uri_prefixes TEXT NOT NULL DEFAULT '[]',
    bioregistry_synonyms    TEXT NOT NULL DEFAULT '[]',
    bioregistry_mappings    TEXT NOT NULL DEFAULT '{}',
    bioregistry_logo        TEXT NOT NULL DEFAULT '',
    bioregistry_extra_providers TEXT NOT NULL DEFAULT '[]',
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
)""",
    """CREATE TABLE IF NOT EXISTS ontology_terms (
    term       TEXT NOT NULL,
    class_iri  TEXT NOT NULL,
    PRIMARY KEY (term, class_iri)
)""",
    """CREATE TABLE IF NOT EXISTS ontology_classes (
    class_iri    TEXT PRIMARY KEY,
    ontology_id  TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS ontology_ancestors (
    class_iri    TEXT NOT NULL,
    position     INTEGER NOT NULL,
    ancestor_iri TEXT NOT NULL,
    PRIMARY KEY (class_iri, position)
)""",
    """CREATE TABLE IF NOT EXISTS ontology_base_uris (
    base_uri    TEXT PRIMARY KEY,
    ontology_id TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS ontology_graph_nodes (
    ontology_id      TEXT PRIMARY KEY,
    preferred_prefix TEXT NOT NULL DEFAULT '',
    base_uris        TEXT NOT NULL DEFAULT '[]',
    domain           TEXT NOT NULL DEFAULT '',
    n_classes        INTEGER NOT NULL DEFAULT 0
)""",
    """CREATE TABLE IF NOT EXISTS ontology_graph_edges (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    rel       TEXT NOT NULL DEFAULT 'imports',
    PRIMARY KEY (source_id, target_id)
)""",
]


class Database:
    """Simple SQLite wrapper for schema & endpoint persistence."""

    def __init__(self, db_path: str | Path = "rdfsolve.db") -> None:
        """Initialize an rdfsolve SQLite Database."""
        self._db_path = str(db_path)
        self._is_memory = self._db_path in (":memory:", "")
        self._lock = threading.Lock()

        self._shared_conn: sqlite3.Connection | None

        if self._is_memory:
            # For in-memory databases, use a single shared connection
            # (thread-safety via the lock).
            self._shared_conn = sqlite3.connect(
                ":memory:",
                check_same_thread=False,
            )
            self._shared_conn.row_factory = sqlite3.Row
        else:
            self._shared_conn = None

        self._local = threading.local()
        self._init_db()

    # -- connection management ------------------------------------------

    @property
    def _conn(self) -> sqlite3.Connection:
        """Return a connection - shared for :memory:, per-thread otherwise."""
        if self._shared_conn is not None:
            return self._shared_conn

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        self._conn.executescript(_DB_INIT_SQL)
        self._conn.commit()
        # Ignore "duplicate column" errors.
        for stmt in _SCHEMAS_MIGRATIONS:
            try:
                self._conn.execute(stmt)
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # Column already exists.
        # Create new tables (idempotent — IF NOT EXISTS).
        for stmt in _NEW_TABLE_MIGRATIONS:
            self._conn.execute(stmt)
        self._conn.commit()

    def close(self) -> None:
        """Close the database connection (no-op for shared in-memory DBs)."""
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
            sql = (
                "SELECT id, name, endpoint, pattern_count, "  # noqa: S608
                "generated_at, strategy, data FROM schemas "
                f"WHERE strategy IN ({placeholders}) ORDER BY name"
            )
            rows = self._conn.execute(sql, values).fetchall()
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
            "SELECT data FROM schemas WHERE id = ?",
            (schema_id,),
        ).fetchone()
        if row is None:
            # Try normalised variant (hyphens -> underscores)
            norm = schema_id.replace("-", "_")
            row = self._conn.execute(
                "SELECT data FROM schemas WHERE id = ?",
                (norm,),
            ).fetchone()
        if row is None:
            return None
        result: dict[str, Any] = json.loads(row["data"])
        return result

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
                schema_id,
                name,
                endpoint,
                pattern_count,
                generated_at,
                strategy,
                json.dumps(data),
                now,
                now,
            ),
        )
        self._conn.commit()
        return schema_id

    def delete_schema(self, schema_id: str) -> bool:
        """Delete a schema. Returns True if a row was deleted."""
        cur = self._conn.execute(
            "DELETE FROM schemas WHERE id = ?",
            (schema_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- endpoint operations --------------------------------------------

    def list_endpoints(self) -> list[dict[str, Any]]:
        """Return all manually registered endpoints."""
        rows = self._conn.execute(
            "SELECT id, name, endpoint, graph, manual FROM endpoints ORDER BY name",
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
                eps.append(
                    {
                        "name": ds_name,
                        "endpoint": ep,
                        "graph": None,
                    }
                )
            for ep_url in about.get("endpoints", []):
                if isinstance(ep_url, str) and ep_url:
                    eps.append(
                        {
                            "name": ds_name,
                            "endpoint": ep_url,
                            "graph": None,
                        }
                    )
        return eps

    # -- report operations ----------------------------------------------

    def save_report(
        self,
        report_id: str,
        data: dict[str, Any],
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
            "SELECT data FROM reports WHERE id = ?",
            (report_id,),
        ).fetchone()
        if row is None:
            return None
        result: dict[str, Any] = json.loads(row["data"])
        return result

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
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = self._conn.execute(
            f"SELECT id, dataset_name, endpoint_url, strategy, "  # noqa: S608
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
                    _log.debug(
                        "authors not valid JSON for report %s",
                        item.get("id"),
                    )
            results.append(item)
        return results

    def delete_report(self, report_id: str) -> bool:
        """Delete a report. Returns True if a row was deleted."""
        cur = self._conn.execute(
            "DELETE FROM reports WHERE id = ?",
            (report_id,),
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
        self,
        catalog_id: str,
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
                    _log.debug(
                        "%s not valid JSON for void_catalog %s",
                        field,
                        item.get("id"),
                    )
        return item

    def list_void_catalogs(
        self,
        dataset_name: str | None = None,
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
            "DELETE FROM void_catalogs WHERE id = ?",
            (catalog_id,),
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
        self,
        schema_id: str,
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
                    _log.debug(
                        "%s not valid JSON for linkml_schema %s",
                        field,
                        item.get("id"),
                    )
        return item

    def list_linkml_schemas(
        self,
        dataset_name: str | None = None,
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
            "DELETE FROM linkml_schemas WHERE id = ?",
            (schema_id,),
        )
        self._conn.commit()
        return cur.rowcount > 0

    # -- sources operations ---------------------------------------------

    _SOURCE_JSON_LISTS = (
        "graph_uris",
        "download_ttl",
        "bioregistry_keywords",
        "bioregistry_publications",
        "bioregistry_uri_prefixes",
        "bioregistry_synonyms",
        "bioregistry_extra_providers",
    )
    _SOURCE_JSON_DICTS = ("bioregistry_mappings",)

    def _source_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Convert a sources table row to a plain dict, decoding JSON cols."""
        item = dict(row)
        for col in self._SOURCE_JSON_LISTS:
            raw = item.get(col)
            if raw:
                try:
                    item[col] = json.loads(raw)
                except Exception:
                    item[col] = []
            else:
                item[col] = []
        for col in self._SOURCE_JSON_DICTS:
            raw = item.get(col)
            if raw:
                try:
                    item[col] = json.loads(raw)
                except Exception:
                    item[col] = {}
            else:
                item[col] = {}
        # Convert SQLite integers back to bool
        for col in ("use_graph", "two_phase", "counts", "unsafe_paging"):
            if col in item:
                item[col] = bool(item[col])
        return item

    def save_source(self, entry: dict[str, Any]) -> str:
        """Insert or replace a single source entry. Returns the source name."""
        now = datetime.now(timezone.utc).isoformat()
        name = entry["name"]
        self._conn.execute(
            """
            INSERT INTO sources (
                name, endpoint, void_iri, graph_uris, use_graph, two_phase,
                chunk_size, class_batch_size, class_chunk_size, timeout, delay,
                counts, unsafe_paging, notes, local_provider, download_ttl,
                bioregistry_prefix, bioregistry_name, bioregistry_description,
                bioregistry_homepage, bioregistry_license, bioregistry_domain,
                bioregistry_keywords, bioregistry_publications,
                bioregistry_uri_prefix, bioregistry_uri_prefixes,
                bioregistry_synonyms, bioregistry_mappings,
                bioregistry_logo, bioregistry_extra_providers,
                created_at, updated_at
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(name) DO UPDATE SET
                endpoint=excluded.endpoint,
                void_iri=excluded.void_iri,
                graph_uris=excluded.graph_uris,
                use_graph=excluded.use_graph,
                two_phase=excluded.two_phase,
                chunk_size=excluded.chunk_size,
                class_batch_size=excluded.class_batch_size,
                class_chunk_size=excluded.class_chunk_size,
                timeout=excluded.timeout,
                delay=excluded.delay,
                counts=excluded.counts,
                unsafe_paging=excluded.unsafe_paging,
                notes=excluded.notes,
                local_provider=excluded.local_provider,
                download_ttl=excluded.download_ttl,
                bioregistry_prefix=excluded.bioregistry_prefix,
                bioregistry_name=excluded.bioregistry_name,
                bioregistry_description=excluded.bioregistry_description,
                bioregistry_homepage=excluded.bioregistry_homepage,
                bioregistry_license=excluded.bioregistry_license,
                bioregistry_domain=excluded.bioregistry_domain,
                bioregistry_keywords=excluded.bioregistry_keywords,
                bioregistry_publications=excluded.bioregistry_publications,
                bioregistry_uri_prefix=excluded.bioregistry_uri_prefix,
                bioregistry_uri_prefixes=excluded.bioregistry_uri_prefixes,
                bioregistry_synonyms=excluded.bioregistry_synonyms,
                bioregistry_mappings=excluded.bioregistry_mappings,
                bioregistry_logo=excluded.bioregistry_logo,
                bioregistry_extra_providers=excluded.bioregistry_extra_providers,
                updated_at=excluded.updated_at
            """,
            (
                name,
                entry.get("endpoint", ""),
                entry.get("void_iri", ""),
                json.dumps(entry.get("graph_uris") or []),
                int(bool(entry.get("use_graph", False))),
                int(bool(entry.get("two_phase", True))),
                entry.get("chunk_size"),
                entry.get("class_batch_size"),
                entry.get("class_chunk_size"),
                entry.get("timeout"),
                entry.get("delay"),
                int(bool(entry.get("counts", False))),
                int(bool(entry.get("unsafe_paging", False))),
                entry.get("notes", ""),
                entry.get("local_provider", ""),
                json.dumps(entry.get("download_ttl") or []),
                entry.get("bioregistry_prefix", ""),
                entry.get("bioregistry_name", ""),
                entry.get("bioregistry_description", ""),
                entry.get("bioregistry_homepage", ""),
                entry.get("bioregistry_license", ""),
                entry.get("bioregistry_domain", ""),
                json.dumps(entry.get("bioregistry_keywords") or []),
                json.dumps(entry.get("bioregistry_publications") or []),
                entry.get("bioregistry_uri_prefix", ""),
                json.dumps(entry.get("bioregistry_uri_prefixes") or []),
                json.dumps(entry.get("bioregistry_synonyms") or []),
                json.dumps(entry.get("bioregistry_mappings") or {}),
                entry.get("bioregistry_logo", ""),
                json.dumps(entry.get("bioregistry_extra_providers") or []),
                now,
                now,
            ),
        )
        self._conn.commit()
        return name

    def save_sources_bulk(self, entries: list[dict[str, Any]]) -> int:
        """Upsert many source entries at once. Returns count saved."""
        count = 0
        for entry in entries:
            self.save_source(entry)
            count += 1
        return count

    def get_source(self, name: str) -> dict[str, Any] | None:
        """Load a single source entry by name."""
        row = self._conn.execute(
            "SELECT * FROM sources WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            return None
        return self._source_row_to_dict(row)

    def list_sources(
        self,
        domain: str | None = None,
        bioregistry_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all source entries, optionally filtered by domain or bioregistry_prefix."""
        conditions: list[str] = []
        params: list[Any] = []
        if domain:
            conditions.append("bioregistry_domain = ?")
            params.append(domain)
        if bioregistry_prefix:
            conditions.append("bioregistry_prefix = ?")
            params.append(bioregistry_prefix)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = self._conn.execute(
            f"SELECT * FROM sources {where} ORDER BY name",  # noqa: S608
            params,
        ).fetchall()
        return [self._source_row_to_dict(r) for r in rows]

    def delete_source(self, name: str) -> bool:
        """Delete a source entry by name. Returns True if deleted."""
        cur = self._conn.execute("DELETE FROM sources WHERE name = ?", (name,))
        self._conn.commit()
        return cur.rowcount > 0

    def count_sources(self) -> int:
        """Return the total number of source entries in the database."""
        row = self._conn.execute("SELECT COUNT(*) FROM sources").fetchone()
        return int(row[0]) if row else 0

    # -- ontology index operations --------------------------------------

    def save_ontology_index(self, index: Any) -> None:
        """Persist an OntologyIndex to the database.

        Clears all existing ontology data and replaces it with *index*.
        Accepts any object with the same attributes as
        :class:`~rdfsolve.ontology.index.OntologyIndex`.

        Parameters
        ----------
        index:
            Populated OntologyIndex instance.
        """
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn

        # Clear existing data
        conn.execute("DELETE FROM ontology_terms")
        conn.execute("DELETE FROM ontology_classes")
        conn.execute("DELETE FROM ontology_ancestors")
        conn.execute("DELETE FROM ontology_base_uris")
        conn.execute("DELETE FROM ontology_graph_nodes")
        conn.execute("DELETE FROM ontology_graph_edges")

        # term_to_classes
        for term, iris in index.term_to_classes.items():
            for iri in iris:
                conn.execute(
                    "INSERT OR IGNORE INTO ontology_terms (term, class_iri) VALUES (?,?)",
                    (term, iri),
                )

        # class_to_ontology
        for class_iri, ont_id in index.class_to_ontology.items():
            conn.execute(
                "INSERT OR REPLACE INTO ontology_classes (class_iri, ontology_id) VALUES (?,?)",
                (class_iri, ont_id),
            )

        # ancestors
        for class_iri, anc_list in index.ancestors.items():
            for pos, anc_iri in enumerate(anc_list):
                conn.execute(
                    "INSERT OR REPLACE INTO ontology_ancestors "
                    "(class_iri, position, ancestor_iri) VALUES (?,?,?)",
                    (class_iri, pos, anc_iri),
                )

        # base_uri_to_ontology
        for base_uri, ont_id in index.base_uri_to_ontology.items():
            conn.execute(
                "INSERT OR REPLACE INTO ontology_base_uris (base_uri, ontology_id) VALUES (?,?)",
                (base_uri, ont_id),
            )

        # ontology graph
        graph = index.ontology_graph
        if graph is not None:
            for node_id, attrs in graph.nodes(data=True):
                conn.execute(
                    """INSERT OR REPLACE INTO ontology_graph_nodes
                    (ontology_id, preferred_prefix, base_uris, domain, n_classes)
                    VALUES (?,?,?,?,?)""",
                    (
                        node_id,
                        attrs.get("preferred_prefix", ""),
                        json.dumps(attrs.get("base_uris", [])),
                        attrs.get("domain", ""),
                        int(attrs.get("n_classes", 0)),
                    ),
                )
            for src, dst, attrs in graph.edges(data=True):
                conn.execute(
                    "INSERT OR REPLACE INTO ontology_graph_edges "
                    "(source_id, target_id, rel) VALUES (?,?,?)",
                    (src, dst, attrs.get("rel", "imports")),
                )

        _log.info("Ontology index saved to DB at %s", now)
        conn.commit()

    def load_ontology_index(self) -> Any:
        """Load a stored OntologyIndex from the database.

        Returns
        -------
        OntologyIndex
            Reconstructed index.

        Raises
        ------
        RuntimeError
            If no ontology data is found in the database.
        """
        from rdfsolve.ontology.index import OntologyIndex

        # term_to_classes
        rows = self._conn.execute("SELECT term, class_iri FROM ontology_terms").fetchall()
        term_to_classes: dict[str, list[str]] = {}
        for r in rows:
            term_to_classes.setdefault(r["term"], []).append(r["class_iri"])

        # class_to_ontology
        rows = self._conn.execute("SELECT class_iri, ontology_id FROM ontology_classes").fetchall()
        class_to_ontology: dict[str, str] = {r["class_iri"]: r["ontology_id"] for r in rows}

        # ancestors
        rows = self._conn.execute(
            "SELECT class_iri, position, ancestor_iri FROM ontology_ancestors "
            "ORDER BY class_iri, position"
        ).fetchall()
        ancestors: dict[str, list[str]] = {}
        for r in rows:
            ancestors.setdefault(r["class_iri"], []).append(r["ancestor_iri"])

        # base_uri_to_ontology
        rows = self._conn.execute("SELECT base_uri, ontology_id FROM ontology_base_uris").fetchall()
        base_uri_to_ontology: dict[str, str] = {r["base_uri"]: r["ontology_id"] for r in rows}

        # ontology graph
        graph: Any = None
        node_rows = self._conn.execute(
            "SELECT ontology_id, preferred_prefix, base_uris, domain, n_classes "
            "FROM ontology_graph_nodes"
        ).fetchall()
        if node_rows:
            import networkx as nx

            graph = nx.DiGraph()
            for r in node_rows:
                graph.add_node(
                    r["ontology_id"],
                    preferred_prefix=r["preferred_prefix"],
                    base_uris=json.loads(r["base_uris"] or "[]"),
                    domain=r["domain"],
                    n_classes=r["n_classes"],
                )
            edge_rows = self._conn.execute(
                "SELECT source_id, target_id, rel FROM ontology_graph_edges"
            ).fetchall()
            for r in edge_rows:
                graph.add_edge(r["source_id"], r["target_id"], rel=r["rel"])

        return OntologyIndex(
            term_to_classes=term_to_classes,
            class_to_ontology=class_to_ontology,
            ancestors=ancestors,
            base_uri_to_ontology=base_uri_to_ontology,
            ontology_graph=graph,
        )

    def ontology_stats(self) -> dict[str, int]:
        """Return row counts for ontology tables."""
        result: dict[str, int] = {}
        for table in (
            "ontology_terms",
            "ontology_classes",
            "ontology_ancestors",
            "ontology_base_uris",
            "ontology_graph_nodes",
            "ontology_graph_edges",
        ):
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
            result[table] = int(row[0]) if row else 0
        return result

    def has_ontology_index(self) -> bool:
        """Return True if any ontology data is stored in the database."""
        row = self._conn.execute("SELECT COUNT(*) FROM ontology_graph_nodes").fetchone()
        return bool(row and row[0] > 0)
