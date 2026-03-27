"""REST API routes for the data-source registry.

Endpoints
---------
GET  /api/sources/
    List all source entries (optional ?domain=&bioregistry_prefix= filters).
GET  /api/sources/<name>
    Get a single source entry by name.
POST /api/sources/
    Upsert a source entry (JSON body; requires auth token if configured).
DELETE /api/sources/<name>
    Delete a source entry.
POST /api/sources/seed
    Seed the database from the configured SOURCES_YAML file.
GET  /api/sources/count
    Return the number of source entries.
"""

from __future__ import annotations

from flask import Blueprint, Response, current_app, jsonify, request

from rdfsolve.backend.database import Database
from rdfsolve.backend.services.sources_service import SourcesService

sources_bp = Blueprint("sources", __name__)

__all__ = ["sources_bp"]


def _db() -> Database:
    """Return the Database from the current Flask app context."""
    db: Database = current_app.config["DB"]
    return db


def _svc() -> SourcesService:
    """Return a SourcesService bound to the current request's database."""
    return SourcesService(_db())


@sources_bp.get("/")
def list_sources() -> Response:
    """List all source entries.

    Query parameters
    ----------------
    domain : str, optional
        Filter by bioregistry domain.
    bioregistry_prefix : str, optional
        Filter by bioregistry prefix.
    """
    domain = request.args.get("domain")
    prefix = request.args.get("bioregistry_prefix")
    entries = _svc().list_sources(domain=domain, bioregistry_prefix=prefix)
    return jsonify({"count": len(entries), "sources": entries})


@sources_bp.get("/count")
def count_sources() -> Response:
    """Return the total number of source entries."""
    return jsonify({"count": _svc().count()})


@sources_bp.get("/<path:name>")
def get_source(name: str) -> tuple[Response, int] | Response:
    """Return a single source entry by name."""
    entry = _svc().get_source(name)
    if entry is None:
        return jsonify({"error": f"Source {name!r} not found"}), 404
    return jsonify(entry)


@sources_bp.post("/")
def upsert_source() -> tuple[Response, int]:
    """Upsert a source entry from a JSON body."""
    body = request.get_json(silent=True)
    if not body or not isinstance(body, dict):
        return jsonify({"error": "JSON body required"}), 400
    try:
        name = _svc().save_source(body)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"name": name, "status": "saved"}), 201


@sources_bp.delete("/<path:name>")
def delete_source(name: str) -> tuple[Response, int]:
    """Delete a source entry by name."""
    deleted = _svc().delete_source(name)
    if not deleted:
        return jsonify({"error": f"Source {name!r} not found"}), 404
    return jsonify({"name": name, "status": "deleted"}), 200


@sources_bp.post("/seed")
def seed_sources() -> tuple[Response, int]:
    """Seed source entries from the configured SOURCES_YAML file.

    Pass ``?overwrite=true`` to force re-seeding even if entries exist.
    """
    overwrite = request.args.get("overwrite", "false").lower() == "true"
    yaml_path: str = current_app.config.get("SOURCES_YAML", "")
    if not yaml_path:
        return jsonify({"error": "SOURCES_YAML not configured"}), 500
    try:
        count = _svc().seed_from_yaml(yaml_path, overwrite=overwrite)
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"seeded": count}), 200
