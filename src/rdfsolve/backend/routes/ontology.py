"""REST API routes for the ontology index.

Endpoints
---------
GET  /api/ontology/stats
    Return row counts for all ontology tables.
GET  /api/ontology/lookup
    Look up class IRIs for a term (?term=<label>).
POST /api/ontology/build
    Build the ontology index from OLS4 and save it to the database.
    Accepts optional JSON body ``{"schema_class_uris": [...], "ontology_ids": [...]}``.
GET  /api/ontology/ready
    Return 200 if the index is present, 404 if not.
"""

from __future__ import annotations

from flask import Blueprint, Response, current_app, jsonify, request

from rdfsolve.backend.database import Database
from rdfsolve.backend.services.ontology_service import OntologyService

ontology_bp = Blueprint("ontology", __name__)

__all__ = ["ontology_bp"]


def _db() -> Database:
    """Return the Database from the current Flask app context."""
    db: Database = current_app.config["DB"]
    return db


def _svc() -> OntologyService:
    """Return an OntologyService bound to the current request's database."""
    return OntologyService(_db())


@ontology_bp.get("/ready")
def ready() -> tuple[Response, int]:
    """Return 200 if an ontology index exists in the database, 404 otherwise."""
    if _svc().has_index():
        return jsonify({"ready": True}), 200
    return jsonify({"ready": False, "error": "No ontology index in database"}), 404


@ontology_bp.get("/stats")
def stats() -> Response:
    """Return row counts for all ontology tables."""
    return jsonify(_svc().stats())


@ontology_bp.get("/lookup")
def lookup() -> tuple[Response, int]:
    """Look up class IRIs for a given term label.

    Query parameters
    ----------------
    term : str
        Natural-language label or synonym to look up.
    """
    term = request.args.get("term", "").strip()
    if not term:
        return jsonify({"error": "?term= parameter required"}), 400
    if not _svc().has_index():
        return jsonify({"error": "No ontology index in database. Call POST /build first."}), 404
    classes = _svc().lookup(term)
    return jsonify({"term": term, "classes": classes}), 200


@ontology_bp.post("/build")
def build_index() -> tuple[Response, int]:
    """Build the ontology index from OLS4 and persist it to the database.

    Optional JSON body
    ------------------
    schema_class_uris : list[str], optional
        Restrict indexing to ontologies whose baseUri matches these IRIs.
    ontology_ids : list[str], optional
        Explicit OLS4 ontology IDs to index.
    cache_dir : str, optional
        Directory for OLS HTTP-response disk cache.
        Defaults to ``ONTOLOGY_CACHE_DIR`` from app config.
    """
    body = request.get_json(silent=True) or {}
    schema_uris_raw = body.get("schema_class_uris")
    schema_class_uris: set[str] | None = set(schema_uris_raw) if schema_uris_raw else None
    ontology_ids: list[str] | None = body.get("ontology_ids")
    cache_dir: str | None = body.get("cache_dir") or current_app.config.get("ONTOLOGY_CACHE_DIR")
    try:
        idx = _svc().build_and_save(
            schema_class_uris=schema_class_uris,
            cache_dir=cache_dir,
            ontology_ids=ontology_ids,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    s = idx.stats()
    return jsonify({"status": "built", "stats": s}), 200
