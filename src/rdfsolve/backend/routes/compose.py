"""Query composition routes — /api/compose/*."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from rdfsolve.backend.services.compose_service import ComposeService

compose_bp = Blueprint("compose", __name__)


@compose_bp.route("/from-paths", methods=["POST"])
def compose_from_paths():
    """Generate a SPARQL query from diagram paths."""
    data = request.get_json(force=True)
    paths = data.get("paths", [])
    prefixes = data.get("prefixes", {})
    options = data.get("options", {})

    svc = ComposeService()
    result = svc.compose_from_paths(paths, prefixes, options)

    return jsonify(result)
