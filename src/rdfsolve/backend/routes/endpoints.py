"""Endpoint discovery routes — /api/endpoints/*."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from rdfsolve.backend.services.endpoint_service import EndpointService

endpoints_bp = Blueprint("endpoints", __name__)


def _get_svc() -> EndpointService:
    return EndpointService(current_app.config["DB"])


@endpoints_bp.route("/", methods=["GET"])
def list_endpoints():
    """Return all known SPARQL endpoints."""
    return jsonify(_get_svc().get_all_endpoints())


@endpoints_bp.route("/", methods=["POST"])
def add_endpoint():
    """Manually register a SPARQL endpoint."""
    data = request.get_json(force=True)
    name = data.get("name", "custom")
    endpoint = data.get("endpoint", "")
    graph = data.get("graph")

    if not endpoint:
        return jsonify({"error": "Missing 'endpoint' URL"}), 400

    _get_svc().add_manual_endpoint(
        name=name, endpoint=endpoint, graph=graph,
    )
    return jsonify({"message": f"Added endpoint: {endpoint}"}), 201


@endpoints_bp.route("/health", methods=["GET"])
def check_health():
    """Ping all known endpoints with ASK {} and report status."""
    return jsonify(_get_svc().check_health())


@endpoints_bp.route("/sources", methods=["GET"])
def get_sources_jsonld():
    """Return all known sources as a JSON-LD document."""
    return jsonify(_get_svc().to_known_sources_jsonld())
