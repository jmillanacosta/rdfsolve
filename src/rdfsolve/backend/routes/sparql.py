"""SPARQL proxy routes — /api/sparql/*."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from rdfsolve.backend.services.sparql_service import SparqlService

sparql_bp = Blueprint("sparql", __name__)


@sparql_bp.route("/query", methods=["POST"])
def proxy_query():
    """Proxy a SPARQL query to a remote endpoint.

    Solves CORS by making the request server-side.
    """
    data = request.get_json(force=True)
    query = data.get("query", "")
    endpoint = data.get("endpoint", "")
    method = data.get("method", "GET")
    timeout = min(
        data.get("timeout", 30),
        current_app.config.get("SPARQL_TIMEOUT", 30),
    )
    variable_map = data.get("variable_map", {})

    if not query or not endpoint:
        return jsonify({"error": "Missing 'query' or 'endpoint'"}), 400

    svc = SparqlService()
    result = svc.execute(
        query=query,
        endpoint=endpoint,
        method=method,
        timeout=timeout,
        variable_map=variable_map,
    )

    return jsonify(result.model_dump())
