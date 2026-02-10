"""IRI resolution routes — /api/iri/*."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from rdfsolve.backend.services.endpoint_service import EndpointService
from rdfsolve.backend.services.iri_service import IriService
from rdfsolve.codegen import resolve_iris_snippet

iri_bp = Blueprint("iri", __name__)


@iri_bp.route("/resolve", methods=["POST"])
def resolve_iris():
    """Resolve IRIs against SPARQL endpoints to discover rdf:type."""
    data = request.get_json(force=True)
    iris = data.get("iris", [])
    timeout = data.get("timeout", 15)

    if not iris:
        return jsonify({"error": "No IRIs provided"}), 400

    endpoints = data.get("endpoints")
    if not endpoints:
        ep_svc = EndpointService(current_app.config["DB"])
        endpoints = ep_svc.get_all_endpoints()

    svc = IriService()
    result = svc.resolve(
        iris=iris, endpoints=endpoints, timeout=timeout,
    )

    result["rdfsolve_code"] = resolve_iris_snippet(
        iris=iris, endpoints=endpoints, timeout=timeout,
    )

    return jsonify(result)
