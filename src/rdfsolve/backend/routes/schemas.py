"""Schema management routes — /api/schemas/*."""

from __future__ import annotations

import json

from flask import Blueprint, current_app, jsonify, request

from rdfsolve.backend.services.schema_service import SchemaService

schemas_bp = Blueprint("schemas", __name__)


def _get_svc() -> SchemaService:
    return SchemaService(current_app.config["DB"])


@schemas_bp.route("/", methods=["GET"])
def list_schemas():
    """Return a list of all available schema IDs and metadata."""
    schemas = _get_svc().list_schemas()
    return jsonify(schemas)


@schemas_bp.route("/<schema_id>", methods=["GET"])
def get_schema(schema_id: str):
    """Return the full JSON-LD schema for a dataset."""
    schema = _get_svc().get_schema(schema_id)
    if schema is None:
        return jsonify({"error": f"Schema '{schema_id}' not found"}), 404
    return jsonify(schema)


@schemas_bp.route("/generate", methods=["POST"])
def generate_schema():
    """Generate a JSON-LD schema from a live SPARQL endpoint."""
    data = request.get_json(force=True)

    endpoint = data.get("endpoint", "")
    if not endpoint:
        return jsonify({"error": "Missing 'endpoint'"}), 400

    dataset_name = data.get("dataset_name", "unnamed")
    strategy = data.get(
        "strategy", current_app.config.get("RDFSOLVE_STRATEGY", "miner"),
    )
    graph = data.get("graph")
    save = data.get("save", True)

    svc = _get_svc()

    try:
        schema_jsonld = svc.generate_schema(
            endpoint=endpoint,
            dataset_name=dataset_name,
            strategy=strategy,
            graph=graph,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    if save:
        schema_id = svc.save_schema(dataset_name, schema_jsonld)
        schema_jsonld["_saved_as"] = schema_id

    return jsonify(schema_jsonld), 201


@schemas_bp.route("/upload", methods=["POST"])
def upload_schema():
    """Upload a JSON-LD schema file."""
    if "file" in request.files:
        file = request.files["file"]
        data = json.loads(file.read())
    else:
        data = request.get_json(force=True)

    if not data or "@context" not in data:
        return jsonify({"error": "Invalid JSON-LD: missing @context"}), 400

    svc = _get_svc()
    about = data.get("@about", data.get("@metadata", {}))
    name = about.get("dataset_name", "uploaded")
    schema_id = svc.save_schema(name, data)

    return jsonify({"id": schema_id, "message": "Schema uploaded"}), 201


@schemas_bp.route("/<schema_id>", methods=["DELETE"])
def delete_schema(schema_id: str):
    """Delete a stored schema."""
    if not _get_svc().delete_schema(schema_id):
        return jsonify({"error": "Not found"}), 404
    return jsonify({"message": f"Deleted {schema_id}"}), 200
