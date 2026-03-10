"""LinkML schema routes — /api/linkml/*."""

from __future__ import annotations

from flask import Blueprint, Response, current_app, jsonify, request

linkml_bp = Blueprint("linkml", __name__)


@linkml_bp.route("/", methods=["GET"])
def list_linkml_schemas():
    """List all LinkML schemas.

    Query parameter: ``dataset`` — filter by dataset name.
    """
    db = current_app.config["DB"]
    dataset = request.args.get("dataset")
    items = db.list_linkml_schemas(dataset_name=dataset)
    return jsonify(items)


@linkml_bp.route("/<schema_id>", methods=["GET"])
def get_linkml_schema(schema_id: str):
    """Return the LinkML schema as ``text/yaml``."""
    db = current_app.config["DB"]
    record = db.get_linkml_schema(schema_id)
    if record is None:
        return jsonify({"error": "LinkML schema not found"}), 404
    yaml_str = record.get("data", "")
    accept = request.headers.get("Accept", "")
    if "application/json" in accept:
        return jsonify(record)
    return Response(
        yaml_str,
        mimetype="text/yaml",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{schema_id}.yaml"'
            ),
        },
    )


@linkml_bp.route("/", methods=["POST"])
def upload_linkml_schema():
    """Upload a LinkML schema YAML.

    Accepts JSON body with ``meta`` (dict) and ``yaml`` (string),
    or a ``.yaml`` / ``.yml`` file upload under the ``file`` field.
    """
    db = current_app.config["DB"]

    if request.is_json:
        body = request.get_json(force=True)
        meta = body.get("meta", {})
        yaml_str = body.get("yaml", "")
    elif "file" in request.files:
        f = request.files["file"]
        yaml_str = f.read().decode("utf-8", errors="replace")
        fname: str = f.filename or "unknown.yaml"
        dataset_name = (
            fname.replace(".yaml", "").replace(".yml", "")
        )
        meta = {"dataset_name": dataset_name}
    else:
        return jsonify(
            {"error": "No JSON body or file provided"},
        ), 400

    dataset_name = meta.get("dataset_name", "unknown")
    schema_id = f"{dataset_name}_linkml"

    db.save_linkml_schema(schema_id, meta, yaml_str)
    return jsonify({"id": schema_id}), 201


@linkml_bp.route("/<schema_id>", methods=["DELETE"])
def delete_linkml_schema(schema_id: str):
    """Delete a LinkML schema."""
    db = current_app.config["DB"]
    deleted = db.delete_linkml_schema(schema_id)
    if not deleted:
        return jsonify({"error": "LinkML schema not found"}), 404
    return jsonify({"deleted": schema_id})
