"""VoID catalog routes - /api/void/*."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, Response, current_app, jsonify, request

void_bp = Blueprint("void", __name__)


@void_bp.route("/", methods=["GET"])
def list_void_catalogs() -> Response:
    """List all VoID catalogs.

    Query parameter: ``dataset`` - filter by dataset name.
    """
    db = current_app.config["DB"]
    dataset = request.args.get("dataset")
    items = db.list_void_catalogs(dataset_name=dataset)
    return jsonify(items)


@void_bp.route("/<catalog_id>", methods=["GET"])
def get_void_catalog(catalog_id: str) -> Response | tuple[Response, Any]:
    """Return the VoID catalog as JSON (Turtle under ``turtle`` key)."""
    db = current_app.config["DB"]
    catalog = db.get_void_catalog(catalog_id)
    if catalog is None:
        return jsonify({"error": "VoID catalog not found"}), 404
    return jsonify(catalog)


@void_bp.route("/<catalog_id>.ttl", methods=["GET"])
def download_void_turtle(catalog_id: str) -> Response | tuple[Response, Any]:
    """Download the raw Turtle file for *catalog_id*."""
    db = current_app.config["DB"]
    catalog = db.get_void_catalog(catalog_id)
    if catalog is None:
        return jsonify({"error": "VoID catalog not found"}), 404
    turtle = catalog.get("data", "")
    return Response(
        turtle,
        mimetype="text/turtle",
        headers={
            "Content-Disposition": (f'attachment; filename="{catalog_id}.ttl"'),
        },
    )


@void_bp.route("/<catalog_id>/metadata", methods=["GET"])
def get_void_metadata(catalog_id: str) -> Response | tuple[Response, Any]:
    """Return only the captured dataset_metadata dict."""
    db = current_app.config["DB"]
    catalog = db.get_void_catalog(catalog_id)
    if catalog is None:
        return jsonify({"error": "VoID catalog not found"}), 404
    return jsonify(catalog.get("dataset_metadata") or {})


@void_bp.route("/", methods=["POST"])
def upload_void_catalog() -> Response | tuple[Response, Any]:
    """Upload a VoID catalog.

    Expects JSON body with ``meta`` (dict) and ``turtle`` (string),
    or a ``.ttl`` file upload under the ``file`` field (meta inferred
    from the Turtle filename).
    """
    db = current_app.config["DB"]

    if request.is_json:
        body = request.get_json(force=True)
        meta = body.get("meta", {})
        turtle = body.get("turtle", "")
    elif "file" in request.files:
        f = request.files["file"]
        turtle = f.read().decode("utf-8", errors="replace")
        fname: str = f.filename or "unknown_void.ttl"
        dataset_name = fname.replace(".ttl", "").replace("_void", "")
        meta = {"dataset_name": dataset_name}
    else:
        return jsonify(
            {"error": "No JSON body or file provided"},
        ), 400

    dataset_name = meta.get("dataset_name", "unknown")
    catalog_id = f"{dataset_name}_void"

    db.save_void_catalog(catalog_id, meta, turtle)
    return jsonify({"id": catalog_id}), 201


@void_bp.route("/<catalog_id>", methods=["DELETE"])
def delete_void_catalog(catalog_id: str) -> Response | tuple[Response, Any]:
    """Delete a VoID catalog."""
    db = current_app.config["DB"]
    deleted = db.delete_void_catalog(catalog_id)
    if not deleted:
        return jsonify({"error": "VoID catalog not found"}), 404
    return jsonify({"deleted": catalog_id})
