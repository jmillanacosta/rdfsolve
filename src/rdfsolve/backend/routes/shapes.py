"""Shapes routes — /api/shapes/*."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from rdfsolve.shapes import jsonld_to_shacl, subset_jsonld

shapes_bp = Blueprint("shapes", __name__)


@shapes_bp.route("/subset", methods=["POST"])
def subset_schema():
    """Subset a JSON-LD schema by keeping only specified edges.

    Request body (JSON):
        schema_jsonld – full JSON-LD schema dict
        keep_edges    – list of {subject, predicate, object} edge specs
    """
    data = request.get_json(force=True)
    schema_jsonld = data.get("schema_jsonld")
    keep_edges = data.get("keep_edges", [])

    if not schema_jsonld:
        return jsonify({"error": "Missing 'schema_jsonld'"}), 400

    try:
        result = subset_jsonld(schema_jsonld, keep_edges)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    return jsonify(result)


@shapes_bp.route("/shacl", methods=["POST"])
def generate_shacl():
    """Convert a JSON-LD schema (or subset) to SHACL shapes.

    Request body (JSON):
        schema_jsonld – JSON-LD schema dict (full or subset)
        schema_name   – optional name for the schema (default "shapes")
        closed        – whether to generate closed shapes (default true)
    """
    data = request.get_json(force=True)
    schema_jsonld = data.get("schema_jsonld")
    schema_name = data.get("schema_name")
    closed = data.get("closed", True)

    if not schema_jsonld:
        return jsonify({"error": "Missing 'schema_jsonld'"}), 400

    try:
        shacl_ttl = jsonld_to_shacl(
            schema_jsonld,
            schema_name=schema_name,
            closed=closed,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    return jsonify({"shacl": shacl_ttl})
