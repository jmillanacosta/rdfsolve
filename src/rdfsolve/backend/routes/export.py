"""Export routes — /api/export/*."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

export_bp = Blueprint("export", __name__)


@export_bp.route("/query", methods=["POST"])
def export_query_jsonld():
    """Export a SPARQL query as sh:SPARQLExecutable JSON-LD."""
    data = request.get_json(force=True)
    query = data.get("query", "")
    query_type = data.get("query_type", "select")
    prefixes = data.get("prefixes", {})
    endpoint = data.get("endpoint")
    description = data.get("description")

    type_name = query_type.capitalize()
    now = datetime.now(timezone.utc)

    result = {
        "@context": {
            **prefixes,
            "sh": "http://www.w3.org/ns/shacl#",
            "schema": "https://schema.org/",
            "sd": (
                "http://www.w3.org/ns/"
                "sparql-service-description#"
            ),
        },
        "@id": f"_:query_{int(now.timestamp() * 1000)}",
        "@type": [
            "sh:SPARQLExecutable",
            f"sh:SPARQL{type_name}Executable",
        ],
        f"sh:{query_type}": query,
        "sh:prefixes": prefixes,
        "schema:dateCreated": now.isoformat(),
    }

    if description:
        result["schema:description"] = description

    if endpoint:
        result["schema:target"] = {
            "@type": "sd:Service",
            "sd:endpoint": endpoint,
        }

    return jsonify(result)


@export_bp.route("/results", methods=["POST"])
def export_results_jsonld():
    """Export SPARQL query results as JSON-LD with provenance."""
    data = request.get_json(force=True)
    now = datetime.now(timezone.utc)

    result = {
        "@context": {
            "schema": "https://schema.org/",
            "sd": (
                "http://www.w3.org/ns/"
                "sparql-service-description#"
            ),
            "prov": "http://www.w3.org/ns/prov#",
        },
        "@type": "schema:Dataset",
        "schema:dateCreated": now.isoformat(),
        "prov:wasGeneratedBy": {
            "@type": "prov:Activity",
            "prov:used": {
                "@type": "sd:Service",
                "sd:endpoint": data.get("endpoint", ""),
            },
            "schema:query": data.get("query", ""),
        },
        "schema:variablesMeasured": data.get("variables", []),
        "schema:size": data.get("row_count", 0),
        "schema:data": data.get("rows", []),
    }

    return jsonify(result)
