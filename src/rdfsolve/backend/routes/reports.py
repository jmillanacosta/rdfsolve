"""Run-report routes — /api/reports/*."""

from __future__ import annotations

import json

from flask import Blueprint, current_app, jsonify, request

reports_bp = Blueprint("reports", __name__)


@reports_bp.route("/", methods=["GET"])
def list_reports():
    """List all run reports.

    Query parameters:
    - ``dataset``: filter by dataset name
    - ``strategy``: filter by strategy string
    """
    db = current_app.config["DB"]
    dataset = request.args.get("dataset")
    strategy = request.args.get("strategy")
    items = db.list_reports(
        dataset_name=dataset, strategy=strategy,
    )
    return jsonify(items)


@reports_bp.route("/<report_id>", methods=["GET"])
def get_report(report_id: str):
    """Return the full report JSON for *report_id*."""
    db = current_app.config["DB"]
    report = db.get_report(report_id)
    if report is None:
        return jsonify({"error": "Report not found"}), 404
    return jsonify(report)


@reports_bp.route("/", methods=["POST"])
def upload_report():
    """Upload / register a report JSON.

    Accepts either ``application/json`` body or a file upload
    under the ``file`` field.
    """
    db = current_app.config["DB"]
    data: dict | None = None

    if request.is_json:
        data = request.get_json(force=True)
    elif "file" in request.files:
        raw = request.files["file"].read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            return jsonify({"error": f"Invalid JSON: {exc}"}), 400
    else:
        return jsonify({"error": "No JSON body or file provided"}), 400

    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400

    dataset_name = data.get("dataset_name", "unknown")
    started_at = data.get("started_at", "")
    strategy = data.get("strategy", "unknown")
    report_id = f"{dataset_name}_{strategy}_{started_at}"

    db.save_report(report_id, data)
    return jsonify({"id": report_id}), 201


@reports_bp.route("/<report_id>", methods=["DELETE"])
def delete_report(report_id: str):
    """Delete a report."""
    db = current_app.config["DB"]
    deleted = db.delete_report(report_id)
    if not deleted:
        return jsonify({"error": "Report not found"}), 404
    return jsonify({"deleted": report_id})
