"""Instance mapping routes — /api/mappings/*."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from rdfsolve.backend.services.mapping_service import MappingService

mappings_bp = Blueprint("mappings", __name__)


def _get_svc() -> MappingService:
    return MappingService(current_app.config["DB"])


@mappings_bp.route("/", methods=["GET"])
def list_mappings():
    """List all stored instance mappings (strategy=instance_matcher).

    Returns
    -------
    JSON array of lightweight mapping metadata dicts, e.g.::

        [
          {
            "id": "ensembl_instance_mapping",
            "name": "ensembl_instance_mapping",
            "endpoint": "",
            "pattern_count": 4,
            "strategy": "instance_matcher",
            "generated_at": "2026-02-19T..."
          }
        ]
    """
    return jsonify(_get_svc().list_mappings())


@mappings_bp.route("/<mapping_id>", methods=["GET"])
def get_mapping(mapping_id: str):
    """Return the full JSON-LD for a stored mapping.

    Parameters
    ----------
    mapping_id:
        e.g. ``ensembl_instance_mapping``.
    """
    data = _get_svc().get_mapping(mapping_id)
    if data is None:
        return jsonify({"error": f"Mapping '{mapping_id}' not found"}), 404
    return jsonify(data)


@mappings_bp.route("/probe", methods=["POST"])
def probe():
    """Probe endpoints for a bioregistry resource and return/save the mapping.

    Request body (JSON)
    -------------------
    prefix : str, required
        Bioregistry prefix to probe (e.g. ``"ensembl"``).
    predicate : str, optional
        Mapping predicate URI.
        Default: ``http://www.w3.org/2004/02/skos/core#narrowMatch``.
    dataset_names : list[str], optional
        Restrict probing to these dataset names.
    sources_csv : str, optional
        Path to the data sources CSV.  Default: ``data/sources.csv``.
    timeout : float, optional
        SPARQL request timeout in seconds.  Default: ``60.0``.
    save : bool, optional
        Persist the result to the database.  Default: ``true``.

    Returns
    -------
    201 JSON-LD mapping document.  When *save* is ``true`` the key
    ``_saved_as`` is included with the mapping id.

    Errors
    ------
    400 if ``prefix`` is missing.
    502 if the probe raises an exception.
    """
    body = request.get_json(force=True) or {}

    prefix = body.get("prefix", "").strip()
    if not prefix:
        return jsonify({"error": "Missing required field 'prefix'"}), 400

    predicate = body.get(
        "predicate",
        "http://www.w3.org/2004/02/skos/core#narrowMatch",
    )
    dataset_names = body.get("dataset_names") or None
    timeout = float(body.get("timeout", 60.0))
    save = body.get("save", True)

    svc = _get_svc()
    try:
        mapping_jsonld = svc.probe(
            prefix=prefix,
            predicate=predicate,
            dataset_names=dataset_names,
            timeout=timeout,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    if save:
        mid = svc.save_mapping(prefix, mapping_jsonld)
        mapping_jsonld["_saved_as"] = mid

    return jsonify(mapping_jsonld), 201


@mappings_bp.route("/<mapping_id>", methods=["DELETE"])
def delete_mapping(mapping_id: str):
    """Delete a stored mapping.

    Parameters
    ----------
    mapping_id:
        e.g. ``ensembl_instance_mapping``.
    """
    if not _get_svc().delete_mapping(mapping_id):
        return jsonify({"error": f"Mapping '{mapping_id}' not found"}), 404
    return jsonify({"message": f"Deleted {mapping_id}"}), 200
