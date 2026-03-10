"""Flask application factory for the rdfsolve backend API."""

from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.security import safe_join

from rdfsolve.backend.config import Config
from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)

_DEFAULT_SECRET = "dev-key-change-in-prod"


def register_error_handlers(app: Flask) -> None:
    """Register consistent JSON error handlers."""

    @app.errorhandler(400)
    def bad_request(exc):
        # (7d) Sanitise exc.description in production.
        msg = (
            str(exc.description)
            if app.config.get("DEBUG")
            else "Bad request"
        )
        return jsonify({"error": msg}), 400

    @app.errorhandler(404)
    def not_found(exc):
        return jsonify({"error": "Resource not found"}), 404

    @app.errorhandler(502)
    def bad_gateway(exc):
        # (7d) Sanitise exc.description in production.
        details = (
            str(exc.description)
            if app.config.get("DEBUG")
            else "Upstream error"
        )
        return jsonify({
            "error": "Upstream SPARQL endpoint error",
            "details": details,
        }), 502

    @app.errorhandler(504)
    def gateway_timeout(exc):
        return jsonify({"error": "SPARQL endpoint timeout"}), 504

    @app.errorhandler(Exception)
    def unhandled(exc):
        app.logger.exception("Unhandled exception")
        return jsonify({"error": "Internal server error"}), 500


def create_app(config_class: type[Config] = Config) -> Flask:
    """Create and configure the Flask application.

    Parameters
    ----------
    config_class:
        Configuration class (default :class:`Config`).

    Returns
    -------
    Flask
        Configured Flask application.
    """
    app = Flask(__name__)
    app.config.from_object(config_class)

    # (7a) Require a non-default SECRET_KEY in production.
    if (
        not config_class.DEBUG
        and config_class.SECRET_KEY == _DEFAULT_SECRET
    ):
        raise RuntimeError(
            "SECRET_KEY must be set to a random value in production "
            "(FLASK_DEBUG=0). Set the SECRET_KEY environment variable."
        )

    # (7f) Upload size limit.
    app.config["MAX_CONTENT_LENGTH"] = config_class.MAX_CONTENT_LENGTH

    # ── CORS ──────────────────────────────────────────────────────────
    try:
        from flask_cors import CORS
        CORS(app, resources={
            r"/api/*": {
                "origins": config_class.CORS_ORIGINS,
                "methods": ["GET", "POST", "DELETE", "OPTIONS"],
                "allow_headers": [
                    "Content-Type", "Authorization",
                ],
            },
        })
    except ImportError:
        logger.warning(
            "flask-cors not installed — CORS headers disabled",
        )

    # ── Database ──────────────────────────────────────────────────────
    db = Database(config_class.DATABASE_PATH)
    app.config["DB"] = db

    @app.teardown_appcontext
    def _close_db(exc):
        db.close()

    # ── API token authentication (7e) ─────────────────────────────────
    api_token = config_class.API_TOKEN

    @app.before_request
    def _check_token():
        if not api_token:
            return None
        # Only protect mutating routes; allow GET and health check.
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return None
        if request.path == "/api/health":
            return None
        auth = request.headers.get("Authorization", "")
        if auth == f"Bearer {api_token}":
            return None
        return jsonify({"error": "Unauthorized"}), 401

    # ── Blueprints ────────────────────────────────────────────────────
    from rdfsolve.backend.routes.compose import compose_bp
    from rdfsolve.backend.routes.endpoints import endpoints_bp
    from rdfsolve.backend.routes.export import export_bp
    from rdfsolve.backend.routes.iri import iri_bp
    from rdfsolve.backend.routes.linkml import linkml_bp
    from rdfsolve.backend.routes.mappings import mappings_bp
    from rdfsolve.backend.routes.reports import reports_bp
    from rdfsolve.backend.routes.schemas import schemas_bp
    from rdfsolve.backend.routes.shapes import shapes_bp
    from rdfsolve.backend.routes.sparql import sparql_bp
    from rdfsolve.backend.routes.void_catalogs import void_bp

    app.register_blueprint(schemas_bp, url_prefix="/api/schemas")
    app.register_blueprint(sparql_bp, url_prefix="/api/sparql")
    app.register_blueprint(iri_bp, url_prefix="/api/iri")
    app.register_blueprint(compose_bp, url_prefix="/api/compose")
    app.register_blueprint(endpoints_bp, url_prefix="/api/endpoints")
    app.register_blueprint(export_bp, url_prefix="/api/export")
    app.register_blueprint(shapes_bp, url_prefix="/api/shapes")
    app.register_blueprint(mappings_bp, url_prefix="/api/mappings")
    app.register_blueprint(reports_bp, url_prefix="/api/reports")
    app.register_blueprint(void_bp, url_prefix="/api/void")
    app.register_blueprint(linkml_bp, url_prefix="/api/linkml")

    # ── Error handlers ────────────────────────────────────────────────
    register_error_handlers(app)

    # ── Health check ──────────────────────────────────────────────────
    @app.route("/api/health")
    def health():
        return jsonify({"status": "ok"})

    # ── Frontend serving ──────────────────────────────────────────────
    frontend_dist = config_class.FRONTEND_DIST
    if frontend_dist:
        dist = Path(frontend_dist).resolve()
        if dist.is_dir():
            logger.info("Serving frontend from %s", dist)

            @app.route("/")
            def serve_index():
                return send_from_directory(str(dist), "index.html")

            @app.route("/<path:filename>")
            def serve_static(filename):
                # (7g) Validate that the resolved path stays within dist.
                safe = safe_join(str(dist), filename)
                if safe is None:
                    return jsonify({"error": "Not found"}), 404
                return send_from_directory(str(dist), filename)
        else:
            logger.warning(
                "FRONTEND_DIST=%s is not a directory", dist,
            )

    # ── Bulk-import from disk (on first startup) ──────────────────────
    import_dir = config_class.SCHEMA_IMPORT_DIR
    if import_dir:
        from rdfsolve.backend.services.schema_service import (
            SchemaService,
        )
        svc = SchemaService(db)
        n = svc.import_from_directory(import_dir)
        if n:
            logger.info("Imported %d schemas from %s", n, import_dir)

    return app


if __name__ == "__main__":
    app = create_app()
    port = app.config.get("PORT", Config.PORT)
    app.run(host="0.0.0.0", port=port, debug=True)
