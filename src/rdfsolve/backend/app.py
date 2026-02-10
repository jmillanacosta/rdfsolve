"""Flask application factory for the rdfsolve backend API."""

from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask, jsonify, send_from_directory

from rdfsolve.backend.config import Config
from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)


def register_error_handlers(app: Flask) -> None:
    """Register consistent JSON error handlers."""

    @app.errorhandler(400)
    def bad_request(exc):
        return jsonify({"error": str(exc.description)}), 400

    @app.errorhandler(404)
    def not_found(exc):
        return jsonify({"error": "Resource not found"}), 404

    @app.errorhandler(502)
    def bad_gateway(exc):
        return jsonify({
            "error": "Upstream SPARQL endpoint error",
            "details": str(exc.description),
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

    # ── CORS ──────────────────────────────────────────────────────────
    try:
        from flask_cors import CORS
        CORS(app, resources={
            r"/api/*": {
                "origins": config_class.CORS_ORIGINS,
                "methods": ["GET", "POST", "DELETE", "OPTIONS"],
                "allow_headers": ["Content-Type", "Authorization"],
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

    # ── Blueprints ────────────────────────────────────────────────────
    from rdfsolve.backend.routes.compose import compose_bp
    from rdfsolve.backend.routes.endpoints import endpoints_bp
    from rdfsolve.backend.routes.export import export_bp
    from rdfsolve.backend.routes.iri import iri_bp
    from rdfsolve.backend.routes.schemas import schemas_bp
    from rdfsolve.backend.routes.sparql import sparql_bp

    app.register_blueprint(schemas_bp, url_prefix="/api/schemas")
    app.register_blueprint(sparql_bp, url_prefix="/api/sparql")
    app.register_blueprint(iri_bp, url_prefix="/api/iri")
    app.register_blueprint(compose_bp, url_prefix="/api/compose")
    app.register_blueprint(endpoints_bp, url_prefix="/api/endpoints")
    app.register_blueprint(export_bp, url_prefix="/api/export")

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
    create_app().run(host="0.0.0.0", port=5000, debug=True)
