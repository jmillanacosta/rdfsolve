"""Flask application factory for the rdfsolve backend API."""

from __future__ import annotations

import logging
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory
from werkzeug.exceptions import HTTPException
from werkzeug.security import safe_join

from rdfsolve.backend.config import Config
from rdfsolve.backend.database import Database

logger = logging.getLogger(__name__)


def register_error_handlers(app: Flask) -> None:
    """Register consistent JSON error handlers."""

    @app.errorhandler(400)
    def bad_request(exc: HTTPException) -> tuple[Response, int]:
        """Return a 400 JSON error response."""
        msg = str(exc.description) if app.config.get("DEBUG") else "Bad request"
        return jsonify({"error": msg}), 400

    @app.errorhandler(404)
    def not_found(exc: HTTPException) -> tuple[Response, int]:
        """Return a 404 JSON error response."""
        return jsonify({"error": "Resource not found"}), 404

    @app.errorhandler(502)
    def bad_gateway(exc: HTTPException) -> tuple[Response, int]:
        """Return a 502 JSON error response with upstream details."""
        details = str(exc.description) if app.config.get("DEBUG") else "Upstream error"
        return jsonify(
            {
                "error": "Upstream SPARQL endpoint error",
                "details": details,
            }
        ), 502

    @app.errorhandler(504)
    def gateway_timeout(exc: HTTPException) -> tuple[Response, int]:
        """Return a 504 JSON error response."""
        return jsonify({"error": "SPARQL endpoint timeout"}), 504

    @app.errorhandler(Exception)
    def unhandled(exc: Exception) -> tuple[Response, int]:
        """Log and return a 500 JSON error response."""
        app.logger.exception("Unhandled exception")
        return jsonify({"error": "Internal server error"}), 500


def _configure_cors(app: Flask, config_class: type[Config]) -> None:
    """Install flask-cors if available; log a warning otherwise."""
    try:
        from flask_cors import CORS

        CORS(
            app,
            resources={
                r"/api/*": {
                    "origins": config_class.CORS_ORIGINS or [],
                    "methods": ["GET", "POST", "DELETE", "OPTIONS"],
                    "allow_headers": ["Content-Type", "Authorization"],
                },
            },
        )
    except ImportError:
        logger.warning("flask-cors not installed - CORS headers disabled")


def _configure_db(app: Flask, config_class: type[Config]) -> Database:
    """Create the Database instance and register teardown."""
    db = Database(config_class.DATABASE_PATH)
    app.config["DB"] = db

    @app.teardown_appcontext
    def _close_db(exc: BaseException | None) -> None:
        db.close()

    return db


def _configure_auth(app: Flask, api_token: str) -> None:
    """Register the before_request token guard (no-op when token is empty)."""

    @app.before_request
    def _check_token() -> tuple[Response, int] | None:
        if not api_token:
            return None
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return None
        if request.path == "/api/health":
            return None
        auth = request.headers.get("Authorization", "")
        if auth == f"Bearer {api_token}":
            return None
        return jsonify({"error": "Unauthorized"}), 401


def _register_blueprints(app: Flask) -> None:
    """Import and register all route blueprints."""
    from rdfsolve.backend.routes.compose import compose_bp
    from rdfsolve.backend.routes.endpoints import endpoints_bp
    from rdfsolve.backend.routes.export import export_bp
    from rdfsolve.backend.routes.iri import iri_bp
    from rdfsolve.backend.routes.linkml import linkml_bp
    from rdfsolve.backend.routes.mappings import mappings_bp
    from rdfsolve.backend.routes.ontology import ontology_bp
    from rdfsolve.backend.routes.reports import reports_bp
    from rdfsolve.backend.routes.schemas import schemas_bp
    from rdfsolve.backend.routes.shapes import shapes_bp
    from rdfsolve.backend.routes.sources import sources_bp
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
    app.register_blueprint(sources_bp, url_prefix="/api/sources")
    app.register_blueprint(ontology_bp, url_prefix="/api/ontology")


def _configure_frontend(app: Flask, frontend_dist: str) -> None:
    """Serve the compiled frontend bundle if FRONTEND_DIST is set."""
    if not frontend_dist:
        return
    dist = Path(frontend_dist).resolve()
    if not dist.is_dir():
        logger.warning("FRONTEND_DIST=%s is not a directory", dist)
        return

    logger.info("Serving frontend from %s", dist)

    @app.route("/")
    def serve_index() -> Response:
        """Serve the frontend ``index.html``."""
        return send_from_directory(str(dist), "index.html")

    @app.route("/<path:filename>")
    def serve_static(filename: str) -> Response | tuple[Response, int]:
        """Serve a static asset from the frontend dist directory."""
        safe = safe_join(str(dist), filename)
        if safe is None:
            return jsonify({"error": "Not found"}), 404
        return send_from_directory(str(dist), filename)


def _bulk_import_schemas(db: Database, import_dir: str) -> None:
    """Import JSON-LD schemas from disk on first startup."""
    if not import_dir:
        return
    from rdfsolve.backend.services.schema_service import SchemaService

    n = SchemaService(db).import_from_directory(import_dir)
    if n:
        logger.info("Imported %d schemas from %s", n, import_dir)


def _seed_sources(db: Database, sources_yaml: str) -> None:
    """Seed the sources registry from YAML on first startup (no-op if already populated)."""
    if not sources_yaml:
        return
    from rdfsolve.backend.services.sources_service import SourcesService

    SourcesService(db).seed_from_yaml(sources_yaml)


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
    config_class.validate()
    app.config.from_object(config_class)
    app.config["MAX_CONTENT_LENGTH"] = config_class.MAX_CONTENT_LENGTH

    _configure_cors(app, config_class)
    db = _configure_db(app, config_class)
    _configure_auth(app, config_class.API_TOKEN)
    _register_blueprints(app)
    register_error_handlers(app)

    @app.route("/api/health")
    def health() -> Response:
        """
        Check app for health.

        :return: Status for health.
        """
        return jsonify({"status": "ok"})

    _configure_frontend(app, config_class.FRONTEND_DIST)
    _bulk_import_schemas(db, config_class.SCHEMA_IMPORT_DIR)
    _seed_sources(db, config_class.SOURCES_YAML)

    return app


if __name__ == "__main__":
    from rdfsolve.backend.config import get_config

    _app = create_app(get_config())
    _port = _app.config.get("PORT", Config.PORT)
    _host = "0.0.0.0"  # noqa: S104
    _app.run(host=_host, port=_port, debug=_app.config.get("DEBUG", False))
