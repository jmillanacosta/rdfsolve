"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Config:
    """Default configuration for the Flask backend."""

    SECRET_KEY = os.getenv("SECRET_KEY", "dev-key-change-in-prod")
    DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"

    # Raise at startup in production if SECRET_KEY is still the default.
    # (7a) Predictable SECRET_KEY fallback guard — checked in create_app.

    # Server port — single source of truth for all runners.
    PORT = int(os.getenv("PORT", "8000"))

    # CORS — enumerate explicit origins (comma-separated).
    # Empty string disables CORS for non-localhost origins.
    # (7c) Enumerate explicit origins; avoid bare glob wildcards.
    CORS_ORIGINS = [
        o.strip()
        for o in os.getenv(
            "CORS_ORIGINS", "http://localhost:3000,http://localhost:8000",
        ).split(",")
        if o.strip()
    ]

    # SQLite database path
    DATABASE_PATH = os.getenv(
        "DATABASE_PATH", "rdfsolve.db",
    )

    # SPARQL proxy defaults
    SPARQL_TIMEOUT = int(os.getenv("SPARQL_TIMEOUT", "30"))
    SPARQL_MAX_ROWS = int(os.getenv("SPARQL_MAX_ROWS", "10000"))

    # (7b) SPARQL endpoint allowlist.
    # "" = strict mode (no proxy allowed unless endpoint listed).
    # "*" = open proxy (document as known trade-off).
    # Comma-separated URLs = explicit allowlist.
    SPARQL_ALLOWED_ORIGINS = os.getenv("SPARQL_ALLOWED_ORIGINS", "*")

    # Cache TTL in seconds (0 = disabled)
    CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))

    # rdfsolve strategy for schema mining
    RDFSOLVE_STRATEGY = os.getenv("RDFSOLVE_STRATEGY", "miner")

    # Optional: directory to bulk-import existing JSON-LD schemas
    SCHEMA_IMPORT_DIR = os.getenv("SCHEMA_IMPORT_DIR", "")

    # Path to the built frontend dist/ directory.
    # Set to the schema-diagram-ts build output, e.g.:
    #   FRONTEND_DIST=/path/to/schema-diagram-ts/demo/dist
    FRONTEND_DIST = os.getenv("FRONTEND_DIST", "")

    # (7e) Optional API token for mutating routes.
    # "" = disabled (all routes unauthenticated).
    # Set to a random string to require Authorization: Bearer <token>.
    API_TOKEN = os.getenv("API_TOKEN", "")

    # (7f) Maximum upload size in bytes.
    MAX_CONTENT_LENGTH = (
        int(os.getenv("MAX_UPLOAD_MB", "32")) * 1024 * 1024
    )


class TestConfig(Config):
    """Configuration overrides for testing."""

    TESTING = True
    DATABASE_PATH = ":memory:"
    CACHE_TTL = 0
