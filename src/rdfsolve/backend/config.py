"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Config:
    """Default configuration for the Flask backend."""

    SECRET_KEY = os.getenv("SECRET_KEY", "dev-key-change-in-prod")
    DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"

    # CORS — origins allowed to call this API
    CORS_ORIGINS = os.getenv(
        "CORS_ORIGINS", "http://localhost:*",
    ).split(",")

    # SQLite database path
    DATABASE_PATH = os.getenv(
        "DATABASE_PATH", "rdfsolve.db",
    )

    # SPARQL proxy defaults
    SPARQL_TIMEOUT = int(os.getenv("SPARQL_TIMEOUT", "30"))
    SPARQL_MAX_ROWS = int(os.getenv("SPARQL_MAX_ROWS", "10000"))

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


class TestConfig(Config):
    """Configuration overrides for testing."""

    TESTING = True
    DATABASE_PATH = ":memory:"
    CACHE_TTL = 0
