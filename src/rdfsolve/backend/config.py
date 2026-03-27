"""Application configuration — layered by environment.

Usage
-----
The active config class is selected by the ``APP_ENV`` environment variable:

    APP_ENV=development   →  DevelopmentConfig  (default)
    APP_ENV=testing       →  TestConfig
    APP_ENV=production    →  ProductionConfig

Pass the result of :func:`get_config` to ``create_app``::

    from rdfsolve.backend.config import get_config
    from rdfsolve.backend.app import create_app

    app = create_app(get_config())

Environment variables
---------------------
All settings are read from the environment at import time.
In production, create a ``.env`` file (or inject via your process manager)
and **never** commit secrets to version control.

See ``docs/notes/deployment.md`` for a full production checklist.
"""

from __future__ import annotations

import os
from typing import ClassVar


def _cors_origins(default: str = "") -> list[str]:
    """Parse ``CORS_ORIGINS`` env var into a list of origin strings."""
    raw = os.getenv("CORS_ORIGINS", default)
    return [o.strip() for o in raw.split(",") if o.strip()]


def _require(name: str) -> str:
    """Return the value of *name* from the environment, raising if absent.

    Called at *app-creation time* (not import time) via
    :meth:`Config.validate`.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"See docs/notes/deployment.md for setup instructions."
        )
    return value


# ---------------------------------------------------------------------------
# Base — shared defaults (never instantiated directly)
# ---------------------------------------------------------------------------


class Config:
    """Shared base configuration.

    Subclass this for each environment; override only what differs.
    """

    # Flask internals
    DEBUG: bool = False
    TESTING: bool = False
    SECRET_KEY: str = os.getenv("SECRET_KEY", "")

    # Server
    PORT: int = int(os.getenv("PORT", "8000"))

    # CORS — empty list = same-origin only (safe default)
    CORS_ORIGINS: ClassVar[list[str]] = _cors_origins()

    # Database
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "rdfsolve.db")

    # SPARQL proxy
    SPARQL_TIMEOUT: int = int(os.getenv("SPARQL_TIMEOUT", "30"))
    SPARQL_MAX_ROWS: int = int(os.getenv("SPARQL_MAX_ROWS", "10000"))
    # "*" = open proxy; "" = strict (no proxy); comma-list = allowlist
    SPARQL_ALLOWED_ORIGINS: str = os.getenv("SPARQL_ALLOWED_ORIGINS", "*")

    # Cache
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "300"))

    # rdfsolve
    RDFSOLVE_STRATEGY: str = os.getenv("RDFSOLVE_STRATEGY", "miner")

    # Optional bulk-import directory
    SCHEMA_IMPORT_DIR: str = os.getenv("SCHEMA_IMPORT_DIR", "")

    # Sources YAML registry (seeded to DB on startup when table is empty)
    SOURCES_YAML: str = os.getenv("SOURCES_YAML", "")

    # OLS4 HTTP-response disk cache directory for the ontology index builder
    ONTOLOGY_CACHE_DIR: str = os.getenv("ONTOLOGY_CACHE_DIR", "")

    # Frontend static build directory
    FRONTEND_DIST: str = os.getenv("FRONTEND_DIST", "")

    # API token for mutating routes ("" = disabled)
    API_TOKEN: str = os.getenv("API_TOKEN", "")

    # Upload size limit
    MAX_CONTENT_LENGTH: int = int(os.getenv("MAX_UPLOAD_MB", "32")) * 1024 * 1024

    @classmethod
    def validate(cls) -> None:
        """Assert that all required settings are present.

        Called by :func:`~rdfsolve.backend.app.create_app` before the
        Flask app is fully configured.  The base implementation is a
        no-op; :class:`ProductionConfig` overrides it to raise
        :exc:`RuntimeError` for any missing required variable.
        """


# ---------------------------------------------------------------------------
# Development — safe defaults, no secrets required
# ---------------------------------------------------------------------------


class DevelopmentConfig(Config):
    """Local development configuration.

    Enables debug mode and sets safe placeholder values so the app
    starts without any environment variables.
    """

    DEBUG = True
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-only-insecure-key")
    CORS_ORIGINS: ClassVar[list[str]] = _cors_origins("http://localhost:3000,http://localhost:8000")
    CACHE_TTL = 0


# ---------------------------------------------------------------------------
# Testing — in-memory database, caching off
# ---------------------------------------------------------------------------


class TestConfig(Config):
    """Configuration for the automated test suite."""

    TESTING = True
    DEBUG = True
    SECRET_KEY = "test-secret"  # noqa: S105
    DATABASE_PATH = ":memory:"
    CACHE_TTL = 0


# ---------------------------------------------------------------------------
# Production — all secrets required; no insecure defaults
# ---------------------------------------------------------------------------


class ProductionConfig(Config):
    """Production configuration.

    All sensitive values **must** be provided via environment variables.
    The app will refuse to start if they are missing or empty — but the
    check is deferred to :meth:`validate` (called by ``create_app``) so
    that importing this module never raises.

    Required env vars: ``SECRET_KEY``, ``API_TOKEN``, ``CORS_ORIGINS``.
    """

    DEBUG = False

    # Read from env at import time; validated (non-empty) at app-creation
    # time via validate() below.
    SECRET_KEY: str = os.getenv("SECRET_KEY", "")
    API_TOKEN: str = os.getenv("API_TOKEN", "")

    CORS_ORIGINS: ClassVar[list[str]] = _cors_origins()

    # Tighten SPARQL proxy in production unless explicitly opened
    SPARQL_ALLOWED_ORIGINS: str = os.getenv("SPARQL_ALLOWED_ORIGINS", "")

    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "300"))

    @classmethod
    def validate(cls) -> None:
        """Raise :exc:`RuntimeError` if any required env var is missing."""
        _require("SECRET_KEY")
        _require("API_TOKEN")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_ENV_MAP: dict[str, type[Config]] = {
    "development": DevelopmentConfig,
    "testing": TestConfig,
    "production": ProductionConfig,
}


def get_config() -> type[Config]:
    """Return the config class for the current ``APP_ENV``.

    Defaults to :class:`DevelopmentConfig` when ``APP_ENV`` is unset.

    Raises
    ------
    ValueError
        If ``APP_ENV`` is set to an unrecognised value.
    """
    env = os.getenv("APP_ENV", "development").lower().strip()
    config_class = _ENV_MAP.get(env)
    if config_class is None:
        valid = ", ".join(sorted(_ENV_MAP))
        raise ValueError(f"Unknown APP_ENV={env!r}. Valid values: {valid}")
    return config_class
