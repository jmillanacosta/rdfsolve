"""Fixtures for backend tests."""

from __future__ import annotations

import pytest

from rdfsolve.backend.app import create_app
from rdfsolve.backend.config import TestConfig


@pytest.fixture()
def app():
    """Create a test Flask application."""
    application = create_app(TestConfig)
    yield application


@pytest.fixture()
def client(app):
    """Flask test client."""
    return app.test_client()


@pytest.fixture()
def db(app):
    """Direct access to the Database instance."""
    return app.config["DB"]
