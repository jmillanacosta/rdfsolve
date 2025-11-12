"""Tests for the RDFSolve web interface."""

import pytest


def test_create_app():
    """Test that the Flask app can be created."""
    try:
        from rdfsolve.web import create_app
        
        app = create_app()
        assert app is not None
        assert app.config['DEBUG'] is True
        
    except ImportError:
        pytest.skip("Flask not available, skipping web interface tests")


def test_index_route():
    """Test the index route."""
    try:
        from rdfsolve.web import create_app
        
        app = create_app()
        client = app.test_client()
        
        response = client.get('/')
        assert response.status_code == 200
        assert b'RDFSolve' in response.data
        
    except ImportError:
        pytest.skip("Flask not available, skipping web interface tests")


def test_about_route():
    """Test the about route."""
    try:
        from rdfsolve.web import create_app
        
        app = create_app()
        client = app.test_client()
        
        response = client.get('/about')
        assert response.status_code == 200
        assert b'About RDFSolve' in response.data
        
    except ImportError:
        pytest.skip("Flask not available, skipping web interface tests")


def test_health_route():
    """Test the health check route."""
    try:
        from rdfsolve.web import create_app
        
        app = create_app()
        client = app.test_client()
        
        response = client.get('/api/health')
        assert response.status_code == 200
        
        json_data = response.get_json()
        assert json_data['status'] == 'ok'
        assert 'RDFSolve' in json_data['message']
        
    except ImportError:
        pytest.skip("Flask not available, skipping web interface tests")