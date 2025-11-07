#!/usr/bin/env python3
"""Standalone Flask application for RDFSolve web interface.

This script provides an easy way to run the RDFSolve web interface
during development.

Usage:
    python app.py

The web interface will be available at http://localhost:5000
"""

import os
import sys
from pathlib import Path

# Add the src directory to the Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from rdfsolve.web import create_app  # noqa: E402


def main():
    """Run the Flask development server."""
    app = create_app()
    
    # Get configuration from environment variables
    debug = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '5000'))
    
    print("Starting RDFSolve web interface...")
    print(f"Server will be available at: http://localhost:{port}")
    print(f"Debug mode: {debug}")
    
    app.run(debug=debug, host=host, port=port)


if __name__ == '__main__':
    main()