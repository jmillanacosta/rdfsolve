"""Flask application factory."""

from flask import Flask, render_template
from pathlib import Path


def create_app():
    """Create and configure the Flask application."""
    # Get the template directory relative to this file
    template_dir = Path(__file__).parent / "templates"
    static_dir = Path(__file__).parent / "static"
    
    app = Flask(__name__,
                template_folder=str(template_dir),
                static_folder=str(static_dir))
    
    # Basic configuration
    app.config.from_mapping(
        SECRET_KEY='dev-secret-key-change-in-production',
        DEBUG=True,
    )
    
    # Register routes
    @app.route('/')
    def index():
        """Home page."""
        return render_template('index.html')
    
    @app.route('/about')
    def about():
        """About page."""
        return render_template('about.html')
    
    @app.route('/api/health')
    def health():
        """Health check endpoint."""
        return {'status': 'ok', 'message': 'RDFSolve web interface is running'}
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)