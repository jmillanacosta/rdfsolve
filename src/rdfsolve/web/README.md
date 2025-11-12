# RDFSolve Web Interface

This directory contains the Flask web interface for RDFSolve.

## Structure

```
web/
├── __init__.py          # Package initialization
├── app.py              # Flask application factory
├── templates/          # Jinja2 templates
│   ├── base.html       # Base template with navigation
│   ├── index.html      # Home page
│   └── about.html      # About page
└── static/             # Static assets
    ├── css/
    │   └── style.css   # Custom styles
    └── js/
        └── main.js     # JavaScript functionality
```

## Running the Web Interface

### Option 1: Using the CLI command
```bash
rdfsolve web --host 0.0.0.0 --port 5000 --debug
```

### Option 2: Using the standalone app.py
```bash
python app.py
```

### Option 3: Using the web module directly
```python
from rdfsolve.web import create_app

app = create_app()
app.run(debug=True)
```

## Environment Variables

- `FLASK_DEBUG`: Enable/disable debug mode (default: True)
- `FLASK_HOST`: Host to bind to (default: 0.0.0.0)
- `FLASK_PORT`: Port to bind to (default: 5000)

## Features

- **Bootstrap 5**: Modern, responsive UI framework
- **Template inheritance**: Clean, maintainable templates
- **Static file serving**: CSS and JavaScript assets
- **Health check endpoint**: `/api/health` for monitoring
- **Extensible structure**: Easy to add new routes and features

## Development


## Templates

Templates use Jinja2 templating engine with Bootstrap 5 for styling:

- `base.html`: Contains navigation, footer, and basic layout
- `index.html`: Home page with feature overview
- `about.html`: Information about RDFSolve

## Static Assets

- `style.css`: Custom CSS with RDFSolve branding and responsive design
- `main.js`: JavaScript for interactive features and UI enhancements