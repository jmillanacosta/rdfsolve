# Docker Setup — rdfsolve

Run the full rdfsolve stack (Flask API + schema diagram frontend) with a single command.

## Architecture

```
┌─────────────────────────────────────────┐
│          Docker container (:5000)        │
│                                         │
│  Gunicorn + Flask                       │
│  ├─ /api/*        → REST API            │
│  ├─ /             → index.html          │
│  └─ /bundle.js    → frontend bundle     │
│                                         │
│  SQLite  (/app/data/rdfsolve.db)        │
│  Schemas (/app/schemas/*.jsonld)         │
└─────────────────────────────────────────┘
```

The Docker image is a **multi-stage build**:

1. **Stage 1 (Node 20)** — builds the TypeScript frontend with esbuild
2. **Stage 2 (Python 3.13)** — installs `rdfsolve[web]`, copies the built
   frontend, and serves everything with Gunicorn

## Quick Start

```bash
# 1. Ensure the frontend submodule is initialised
git submodule update --init

# 2. Build and start
docker compose up --build

# 3. Open the app
open http://localhost:5000
```

The app starts with **8 pre-seeded schemas** (WikiPathways, AOP-Wiki, PubChem, etc.)
that are automatically imported from `docker/schemas/` on first startup.

## Mine More Schemas

To add schemas from `data/sources.csv`:

```bash
# Mine all 88 sources (takes a while — queries live SPARQL endpoints)
python scripts/seed_schemas.py

# Mine specific datasets
python scripts/seed_schemas.py --names chembl drugbank cellosaurus

# Mine the first 5
python scripts/seed_schemas.py --limit 5
```

Schemas are saved to `docker/schemas/`. Since the directory is bind-mounted,
restart the container to pick up new schemas:

```bash
docker compose restart
```

Or upload schemas via the API without restarting:

```bash
curl -X POST http://localhost:5000/api/schemas/upload \
  -H "Content-Type: application/json" \
  -d @docker/schemas/chembl_schema.jsonld
```

## Environment Variables

| Variable           | Default                 | Description                                |
|--------------------|-------------------------|--------------------------------------------|
| `DATABASE_PATH`    | `/app/data/rdfsolve.db` | SQLite database file path                  |
| `SCHEMA_IMPORT_DIR`| `/app/schemas`          | Directory to bulk-import `.jsonld` files    |
| `FRONTEND_DIST`    | `/app/frontend`         | Directory with built frontend assets       |
| `CORS_ORIGINS`     | `*`                     | Allowed CORS origins (comma-separated)     |
| `SPARQL_TIMEOUT`   | `60`                    | Timeout for upstream SPARQL queries (sec)  |
| `FLASK_DEBUG`      | `0`                     | Set to `1` for debug mode                  |

## API Endpoints

| Method | Path                     | Description                            |
|--------|--------------------------|----------------------------------------|
| GET    | `/api/schemas/`          | List all schemas (id, name, metadata)  |
| GET    | `/api/schemas/{id}`      | Get full JSON-LD schema                |
| POST   | `/api/schemas/generate`  | Mine a schema from a SPARQL endpoint   |
| POST   | `/api/schemas/upload`    | Upload a JSON-LD schema file           |
| DELETE | `/api/schemas/{id}`      | Delete a schema                        |
| POST   | `/api/sparql/query`      | Proxy a SPARQL query to an endpoint    |
| POST   | `/api/iri/resolve`       | Resolve IRI types across endpoints     |
| POST   | `/api/compose/from-paths`| Generate SPARQL from diagram paths     |
| GET    | `/api/endpoints/`        | List discovered SPARQL endpoints       |
| POST   | `/api/endpoints/`        | Register a manual endpoint             |
| GET    | `/api/export/csv`        | Export query results as CSV            |
| GET    | `/api/health`            | Health check                           |

## Development (without Docker)

```bash
# Install with web extras
pip install -e ".[web]"

# Run Flask dev server
SCHEMA_IMPORT_DIR=docker/schemas python -m rdfsolve.backend.app
```

## Project Structure

```
rdfsolve-2/
├── frontend/                    ← git submodule (schema-diagram-ts)
│   ├── src/
│   │   ├── api-entry.ts         ← API-driven entry point (Docker)
│   │   ├── demo-entry.ts        ← standalone demo entry point
│   │   └── components/          ← web components (diagram, editor, etc.)
│   └── demo/                    ← demo HTML + pre-built schemas
├── src/rdfsolve/
│   └── backend/                 ← Flask API
│       ├── app.py               ← app factory
│       ├── database.py          ← SQLite layer
│       ├── routes/              ← API blueprints
│       └── services/            ← business logic
├── docker/
│   ├── Dockerfile               ← multi-stage build
│   └── schemas/                 ← seed JSON-LD schemas
├── docker-compose.yml
└── scripts/
    └── seed_schemas.py          ← mine schemas from sources.csv
```
