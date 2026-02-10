# Frontend Integration Guide

## Architecture

```
schema-diagram-ts/          ← separate repo (TypeScript)
├── src/                    ← Web Components source
├── demo/
│   ├── index.html          ← entry point
│   └── dist/
│       └── bundle.js       ← webpack output
└── package.json

rdfsolve/                   ← this repo (Python)
└── src/rdfsolve/backend/   ← Flask API
```

The two repos stay **independent**. Flask serves the built frontend
files and proxies all `/api/*` requests. No CORS needed in production
because everything is on the same origin.

---

## Quick Start (development)

### 1. Build the frontend

```bash
cd /path/to/schema-diagram-ts
npm install
npm run build          # produces demo/dist/bundle.js
```

### 2. Point Flask at the frontend build

```bash
cd /path/to/rdfsolve
export FRONTEND_DIST=/path/to/schema-diagram-ts/demo
export SCHEMA_IMPORT_DIR=/path/to/rdfsolve/test_mine   # optional: pre-load schemas

pip install -e ".[web]"
python -m rdfsolve.backend.app
```

Open <http://localhost:5000> — Flask serves `index.html` + `bundle.js`
from the frontend repo and handles all `/api/*` calls.

### 3. Alternative: symlink during development

```bash
# From the rdfsolve repo root
ln -s /path/to/schema-diagram-ts/demo frontend_dist
export FRONTEND_DIST=frontend_dist
python -m rdfsolve.backend.app
```

---

## What needs to change in the frontend

In `demo-entry.ts` (or wherever the API base is configured), set the
base URL to be **relative** so it works on the same origin:

```typescript
// Before (hardcoded / direct SPARQL calls):
// const resp = await fetch('https://sparql.wikipathways.org/sparql?query=...');

// After (through Flask proxy):
const API_BASE = '';   // same origin — no prefix needed

// Load schemas
const schemas = await fetch(`${API_BASE}/api/schemas/`).then(r => r.json());
const schema  = await fetch(`${API_BASE}/api/schemas/${id}`).then(r => r.json());

// Execute SPARQL (proxied — no CORS issues)
const result = await fetch(`${API_BASE}/api/sparql/query`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ query, endpoint, variable_map }),
}).then(r => r.json());

// Resolve IRIs
const resolved = await fetch(`${API_BASE}/api/iri/resolve`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ iris }),
}).then(r => r.json());
```

### Component-by-component changes

| Component | Current behavior | Change to |
|-----------|-----------------|-----------|
| `dataset-selector.ts` | Loads local `.jsonld` files | `GET /api/schemas/` + `GET /api/schemas/{id}` |
| `results-panel.ts` | Direct `fetch()` to SPARQL endpoints | `POST /api/sparql/query` |
| `iri-resolver.ts` | Direct `fetch()` to SPARQL endpoints | `POST /api/iri/resolve` |
| `sparql-editor.ts` | Displays query text | `POST /api/export/query` for JSON-LD export |
| `composer.ts` | Generates query client-side | Can also use `POST /api/compose/from-paths` |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FRONTEND_DIST` | _(empty)_ | Path to the frontend `demo/` directory |
| `DATABASE_PATH` | `rdfsolve.db` | SQLite database file path |
| `SCHEMA_IMPORT_DIR` | _(empty)_ | Directory of `.jsonld` files to import on startup |
| `SPARQL_TIMEOUT` | `30` | Max seconds for proxied SPARQL queries |
| `CORS_ORIGINS` | `http://localhost:*` | Comma-separated allowed origins (dev only) |
| `FLASK_DEBUG` | `0` | Set to `1` for debug mode |

---

## Production Deployment

```bash
pip install rdfsolve[web]

# Build frontend once
cd /path/to/schema-diagram-ts && npm run build

# Run with gunicorn
FRONTEND_DIST=/path/to/schema-diagram-ts/demo \
DATABASE_PATH=/var/lib/rdfsolve/rdfsolve.db \
gunicorn "rdfsolve.backend.app:create_app()" \
  --bind 0.0.0.0:5000 \
  --workers 4 \
  --timeout 60
```

### Docker

```dockerfile
FROM python:3.12-slim

# Install backend
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir ".[web]"

# Copy pre-built frontend
COPY --from=frontend /app/demo /app/frontend_dist

ENV FRONTEND_DIST=/app/frontend_dist
ENV DATABASE_PATH=/data/rdfsolve.db

EXPOSE 5000
CMD ["gunicorn", "rdfsolve.backend.app:create_app()", \
     "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "60"]
```
