# rdfsolve — unified Dockerfile (backend + frontend)
#
# Multi-stage build:
#   Stage 1: Build the TypeScript frontend bundle with Node + esbuild
#   Stage 2: Install Python backend and serve everything with Gunicorn
#
# The frontend lives in the repo as a git submodule at ./frontend/

# ── Stage 1: Frontend build ──────────────────────────────────────
FROM node:20-alpine AS frontend-builder

WORKDIR /build

# Install JS dependencies
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --ignore-scripts

# Copy TS sources + HTML
COPY frontend/tsconfig.json ./
COPY frontend/src/ src/
COPY frontend/demo/index.html ./

# Build the API-driven bundle
RUN npx esbuild src/api-entry.ts \
    --bundle \
    --outfile=out/dist/bundle.js \
    --format=esm \
    --target=es2020 \
    --sourcemap \
    --minify

# Place index.html at the root (it references ./dist/bundle.js)
RUN cp index.html out/

# ── Stage 2: Python backend ─────────────────────────────────────
FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Install rdfsolve with [web] extras (flask, gunicorn, etc.)
COPY pyproject.toml README.md CITATION.cff LICENSE ./
COPY src/ src/
RUN uv pip install --system --no-cache ".[web]"

# Copy seed schemas (pre-mined JSON-LD files)
COPY docker/schemas/ /app/schemas/

# Copy built frontend from stage 1
COPY --from=frontend-builder /build/out/ /app/frontend/

# Environment defaults
ENV DATABASE_PATH=/app/data/rdfsolve.db \
    SCHEMA_IMPORT_DIR=/app/schemas \
    FRONTEND_DIST=/app/frontend \
    FLASK_DEBUG=0 \
    CORS_ORIGINS=* \
    PYTHONUNBUFFERED=1

# Create data directory for SQLite persistence
RUN mkdir -p /app/data

EXPOSE 5000

CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "2", \
     "--timeout", "120", \
     "rdfsolve.backend.app:create_app()"]
