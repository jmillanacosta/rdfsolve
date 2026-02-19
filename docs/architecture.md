# rdfsolve Architecture

## The One-Sentence Summary

**rdfsolve mines the *shape* of RDF datasets (what classes exist, what properties connect them) from live SPARQL endpoints, stores those shapes as JSON-LD files in a SQLite database, and lets users draw paths through them to generate and run SPARQL queries — then packages the paths, queries, and results together as self-describing JSON-LD.**

---

## 1. What Goes In (Data Ingestion)

There is exactly **one way** data enters the system: **mining**.

```
┌──────────────────┐      SPARQL SELECT       ┌─────────────────┐
│ Remote SPARQL     │ ◄───────────────────────  │  SchemaMiner    │
│ Endpoint          │ ──────────────────────►  │  (miner.py)     │
│ (e.g. WikiPathways│    results (classes,     └────────┬────────┘
│   PubChem, AOP…)  │    properties, types,             │
└──────────────────┘    datatypes, labels)              │
                                                        ▼
                                              ┌─────────────────┐
                                              │  MinedSchema     │
                                              │  (models.py)     │
                                              │                  │
                                              │  A list of       │
                                              │  SchemaPatterns:  │
                                              │  (subject_class,  │
                                              │   property_uri,   │
                                              │   object_class,   │
                                              │   labels, count)  │
                                              └────────┬────────┘
                                                       │
                                              .to_jsonld()
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │  JSON-LD file    │
                                              │                  │
                                              │  @context: {…}   │
                                              │  @graph: [{…}]   │
                                              │  _labels: {…}    │
                                              │  @about: {…}     │
                                              └─────────────────┘
```

### What the miner asks the endpoint (3 queries)

| Query | What it finds | Example result |
|-------|--------------|----------------|
| **Typed-object** | Subject class → property → object class | `Gene --encodes--> Protein` |
| **Literal** | Subject class → property → datatype | `Gene --name--> xsd:string` |
| **Untyped URI** | Subject class → property → ??? (URI with no rdf:type) | `Gene --seeAlso--> Resource` |

After those three, two optional steps:
- **Counts**: how many triples match each pattern
- **Labels**: `rdfs:label` / `dc:title` for each URI (so we can display "Key Event" instead of `aopo:KeyEvent`)

### When mining happens

| Trigger | What runs | Output lands in |
|---------|-----------|-----------------|
| `python scripts/mine_all_sources.py` | Mines every row in `data/sources.csv` | `mined_schemas/*.jsonld` + `*.ttl` |
| `python scripts/seed_schemas.py` | Mines and saves to `docker/schemas/` | `docker/schemas/*.jsonld` |
| `docker compose build` (with mining stage) | Mines inside the container at build time | `/app/schemas/*.jsonld` |
| `POST /api/schemas/generate` | Mines one endpoint on-demand via the web UI | Saved directly to SQLite |
| `POST /api/schemas/upload` | User uploads a pre-existing JSON-LD file | Saved directly to SQLite |

---

## 2. What Stores It (The Database)

A single **SQLite** file (`rdfsolve.db`) with two tables:

### `schemas` table

| Column | What it holds |
|--------|--------------|
| `id` | e.g. `aopwikirdf_schema` |
| `name` | e.g. `aopwikirdf` |
| `endpoint` | The SPARQL endpoint URL it was mined from |
| `pattern_count` | Number of SchemaPatterns in this schema |
| `data` | **The entire JSON-LD document as a JSON string** |
| `strategy` | Always `miner` for now |
| `created_at` | Timestamp |

### `endpoints` table

| Column | What it holds |
|--------|--------------|
| `name` | Human-readable name |
| `endpoint` | SPARQL endpoint URL |
| `graph` | Optional named graph URI |
| `manual` | 1 = manually added by user |

### How data enters the database

```
 docker/schemas/*.jsonld  ──┐
                            │  (on Flask startup)
                            ▼
                     SchemaService.import_from_directory()
                            │
                            ▼
                     Database.save_schema()
                            │
                            ▼
                     INSERT INTO schemas (id, name, data, …)
```

Flask checks `SCHEMA_IMPORT_DIR` on every startup. If there are `.jsonld` files there, it reads each one and `INSERT OR REPLACE`s it into the `schemas` table. The JSON-LD is stored **as-is** — the database is just a persistence layer for the full document.

---

## 3. What Goes Out (Data Retrieval)

### 3a. The frontend loads a schema list

```
Browser                          Flask                         SQLite
  │                                │                              │
  │  GET /api/schemas/             │                              │
  │ ─────────────────────────────► │  SELECT id, name, endpoint   │
  │                                │  FROM schemas                │
  │                                │ ────────────────────────────► │
  │  [{id, name, endpoint, …}]    │                              │
  │ ◄───────────────────────────── │ ◄──────────────────────────── │
```

### 3b. The frontend loads one schema

```
Browser                          Flask                         SQLite
  │                                │                              │
  │  GET /api/schemas/aopwikirdf_schema                           │
  │ ─────────────────────────────► │  SELECT data FROM schemas    │
  │                                │  WHERE id = ?                │
  │                                │ ────────────────────────────► │
  │  {                             │                              │
  │    @context: {…},              │                              │
  │    @graph: [{…}],              │  (returns the full JSON-LD)  │
  │    _labels: {…},               │                              │
  │    @about: {…}                 │                              │
  │  }                             │                              │
  │ ◄───────────────────────────── │ ◄──────────────────────────── │
```

### 3c. The frontend parses it into a diagram

```
JSON-LD  ──►  parseJSONLD()  ──►  CanonicalSchema  ──►  buildTree()  ──►  PathTree  ──►  D3 rendering
                                  (all triples,         (visual nodes
                                   all labels,           and edges for
                                   all indices)          selected paths)
```

---

## 4. Paths: The Core User Interaction

A **path** is a sequence of edges the user clicks in the diagram:

```
Gene ──encodes──► Protein ──participatesIn──► Pathway
 │                  │                           │
 source            target/source              target
```

Each edge has:
```json
{
  "source": "http://…/Gene",
  "target": "http://…/Protein",
  "predicate": "http://…/encodes",
  "is_forward": true
}
```

Paths are **the bridge** between the visual schema and executable SPARQL queries.

### From path to SPARQL (compose.py)

```
[path edges]  ──►  compose_query_from_paths()  ──►  SPARQL query string
                                                      + variable_map
                                                      + JSON-LD (sh:SPARQLExecutable)
```

The generated query:
```sparql
PREFIX wp: <http://…>
SELECT DISTINCT ?gene ?protein ?pathway
WHERE {
  ?gene wp:encodes ?protein .
  ?protein wp:participatesIn ?pathway .
  OPTIONAL { ?gene rdfs:label ?geneLabel . }
  OPTIONAL { ?protein rdfs:label ?proteinLabel . }
  OPTIONAL { ?pathway rdfs:label ?pathwayLabel . }
}
LIMIT 100
```

### From SPARQL to results (query.py)

```
SPARQL query  ──►  POST /api/sparql/query  ──►  Flask proxies to endpoint  ──►  results (rows)
```

---

## 5. The New Feature: Path → Subset → Export

### The idea

Right now, a JSON-LD schema contains the **full shape** of a dataset — every class, every property, every connection. But when a user draws paths, they're saying "I only care about *this* slice."

We want to let users **export a subset** of the schema that contains only the classes and properties involved in their selected paths, along with the SPARQL query that was generated and (optionally) the query results.

### The data flow

```
Full JSON-LD schema(s)
         │
         │  User draws paths in the diagram
         ▼
    Selected Paths
         │
         ├──► (1) Subset JSON-LD: only the classes/properties in the paths
         │
         ├──► (2) SPARQL query: generated from those paths
         │
         └──► (3) Results: if the user ran the query
                    │
                    ▼
            PathBundle (the exportable package)
```

### PathBundle: the output format

A `PathBundle` is a single JSON-LD document that wraps everything together:

```json
{
  "@context": {
    "sh": "http://www.w3.org/ns/shacl#",
    "schema": "https://schema.org/",
    "prov": "http://www.w3.org/ns/prov#",
    "void": "http://rdfs.org/ns/void#",
    "rdfsolve": "https://w3id.org/rdfsolve/",
    "wp": "http://vocabularies.wikipathways.org/wp#"
  },

  "@type": "rdfsolve:PathBundle",

  "rdfsolve:sourceSchemas": ["aopwikirdf_schema", "wikipathways_schema"],

  "rdfsolve:paths": [
    {
      "@type": "rdfsolve:Path",
      "rdfsolve:edges": [
        {
          "rdfsolve:source": "aopo:KeyEvent",
          "rdfsolve:target": "aopo:AdverseOutcome",
          "rdfsolve:predicate": "aopo:has_adverse_outcome",
          "rdfsolve:isForward": true
        }
      ]
    }
  ],

  "rdfsolve:schemaSubset": {
    "@context": { "…": "…" },
    "@graph": [
      {
        "@id": "aopo:KeyEvent",
        "aopo:has_adverse_outcome": { "@id": "aopo:AdverseOutcome" }
      }
    ],
    "_labels": {
      "aopo:KeyEvent": "Key Event",
      "aopo:AdverseOutcome": "Adverse Outcome"
    }
  },

  "rdfsolve:query": {
    "@type": ["sh:SPARQLExecutable", "sh:SPARQLSelectExecutable"],
    "sh:select": "SELECT DISTINCT ?keyEvent ?adverseOutcome WHERE { … }",
    "sh:prefixes": { "aopo": "http://…" },
    "schema:target": {
      "@type": "sd:Service",
      "sd:endpoint": "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql/"
    }
  },

  "rdfsolve:results": {
    "@type": "schema:Dataset",
    "schema:variablesMeasured": ["keyEvent", "adverseOutcome"],
    "schema:size": 42,
    "schema:data": [
      {"keyEvent": "http://…/KE_1", "adverseOutcome": "http://…/AO_1"},
      "…"
    ]
  }
}
```

### Where this lives in the codebase

```
Python (src/rdfsolve/)                        TypeScript (frontend/src/)
─────────────────────                         ─────────────────────────
                                              User draws paths
                                              ↓
                                              paths-changed event
                                              ↓
models.py                                     POST /api/compose/from-paths
  PathBundle (Pydantic)                         paths + prefixes
  SchemaSubset (Pydantic)                     ↓
                                              compose.py → query + variable_map
api.py                                        ↓
  create_path_bundle(                         POST /api/sparql/query (optional)
    paths, schema_ids,                          query + endpoint
    query, results                            ↓
  ) → PathBundle                              POST /api/path-bundle (NEW)
                                                paths + schema_ids + query + results
backend/routes/path_bundle.py (NEW)           ↓
  POST /api/path-bundle                       Returns PathBundle JSON-LD
  GET  /api/path-bundle/:id                   (and optionally saves to DB)
```

### Implementation plan (3 pieces)

#### Piece 1: Schema subsetting (Python — `models.py`)

A function that takes a full JSON-LD `@graph` and a list of paths, and returns only the `@graph` nodes and properties that appear in those paths:

```python
def subset_schema(
    schema_jsonld: dict,
    paths: list[dict],
) -> dict:
    """Extract only the @graph nodes touched by the given paths.

    For each edge in each path, keeps:
    - The subject node (@id = edge.source)
    - The specific property (edge.predicate)
    - The object reference (edge.target)
    - Their labels from _labels
    """
```

This is pure filtering — no new queries needed.

#### Piece 2: PathBundle assembly (Python — `api.py`)

A function that composes the three pieces into one document:

```python
def create_path_bundle(
    paths: list[dict],
    schema_ids: list[str],
    prefixes: dict,
    query: str | None = None,
    results: dict | None = None,
    endpoint: str | None = None,
) -> dict:
    """Build a PathBundle JSON-LD from paths + schemas + optional results."""
```

This calls `subset_schema()` for each schema, merges them, attaches the query and results.

#### Piece 3: API route (Python — `routes/path_bundle.py`)

```
POST /api/path-bundle
  Body: { paths, schema_ids, prefixes, query?, results?, endpoint?, save? }
  Returns: PathBundle JSON-LD

GET /api/path-bundle/:id
  Returns: saved PathBundle
```

### What we do NOT build yet

- **LinkML integration**: Later, we'll define a proper LinkML schema for `PathBundle`, `Path`, `Edge`, etc. so that the subset can be validated and converted to other formats (SHACL, JSON Schema, etc.). The JSON-LD structure above is already designed to be compatible with this — every key uses a `rdfsolve:` namespace that will map to LinkML slots.

- **Result provenance chains**: Later, we'll add `prov:wasDerivedFrom` links between results, queries, and schemas. The structure supports it — we just don't generate those triples yet.

- **Cross-schema path resolution**: The current design already supports paths that span multiple schemas (a path can go from a class in schema A to a class in schema B if they share a URI). The subsetting merges them. Federation queries come later.

---

## 6. Summary Diagram

```
                         data/sources.csv
                              │
                    ┌─────────┴─────────┐
                    │  mine_all_sources  │
                    │  or seed_schemas   │
                    │  or docker build   │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  docker/schemas/   │
                    │  *.jsonld files    │
                    └─────────┬─────────┘
                              │  (Flask startup)
                    ┌─────────▼─────────┐
                    │  SQLite DB         │
                    │  schemas table     │
                    └─────────┬─────────┘
                              │  GET /api/schemas/:id
                    ┌─────────▼─────────┐
                    │  Frontend (TS)     │
                    │  parseJSONLD()     │
                    │  CanonicalSchema   │
                    │  buildTree()       │
                    │  PathTree          │
                    │  D3 render         │
                    └─────────┬─────────┘
                              │  User draws paths
                    ┌─────────▼─────────┐
                    │  Selected Paths    │
                    └──┬──────┬──────┬──┘
                       │      │      │
              ┌────────▼┐  ┌──▼───┐ ┌▼────────┐
              │ Schema   │  │SPARQL│ │ Results  │
              │ Subset   │  │Query │ │ (if run) │
              └────┬─────┘  └──┬───┘ └┬────────┘
                   │           │      │
                   └─────┬─────┘──────┘
                         │
                ┌────────▼────────┐
                │   PathBundle    │
                │   (JSON-LD)     │
                │                 │
                │  One exportable │
                │  document with  │
                │  everything     │
                └─────────────────┘
```

---

## 7. File Map

| File | Role |
|------|------|
| `data/sources.csv` | List of SPARQL endpoints to mine |
| `scripts/mine_all_sources.py` | CLI: mine all sources → files on disk |
| `scripts/seed_schemas.py` | CLI: mine sources → `docker/schemas/` |
| `src/rdfsolve/miner.py` | The miner: 3 SPARQL queries → `MinedSchema` |
| `src/rdfsolve/models.py` | `SchemaPattern`, `MinedSchema`, `.to_jsonld()`, `.to_void_graph()` |
| `src/rdfsolve/compose.py` | Paths → SPARQL query string |
| `src/rdfsolve/query.py` | Execute SPARQL via `SparqlHelper` |
| `src/rdfsolve/parser.py` | `VoidParser`: JSON-LD → LinkML / SHACL / RDF-config |
| `src/rdfsolve/backend/database.py` | SQLite: `schemas` + `endpoints` tables |
| `src/rdfsolve/backend/services/schema_service.py` | Import, save, generate schemas |
| `src/rdfsolve/backend/routes/schemas.py` | REST: `/api/schemas/*` |
| `src/rdfsolve/backend/routes/compose.py` | REST: `/api/compose/from-paths` |
| `src/rdfsolve/backend/routes/sparql.py` | REST: `/api/sparql/query` |
| `src/rdfsolve/backend/app.py` | Flask factory + startup import |
| `docker/Dockerfile` | Multi-stage: build TS + install Python + copy schemas |
| `docker/schemas/*.jsonld` | Pre-mined schemas ready for Docker import |
| `frontend/src/parsers/jsonld-parser.ts` | JSON-LD → `CanonicalSchema` |
| `frontend/src/data/view-builder.ts` | `CanonicalSchema` → `PathTree` (visual nodes/edges) |
| `frontend/src/renderer/tree-renderer.ts` | `PathTree` → SVG via D3 |
| `frontend/src/components/sparql-editor.ts` | Paths → SPARQL editor UI |
| `frontend/src/api-entry.ts` | Wires API to web components |
