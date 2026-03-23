# RDFSolve

<p align="center">
    <a href="https://github.com/jmillanacosta/rdfsolve/actions/workflows/tests.yml">
        <img alt="Tests" src="https://github.com/jmillanacosta/rdfsolve/actions/workflows/tests.yml/badge.svg" /></a>
    <a href="https://pypi.org/project/rdfsolve">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/rdfsolve" /></a>
    <a href="https://pypi.org/project/rdfsolve">
        <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/rdfsolve" /></a>
    <a href="https://github.com/jmillanacosta/rdfsolve/blob/main/LICENSE">
        <img alt="PyPI - License" src="https://img.shields.io/pypi/l/rdfsolve" /></a>
    <a href='https://rdfsolve.readthedocs.io/en/latest/?badge=latest'>
        <img src='https://readthedocs.org/projects/rdfsolve/badge/?version=latest' alt='Documentation Status' /></a>
</p>

Extract RDF schemas from SPARQL endpoints, convert between formats
(JSON-LD, LinkML, SHACL, VoID, RDF-config), and derive cross-dataset mappings.

Dashboard (static demo): [jmillanacosta.github.io/rdfsolve-frontend](https://jmillanacosta.github.io/rdfsolve-frontend)

## Installation

```bash
uv pip install rdfsolve
```

## CLI

```text
rdfsolve [--verbose] <group> <command> [OPTIONS]
```

### Schema mining (`pipeline`)

```bash
# Mine schemas from remote SPARQL endpoints listed in sources.yaml
rdfsolve pipeline mine --sources data/sources.yaml

# Mine a single source from a local QLever endpoint
rdfsolve pipeline local-mine --name drugbank --endpoint http://localhost:7026

# Generate Qleverfiles for local QLever instances
rdfsolve pipeline qleverfile --data-dir /data/rdf
```

### Format conversion (`export`)

Convert any VoID `.ttl` or rdfsolve `.jsonld` schema to another format
(auto-detected from extension):

```bash
rdfsolve export csv       schema.jsonld
rdfsolve export jsonld    void.ttl
rdfsolve export void      schema.jsonld
rdfsolve export linkml    schema.jsonld -o ./out
rdfsolve export shacl     schema.jsonld --closed
rdfsolve export rdfconfig void.ttl --endpoint-url https://sparql.example.org
```

### Web backend

A Flask REST API exposes schemas, SPARQL query generation, IRI resolution, export,
mappings, and SHACL/LinkML conversion over HTTP. It can also serve the
[rdfsolve-frontend](https://github.com/jmillanacosta/rdfsolve-frontend) app.

```bash
# Quick start with Docker
docker compose up --build        # http://localhost:8000

# Or run directly
python -m rdfsolve.backend.app   # uses env vars for config
```

Key endpoints: `/api/schemas`, `/api/sparql`, `/api/export`, `/api/shapes`,
`/api/mappings`, `/api/linkml`, `/api/compose`.

## Python API

```python
from rdfsolve.api import mine_schema, load_parser_from_file

# Mine a schema from a SPARQL endpoint
result = mine_schema(
    name="rhea",
    endpoint_url="https://sparql.rhea-db.org/sparql",
)

# Load a previously mined schema and convert
parser = load_parser_from_file("rhea_schema.jsonld")
parser.to_schema()       # pandas DataFrame
parser.to_jsonld()       # JSON-LD dict
parser.to_linkml_yaml()  # LinkML YAML string
parser.to_shacl()        # SHACL Turtle string
```

## Documentation

Full docs: [rdfsolve.readthedocs.io](https://rdfsolve.readthedocs.io)

## License

MIT — see [LICENSE](LICENSE).
