# rdfsolve

<p align="center">
    <a href="https://github.com/jmillanacosta/rdfsolve/actions/workflows/tests.yml">
        <img
        alt="Tests"
        src="https://github.com/jmillanacosta/rdfsolve/actions/workflows/tests.yml/badge.svg"
    /></a>
    <a href="https://pypi.org/project/rdfsolve">
        <img
        alt="PyPI"
        src="https://img.shields.io/pypi/v/rdfsolve"
    /></a>
    <a href="https://pypi.org/project/rdfsolve">
        <img
        alt="PyPI - Python Version"
        src="https://img.shields.io/pypi/pyversions/rdfsolve"
    /></a>
    <a href="https://github.com/jmillanacosta/rdfsolve/blob/main/LICENSE">
        <img
        alt="PyPI - License"
        src="https://img.shields.io/pypi/l/rdfsolve"
    /></a>
    <a href='https://rdfsolve.readthedocs.io/en/latest/?badge=latest'>
        <img
        src='https://readthedocs.org/projects/rdfsolve/badge/?version=latest'
        alt='Documentation Status'
    /></a>
</p>

An RDF toolkit for retrieving SPARQL endpoint and RDF dump metadata, testing
endpoint availability, running queries with batching and retries, extract and
convert schemas between descriptive (VoID) and validation (SHACL) formats,
generate typed Python models (Pydantic), and derive mappings across datasets.

## Installation

```bash
pip install rdfsolve
```

## Quick Start

### Mine an endpoint

```python
from rdfsolve import SchemaMiner

# Query a SPARQL endpoint
miner = SchemaMiner(endpoint_url="https://sparql.example.org/sparql")
schema = miner.mine(dataset_name="example")

# Enrich with labels and definitions, and examples
schema.enrichment = miner.query_enrichment(schema)

# Export formats
schema.to_void_graph()  # VoID RDF graph
schema.to_linkml_yaml()  # LinkML schema YAML string
schema.to_shacl()  # SHACL shapes
schema.to_pydantic()  # Python source for dataset-specific Pydantic classes
schema.to_dict()  # Versioned canonical document (dict)

# Inspect
schema.get_classes()  # List with classes
schema.get_properties()  # List with properties

print(schema.get_metadata())  # Print a summary view of found metadata across graphs
print(schema.get_metadata().to_trig())  # Print metadata as trig
print(schema.get_metadata().to_turtle())  # Print metadata as turtle

import json

with open("example_schema.json", "w", encoding="utf-8") as f:
    json.dump(schema.to_dict(), f, indent=2)
```

`schema.model_dump_json()` returns the `MinedSchema` data as a JSON string.

Use `schema.to_dict()` with `json.dump()` for saved rdfsolve files.
`MinedSchema.model_json_schema()` describes the internal model;
`schema.to_pydantic()` instead generates Python classes for the mined RDF types.

### Discover existing VoID descriptions

VoID is a published dataset description. It may contain metadata without any
relationship patterns:

```python
from rdfsolve.api import discover_void_source

void = discover_void_source(
    endpoint="https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
    name="aopwikirdf",
)
print(void.has_void, void.has_partitions, void.has_patterns)
print(void.get_metadata())  # Show retrieved metadata; no new request.

schema = void.to_mined_schema()
print(schema.get_metadata())  # Show metadata from the stored schema.
```

`has_void` means any VoID description was found; `has_patterns` means it
provides relationship patterns.

Discovery checks all graphs, one request at a time. Use `void.graph_uris` to
list matches and `void.for_graph(uri).get_metadata()` to inspect one. Set
`output_dir` only when you want files.

### Query metadata without mining

Read the available metadata (queried with the package):

```python
from rdfsolve.api import query_metadata

metadata = query_metadata(
    "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
    graph_uris=["http://aopwiki.org/"],
)
print(metadata)  # Readable view; notebooks also display it automatically.
```

The view shows what was retrieved, not everything the endpoint may contain. Use
`metadata.to_turtle()`/`metadata.to_trig()` for all retained details, or
`metadata.to_markdown()` to save the readable view.

### Read schema files

```python
from pathlib import Path
from rdfsolve import MinedSchema

schema = MinedSchema.from_json("example_schema.json")
schema = MinedSchema.from_void(Path("example_void.ttl").read_text(encoding="utf-8"))
schema = MinedSchema.from_shacl(Path("example_shacl.ttl").read_text(encoding="utf-8"))
```

Allows (partial) interconversion through `MinedSchema`.

### Load and convert existing schemas

```python
from rdfsolve import VoidParser

# Load VoID Turtle or JSON-LD
parser = VoidParser(void_source="schema.ttl")
schema = parser.to_mined_schema()

# Convert between formats
schema.to_jsonld()  # To JSON-LD
schema.to_linkml_yaml()  # To LinkML
schema.to_shacl()  # To SHACL
```

### Batch mining

Mine multiple endpoints from a YAML file:

**Create `sources.yaml`:**

```yaml
sources:
  uniprot:
    endpoint: https://sparql.uniprot.org/sparql

  rhea:
    endpoint: https://sparql.rhea-db.org/sparql
```

**Run batch mining:**

```bash
python scripts/pipeline.py --sources sources.yaml --remote-only
```

**Output:**

```text
output/
├── uniprot/
│   ├── uniprot_schema.jsonld
│   ├── uniprot_void.ttl
│   └── uniprot_report.json
└── rhea/
    ├── rhea_schema.jsonld
    ├── rhea_void.ttl
    └── rhea_report.json
```

### Local RDF Files (with QLever)

Mine local RDF dumps using QLever:

```yaml
sources:
  drugbank:
    download_urls:
      - https://example.org/drugbank.nt.gz
    local_provider: qlever
```

```bash
# Download, index, and mine
python scripts/pipeline.py --sources sources.yaml --local-only
```

### Probe endpoints for entity matching

Match URI patterns across endpoints to find datasets containing specific entity
types:

```python
from rdfsolve.instance_matcher import probe_endpoint

match = probe_endpoint(
    endpoint_url="https://sparql.example.org/sparql",
    uri_prefix="http://identifiers.org/ncbigene/",
    limit=100,
)
```

### Check endpoint health

Test endpoint availability and response times:

```python
from rdfsolve.endpoint_health import check_endpoint_health

check_endpoint_health("https://aopwiki.rdf.bigcat-bioinformatics.org/sparql")
# EndpointHealthCheck(
#     endpoint_url='https://aopwiki.rdf.bigcat-bioinformatics.org/sparql',
#     status='up', response_time=0.1596362590789795, error_message='',
#     timestamp='2026-09-08T08:16:36.126659+00:00'
# )
```

### Infer cross-dataset mappings

Derive new mappings through inversion and transitivity:

```bash
python scripts/infer_mappings.py mappings/*.jsonld -o inferred.jsonld --transitivity
```

### Build connectivity graphs

Create graphs showing dataset relationships via shared classes and mappings:

```bash
python scripts/build_graphs.py output/schemas/ --mappings output/mappings/
```

## Documentation

Full docs: [rdfsolve.readthedocs.io](https://rdfsolve.readthedocs.io)

## License

MIT — see [LICENSE](LICENSE).
