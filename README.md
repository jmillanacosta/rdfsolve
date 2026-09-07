# rdfsolve

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

Mine typed RDF schemas, convert between formats, and derive cross-dataset mappings.

## Installation

```bash
pip install rdfsolve
```

## Quick Start

### Mine an endpoint

```python
from rdfsolve import SchemaMiner

# Query a SPARQL endpoint
miner = SchemaMiner(endpoint_url="https://sparql.uniprot.org/sparql", source_name="uniprot")
schema = miner.mine(dataset_name="uniprot")

# Export formats
schema.to_void_graph()  # VoID RDF graph
schema.to_jsonld()  # JSON-LD dict
schema.to_linkml_yaml()  # LinkML YAML string
schema.to_shacl()  # SHACL shapes

# Save the complete model for later analysis
import json

with open("uniprot_schema.json", "w", encoding="utf-8") as f:
    json.dump(schema.to_dict(), f, indent=2)
```

### Read saved analysis data

```python
from rdfsolve import MinedSchema

schema = MinedSchema.from_json("uniprot_schema.json")
```

### Add definitions, examples, and a typed API

```python
from pathlib import Path
from rdfsolve import SchemaMiner

miner = SchemaMiner(
    endpoint_url="https://sparql.uniprot.org/sparql",
    enrich=True,
    examples_per_pattern=2,
)
schema = miner.mine(dataset_name="uniprot")
Path("uniprot_models.py").write_text(schema.to_pydantic(), encoding="utf-8")

# You can also enrich a saved schema with the same source and graph scope.
# schema.enrichment = miner.query_enrichment(schema)
```

Generated classes use cleaned source labels. Definitions become class docstrings;
observed values become field examples. Fields allow the observed ranges without
claiming that the sample proves required fields or maximum cardinality.

The pipeline retrieves this enrichment in every mining mode. Use
`--examples-per-pattern 0` for definitions only, or `--no-enrichment` to skip it.
Samples follow endpoint order, not a random sampling design. Query failures and
missing definitions remain visible in the canonical JSON and mining report.

`schema.about.source_version_iri` retains the source release IRI when metadata
identifies one. `schema_version` uses that IRI, a release label, a source date,
or a dated mining snapshot. The canonical JSON envelope's `version` is a separate
storage-format identifier. A snapshot date does not certify an upstream release.

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
```
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


### Query metadata without mining

Extract dataset metadata (license, publisher, version) without full schema extraction:

```python
from rdfsolve.api import query_metadata

metadata = query_metadata("https://sparql.uniprot.org/sparql")
```

### Discover existing VoID descriptions

Find and export pre-existing VoID descriptions at an endpoint:

```python
from rdfsolve.api import discover_void_source

result = discover_void_source(
    endpoint="https://sparql.uniprot.org/sparql", name="uniprot", output_dir="output/"
)
```

### Probe endpoints for entity matching

Match URI patterns across endpoints to find datasets containing specific entity types:

```python
from rdfsolve.instance_matcher import probe_endpoint

match = probe_endpoint(
    endpoint_url="https://sparql.uniprot.org/sparql",
    uri_prefix="http://identifiers.org/ncbigene/",
    limit=100,
)
```

### Check endpoint health

Test endpoint availability and response times:

```python
from rdfsolve.endpoint_health import check_endpoint_health

status = check_endpoint_health("https://sparql.uniprot.org/sparql")
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
