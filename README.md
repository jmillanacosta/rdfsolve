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

### Typed client generation

Use `rdfsolve.client_api` to find names or identifiers, then follow the results.
For an AOPWiki schema:

```python
from rdfsolve.client_api import Client

data = Client(schema)
matches = data.find("thyroid")
matches.types()
```

Pick the records you want and see where they lead:

```python
pathways = matches.of_type("Adverse outcome pathway")
pathways.paths()

stressors = pathways.related("Stressor")
chemicals = stressors.related("Chemical entity")
chemicals.show("identifier")
```

Use `values("title")` to list values across your selected records.
Use `target_value` when you know a name but not its class:

```python
from IPython.display import Markdown, display

paths = data.paths_between("Adverse outcome pathway", target_value="Phenobarbital", max_hops=3)
display(paths)
display(Markdown(data.diagram(paths=paths)))
```

This verifies mined class routes against matching names or identifiers, ignoring case.
It does not link records just because they share a type. Passing a second class
instead lists possible class routes without querying the data.
Use `diagram(paths=paths, path=1)` for the first complete path. Add
`instances=False` to show its classes instead of its records.

Start from one record to keep the search within its connections:

```python
name_paths = data.paths_between(pathways[0], target_value="Phenobarbital", max_hops=3)
chemical_paths = data.paths_between(pathways[0], "Chemical entity", max_hops=3)
```

The first finds matching names; the second finds records of the chosen class.
Both verify the links from this pathway, not all pathways of its class.

Press Tab after `pathways.fields.` to discover fields while typing.
`show()` retrieves only the fields you ask for; displaying results does not
send requests.

Start directly from an endpoint with `explore(endpoint, graph=graph_iri)`.
Show each query and its returned data with `data.query_log()`.
Save the queries, results, and steps with `data.save_session("session.json")`,
then close the connection with `data.close()`.

[Walk through the thyroid investigation](notebooks/pydantic_clients/01_mine_explore.ipynb).

### SparqlHelper

Large SPARQL queries can time out, and endpoints can fail intermittently.
`SparqlHelper` retries temporary failures and fetches large results in smaller
batches. It reduces page sizes with LIMIT/OFFSET steps after timeouts and spaces
requests to ease the load on endpoints.

```python
from rdfsolve.sparql_helper import SparqlHelper

endpoint = "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql"
query = "SELECT DISTINCT ?class WHERE { ?s a ?class } ORDER BY ?class"

with SparqlHelper(endpoint, timeout=30) as helper:
    pages = helper.prepare_paginated_query(query)
    for rows in helper.select_chunked(pages, chunk_size=100):
        print(rows)
```

For a single request, use `helper.select(query)`. Use `helper.ask(query)` for
yes/no questions or `helper.construct_graph(query)` to retrieve RDF. No mining
pipeline or registry is required.

Paging helps with result size; it cannot guarantee that an expensive query will
finish.

### Keep and share useful queries

Give queries names, run them again, and share them as Turtle:

```python
with SparqlHelper(endpoint, timeout=30) as helper:
    helper.add_query("classes", query)
    results = helper.run_query("classes")
    helper.export_queries_as_ttl("queries.ttl")
    print(helper.history)  # Named runs: time, duration, and success or error
```

Loading a SHACL with Sparql Examples:

```python
with SparqlHelper(endpoint, timeout=30) as helper:
    names = helper.load_shacl("queries.ttl")
    print(names)
    helper.queries.rename(names[0], "my query")
    results = helper.run_query("my query")
```

SHACL paths can also become queries for a particular entity:

```python
helper.load_shacl("shapes.ttl")
print(helper.queries.paths)  # Choose a property shape
query = helper.queries.path_query(property_shape_id, entity_iri, limit=20)
```

### Add metadata to a source registry

Start with a name and endpoint. Add `sources_file` to create or update a YAML
entry; omit it to preview without writing:

```python
from rdfsolve import enrich_source

source = enrich_source(
    "aopwikirdf",
    "https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
    sources_file="sources.yaml",
)
print(source["dataset_metadata"])
print(source["enrichment"])  # Completed, failed, or skipped retrieval steps
```

This tests availability and retrieves dataset descriptions, not instance
patterns. It fills supported metadata such as the description, license, and
version when one dataset can be identified. Missing or ambiguous metadata stays
blank; failed retrieval leaves previous values intact. Existing query settings
and unrelated fields are kept. Saves make unique backups; YAML comments and
layout are not preserved.

Metadata comes from the default graph unless `metadata_graph_uris=[...]` is
supplied or stored. Add `discover_void=True` to find published VoID descriptions
and record their graph locations and pattern availability. That scan can take
many requests; it does not run by default. Metadata locations do not become
instance-query scopes.

Reuse the registry for your own queries:

```python
from rdfsolve import load_sources

source = next(s for s in load_sources("sources.yaml") if s["name"] == "aopwikirdf")
with SparqlHelper.from_source_entry(source) as helper:
    results = helper.select(query + " LIMIT 20")
```

### Batch mining

Mine multiple endpoints from a YAML file:

**Create `sources.yaml`:**

```yaml
- name: uniprot
  endpoint: https://sparql.uniprot.org/sparql

- name: rhea
  endpoint: https://sparql.rhea-db.org/sparql
```

To populate an entry from its name and endpoint, use
`enrich_source(name, endpoint, sources_file="sources.yaml")` as above. Retrieved
metadata is separate from settings such as `chunk_size`, `class_batch_size`, and
`timeout`; choose those for the workload.

**Run batch mining:**

```bash
python scripts/pipeline.py --sources-file sources.yaml --remote-only
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
- name: drugbank
  download_nt:
    - https://example.org/drugbank.nt.gz
```

```bash
# Download, index, and mine
python scripts/pipeline.py --sources-file sources.yaml --local-only
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

### Let an agent use your typed client

Install `rdfsolve[agents]` to give a PydanticAI agent the same classes, searches,
and links of the typed client. Records stay in Python; queries and returned
data remain in the session log.

```python
from pydantic_ai.usage import UsageLimits
from rdfsolve.pydantic_ai import ClientTools

tools = ClientTools(data)
agent = tools.agent("openai:gpt-5.4-mini-2026-03-17")
answer = await agent.run(
    "Find Phenobarbital and tell me which classes and links describe it.",
    usage_limits=UsageLimits(request_limit=8, tool_calls_limit=12),
)
print(answer.output)
data.query_log()
```

## Documentation

Full docs: [rdfsolve.readthedocs.io](https://rdfsolve.readthedocs.io)

## License

MIT — see [LICENSE](LICENSE).
