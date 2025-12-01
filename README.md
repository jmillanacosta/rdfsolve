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

RDF schema extraction and analysis toolkit for SPARQL endpoints.

## Installation

```bash
uv pip install rdfsolve
```

## Quick Start

### CLI

Generate a VoID description from a SPARQL endpoint:

```bash
rdfsolve void https://sparql.example.org/ -o schema.ttl
```

Extract schema and convert to LinkML:

```bash
rdfsolve linkml https://sparql.example.org/ -o schema.yaml
```

### Python API

```python
from rdfsolve.api import generate_void_from_endpoint, load_parser_from_graph

# Generate VoID from endpoint
void_graph = generate_void_from_endpoint("https://sparql.example.org/")

# Load parser and extract schema
parser = load_parser_from_graph(void_graph)
schema_df = parser.to_schema()
jsonld = parser.to_jsonld()
linkml_yaml = parser.to_linkml_yaml()
```

## Documentation

- **Full documentation**: [rdfsolve.readthedocs.io](https://rdfsolve.readthedocs.io)
- **Results dashboard**: [jmillanacosta.github.io/rdfsolve](https://jmillanacosta.github.io/rdfsolve)

## Examples

See the [notebooks](notebooks/) directory for detailed examples of schema extraction from various SPARQL endpoints.

The [GitHub Actions workflow](.github/workflows/make-notebooks.yml) carries out for now the automated batch processing of multiple endpoints.

## License

MIT License - see [LICENSE](LICENSE) for details.
