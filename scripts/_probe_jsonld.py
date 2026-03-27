"""Probe script to inspect what class URIs rdflib extracts from JSON-LD schemas."""
import rdflib
from rdflib import URIRef
from pathlib import Path

schema_root = Path("docker/schemas")

for schema_dir in sorted(schema_root.iterdir()):
    name = schema_dir.name
    path = schema_dir / f"{name}_schema.jsonld"
    if not path.exists():
        continue
    g = rdflib.Graph()
    g.parse(str(path), format="json-ld")
    # Every IRI subject is a candidate class
    iris = {str(s) for s in g.subjects() if isinstance(s, URIRef)}
    if iris:
        print(f"{name}: {sorted(iris)[:5]} … ({len(iris)} total)")
