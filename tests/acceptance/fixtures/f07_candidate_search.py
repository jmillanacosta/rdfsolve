"""F07: Recoverable candidate search with many variants.

Generate 128 distinct schema relations ex:A ex:p001 ex:B through ex:A ex:p128 ex:B.
Each has label "relation variant 001" through "relation variant 128", no descriptions.

Data contains ex:a a ex:A and for each index i, ex:a ex:pNNN ex:bNNN, ex:bNNN a ex:B.

This tests bounded candidate search and window paging.
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from . import EX

VARIANT_COUNT = 128


def create_graph(n_variants: int = VARIANT_COUNT) -> Graph:
    """Create F07 fixture graph with n variants."""
    g = Graph()

    # Add source instance
    g.add((EX.a, RDF.type, EX.A))

    # Add variants
    for i in range(1, n_variants + 1):
        pred_uri = EX[f"p{i:03d}"]
        target_uri = EX[f"b{i:03d}"]

        g.add((EX.a, pred_uri, target_uri))
        g.add((target_uri, RDF.type, EX.B))

    return g


def create_schema_metadata(n_variants: int = VARIANT_COUNT) -> dict:
    """Create schema metadata for n variants."""
    predicates = []
    patterns = []

    for i in range(1, n_variants + 1):
        pred_uri = str(EX[f"p{i:03d}"])
        predicates.append({
            "iri": pred_uri,
            "label": f"relation variant {i:03d}",
        })
        patterns.append({
            "subject": str(EX.A),
            "predicate": pred_uri,
            "object": str(EX.B),
        })

    return {
        "classes": [
            {"iri": str(EX.A), "label": "A"},
            {"iri": str(EX.B), "label": "B"},
        ],
        "predicates": predicates,
        "patterns": patterns,
    }


def add_labels(g: Graph, n_variants: int = VARIANT_COUNT) -> None:
    """Add rdfs:label annotations."""
    g.add((EX.A, RDFS.label, Literal("A")))
    g.add((EX.B, RDFS.label, Literal("B")))

    for i in range(1, n_variants + 1):
        pred_uri = EX[f"p{i:03d}"]
        g.add((pred_uri, RDFS.label, Literal(f"relation variant {i:03d}")))


def query_for_variant(variant_index: int) -> str:
    """Generate query for specific variant."""
    pred = f"p{variant_index:03d}"
    return f"""
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE {{
    ?a a ex:A; ex:{pred} ?b .
    ?b a ex:B .
}}
"""


def expected_for_variant(variant_index: int) -> set:
    """Expected result for specific variant."""
    return {(URIRef(str(EX.a)), URIRef(str(EX[f"b{variant_index:03d}"])))}


def verify_variant(g: Graph, variant_index: int) -> bool:
    """Verify specific variant query."""
    query = query_for_variant(variant_index)
    result = list(g.query(query))
    actual = {tuple(row) for row in result}
    return actual == expected_for_variant(variant_index)


# Test boundary counts as specified
BOUNDARY_COUNTS = [0, 1, 2, 4, 5, 50, 51, 128]


def create_graph_with_count(count: int) -> Graph:
    """Create F07 variant with specific count."""
    g = Graph()
    g.add((EX.A, RDFS.label, Literal("A")))
    g.add((EX.B, RDFS.label, Literal("B")))

    if count == 0:
        # Just declare the classes
        return g

    return create_graph(count)


def verify_boundary_count(count: int) -> bool:
    """Verify fixture with specific boundary count."""
    g = create_graph_with_count(count)

    if count == 0:
        # No routes, should be blocked
        query = """
PREFIX ex: <https://fixture.invalid/>
SELECT ?a ?b WHERE { ?a a ex:A; ?p ?b . ?b a ex:B . FILTER(?p != rdf:type) }
"""
        result = list(g.query(query))
        return len(result) == 0

    # Each variant should return exactly one result
    for i in range(1, count + 1):
        if not verify_variant(g, i):
            return False
    return True
