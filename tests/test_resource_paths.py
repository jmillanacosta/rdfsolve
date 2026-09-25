"""Discover resource routes without supplying their predicates."""
from rdflib import Graph
from rdfsolve.client.api import Client
from rdfsolve.schema_models import MinedSchema

def test_resource_routes_keep_direction_and_exclude_cycles():
    graph = Graph().parse(data="""
        @prefix e: <urn:example:> .
        e:drug a e:Chemical .
        e:measurement a e:Measurement; e:subject e:drug; e:target e:protein;
            e:value "0.011"; e:loop e:measurement .
        e:protein e:back e:drug .
    """, format="turtle")
    schema = MinedSchema.from_shacl("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix e: <urn:example:> .
        e:shape a sh:NodeShape; sh:targetClass e:Chemical .
    """)
    with Client(schema, graph) as client:
        paths = client.connections("urn:example:drug", "urn:example:protein", max_hops=3)
        routes = paths.attrs["routes"]
        sequences = {
            tuple(route["bindings"][f"n{i}"]["value"] for i in range(route["hops"] + 1))
            for route in routes
        }
        assert ("urn:example:drug", "urn:example:protein") in sequences, "Find an incoming direct edge"
        assert ("urn:example:drug", "urn:example:measurement", "urn:example:protein") in sequences, "Discover the measured relationship without its predicate names"
        assert all(len(nodes) == len(set(nodes)) for nodes in sequences), "Exclude loops and repeated resources"
        assert paths.attrs["status"] == "complete"
        neighbours = client.connections("urn:example:measurement", max_hops=1, both_directions=False)
        assert set(neighbours.To) == {"urn:example:drug", "urn:example:protein"}, "Neighbourhoods exclude literal leaves, type edges and self loops"
