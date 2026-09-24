def aop_schema():
    path = Path(__file__).parent / "test_data" / "aopwikirdf_schema.json"
    triples = json.loads(path.read_text())["triples"]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="aopwikirdf"),
        patterns=[
            SchemaPattern(subject_class=s, property_uri=p, object_class=o) for s, p, o in triples
        ],
    )


import json
from pathlib import Path

from rdflib import RDF, SH, Graph
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern


def test_path_probes_measure_joins_and_keep_zero_degree_sources():
    from rdfsolve.client.api import Client

    from rdfsolve import SchemaMiner

    graph = Graph().parse(
        data="@prefix e: <urn:route:> .\n        e:a1 a e:A; e:p e:b1 . e:a2 a e:A; e:p e:b2 .\n        e:b1 a e:B . e:b2 a e:B; e:q e:c . e:c a e:C .\n        e:orphan a e:B; e:r e:d . e:d a e:D .",
        format="turtle",
    )
    with SchemaMiner.from_graph(graph) as miner:
        schema = miner.mine()
        nav = schema.discover_paths(
            max_hops=2, max_paths_per_length=10, helper=miner.helper, probe_limit=10
        )
    routes = {
        p.steps[-1].property_uri: p for p in nav.paths if p.steps[0].subject_class == "urn:route:A"
    }
    assert (routes["urn:route:q"].matched_sources, routes["urn:route:q"].source_count) == (1, 2)
    assert (routes["urn:route:q"].min_count, routes["urn:route:q"].max_count) == (0, 1)
    assert routes["urn:route:r"].instance_support == "no_match"
    restored = MinedSchema.from_dict(schema.to_dict())
    assert restored.navigation == nav
    shapes = Graph().parse(data=schema.to_shacl(), format="turtle")
    profiles = [s for s in shapes.subjects(RDF.type, SH.NodeShape) if "observed-route-" in str(s)]
    assert profiles and all((bool(shapes.value(s, SH.deactivated)) for s in profiles))
    assert list(shapes.objects(None, SH.qualifiedValueShape))
    roundtrip = MinedSchema.from_shacl(schema.to_shacl()).to_shacl()
    assert list(
        Graph().parse(data=roundtrip, format="turtle").objects(None, SH.qualifiedValueShape)
    )
    client = Client(schema, graph)
    table = client.navigation(observed_only=True)
    query = client.prepare_path(table.loc[table.Target == "urn:route:C"].iloc[0]["Reference"])
    assert client.select(query).row_count == 1

    from rdflib import Dataset, URIRef

    scoped = Dataset(default_union=False)
    for name in ["urn:data:left", "urn:data:right"]:
        for triple in graph:
            if triple != (URIRef("urn:route:c"), RDF.type, URIRef("urn:route:C")):
                scoped.graph(URIRef(name)).add(triple)
    context = scoped.graph(URIRef("urn:types"))
    context.add((URIRef("urn:route:c"), RDF.type, URIRef("urn:route:C")))
    context.add((URIRef("urn:route:decoy"), RDF.type, URIRef("urn:route:A")))
    with SchemaMiner.from_graph(scoped, graph_uris=["urn:data:left", "urn:data:right"],
                                type_context_graph_uris=["urn:types"], delay=0) as miner:
        schema = miner.mine()
        nav = schema.discover_paths(max_hops=2, max_paths_per_length=10,
                                    helper=miner.helper, probe_limit=10)
    route = next(p for p in nav.paths if p.steps[0].subject_class == "urn:route:A"
                 and p.steps[-1].object_class == "urn:route:C")
    assert (route.matched_sources, route.source_count) == (1, 2), "Merge data graphs; keep context out of route populations"

    assert route.graph_uris == ["urn:data:left", "urn:data:right"]
    assert route.type_context_graph_uris == ["urn:types"]
    assert MinedSchema.from_dict(schema.to_dict()).navigation == nav
    from unittest.mock import patch
    from rdfsolve.mining.navigation import observe_path

    with SchemaMiner.from_graph(scoped) as probe:
        with patch.object(probe.helper, "select_with_fallback", side_effect=TimeoutError("probe")):
            observe_path(route, probe.helper, ["urn:data:left"])
        assert route.instance_support == "timeout" and route.error
        assert (route.source_count, route.matched_sources, route.min_count, route.max_count) == (None,) * 4
        observe_path(route, probe.helper, ["urn:data:left", "urn:data:right"],
                     type_context_graph_uris=["urn:types"])
    assert route.instance_support == "matched" and route.error is None
    assert (route.matched_sources, route.source_count) == (1, 2)

    candidates = schema.discover_paths(max_hops=2, max_paths_per_length=10)
    selected = next(p for p in candidates.paths if p.steps[-1].object_class == "urn:route:C")
    with SchemaMiner.from_graph(scoped) as probe:
        observed = schema.probe_paths([selected], helper=probe.helper)
    assert observed == [selected] and selected.matched_sources == 1
    assert all(p.instance_support == "not_checked" for p in candidates.paths if p is not selected)
    assert candidates.probe_selection == "explicit"
    assert MinedSchema.from_dict(schema.to_dict()).navigation == candidates
    with SchemaMiner.from_graph(Graph()) as empty:
        observe_path(selected, empty.helper, [])
    assert (selected.instance_support, selected.source_count) == ("no_sources", 0)
    assert MinedSchema.from_dict(schema.to_dict()).navigation == candidates
