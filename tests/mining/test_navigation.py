"""rdfsolve.mining.navigation: navigation routes between classes, selected from the schema and
written as path queries."""

import json
from pathlib import Path

import pytest
from pyoxigraph import RdfFormat, Store
from rdflib import RDF, SH, Graph

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.selection import SchemaSelection


def aop_schema():
    path = Path(__file__).parents[1] / "test_data" / "aopwikirdf_schema.json"
    triples = json.loads(path.read_text())["triples"]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name="aopwikirdf"),
        patterns=[
            SchemaPattern(subject_class=s, property_uri=p, object_class=o) for s, p, o in triples
        ],
    )


def test_path_probes_measure_joins_and_keep_zero_degree_sources():
    from rdfsolve import SchemaMiner
    from rdfsolve.client.api import Client

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
    assert profiles and all(bool(shapes.value(s, SH.deactivated)) for s in profiles)
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
    with SchemaMiner.from_graph(
        scoped,
        graph_uris=["urn:data:left", "urn:data:right"],
        type_context_graph_uris=["urn:types"],
        delay=0,
    ) as miner:
        schema = miner.mine()
        nav = schema.discover_paths(
            max_hops=2, max_paths_per_length=10, helper=miner.helper, probe_limit=10
        )
    route = next(
        p
        for p in nav.paths
        if p.steps[0].subject_class == "urn:route:A" and p.steps[-1].object_class == "urn:route:C"
    )
    assert (route.matched_sources, route.source_count) == (1, 2), (
        "Merge data graphs; keep context out of route populations"
    )

    assert route.graph_uris == ["urn:data:left", "urn:data:right"]
    assert route.type_context_graph_uris == ["urn:types"]
    assert MinedSchema.from_dict(schema.to_dict()).navigation == nav
    from unittest.mock import patch

    from rdfsolve.mining.navigation import observe_path

    with SchemaMiner.from_graph(scoped) as probe:
        with patch.object(probe.helper, "select_with_fallback", side_effect=TimeoutError("probe")):
            observe_path(route, probe.helper, ["urn:data:left"])
        assert route.instance_support == "timeout" and route.error
        assert (route.source_count, route.matched_sources, route.min_count, route.max_count) == (
            None,
        ) * 4
        observe_path(
            route,
            probe.helper,
            ["urn:data:left", "urn:data:right"],
            type_context_graph_uris=["urn:types"],
        )
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


def test_select_publication_paths_and_preserve_evidence():
    def row(subject, predicate, target):
        return SchemaPattern(
            subject_class=f"urn:{subject}",
            property_uri=f"urn:{predicate}",
            object_class=target if target == "Literal" else f"urn:{target}",
            count=4,
            graphs={"urn:data": 4},
        )

    schema = MinedSchema(
        about=AboutMetadata(
            dataset_name="publications",
            graph_uris=["urn:data"],
            class_entity_counts={"urn:Article": 7},
            class_entity_count_states={"urn:Article": "partial"},
        ),
        patterns=[
            row("Article", "author", "Person"),
            row("Person", "name", "Literal"),
            row("Person", "affiliation", "Organization"),
            row("Person", "affiliation", "Consortium"),
            row("Person", "publication", "Article"),
            row("Article", "title", "Literal"),
            row("Other", "noise", "Literal"),
        ],
        collections=[
            CollectionProfile(
                subject_class="urn:Article",
                property_uri="urn:authors",
                member_types=["urn:Person"],
                member_kinds=["IRI"],
                graph_uri="urn:data",
                list_count=4,
            )
        ],
    )
    navigation = schema.discover_paths(max_hops=2, max_paths_per_length=30)
    paths = [
        p
        for p in navigation.paths
        if p.steps[0].property_uri == "urn:author"
        and p.steps[1].property_uri in {"urn:name", "urn:publication"}
    ]
    paths[0].instance_support = "matched"
    paths[0].source_count, paths[0].matched_sources = 7, 4
    selected = schema.select(
        paths=paths, fields=[("urn:Person", "urn:affiliation"), ("urn:Article", "urn:authors")]
    )
    assert len(selected.patterns) == 5, "Keep both affiliation ranges and the return path"
    assert {p.object_class for p in selected.patterns if p.property_uri == "urn:affiliation"} == {
        "urn:Organization",
        "urn:Consortium",
    }
    assert selected.collections == schema.collections, "Keep ordered author evidence"
    assert all(p.count == 4 and p.graphs == {"urn:data": 4} for p in selected.patterns)
    assert selected.source.about.class_entity_count_states == {"urn:Article": "partial"}
    assert selected.paths[0].matched_sources == 4
    restored = type(selected).model_validate_json(selected.model_dump_json())
    assert restored == selected, "Selected fields, paths and complete source survive saving"
    schema.patterns[0].count = 999
    assert selected.patterns[0].count == 4, "Selection retains its own evidence snapshot"
    with pytest.raises(ValueError, match="Unknown selected field"):
        schema.select(fields=[("urn:Article", "urn:missing")])
    foreign = paths[0].model_copy(deep=True)
    foreign.steps[0].property_uri = "urn:foreign"
    with pytest.raises(ValueError, match="Path is not retained"):
        schema.select(paths=[foreign])


def test_retrieve_selected_chemical_classifications():
    store = Store()
    store.load(
        input="""
        @prefix e: <urn:chemical:> .
        e:data {
            e:c1 a e:Chemical; e:group e:g1, e:g2 .
            e:c2 a e:Chemical; e:group e:untyped .
            e:c3 a e:Chemical .
            e:g1 e:label "PFAS"@en .
        }
        e:labels {
            e:g1 e:label "PFAS"@en .
            e:g2 e:label "Other"@en .
            e:untyped e:label "No group type"@en .
        }
        e:types {
            e:g1 a e:Group . e:g2 a e:Group .
            e:decoy a e:Chemical; e:group e:g1 .
            e:g1 e:label "Context label"@en .
        }
        e:outside { e:g1 e:label "Outside"@en . }
    """,
        format=RdfFormat.TRIG,
    )
    schema = MinedSchema(
        about=AboutMetadata(
            graph_uris=["urn:chemical:data", "urn:chemical:labels"],
            type_context_graph_uris=["urn:chemical:types"],
        ),
        patterns=[
            SchemaPattern(
                subject_class="urn:chemical:Chemical",
                property_uri="urn:chemical:group",
                object_class="urn:chemical:Group",
            ),
            SchemaPattern(
                subject_class="urn:chemical:Group",
                property_uri="urn:chemical:label",
                object_class="Literal",
                datatype="http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
            ),
        ],
    )
    path = schema.discover_paths(max_hops=2).paths[0]
    selected = SchemaSelection.model_validate_json(schema.select(paths=[path]).model_dump_json())
    query = selected.path_query(selected.paths[0])
    rows = list(store.query(query))
    matches = {
        (r["n0"].value, r["n1"].value, r["n2"].value, r["n2"].language)
        for r in rows
        if r["n2"] is not None
    }
    assert matches == {
        ("urn:chemical:c1", "urn:chemical:g1", "PFAS", "en"),
        ("urn:chemical:c1", "urn:chemical:g2", "Other", "en"),
    }, "Keep alternative groups, intermediate identities and language; exclude context edges"
    assert {r["n0"].value for r in rows} == {
        "urn:chemical:c1",
        "urn:chemical:c2",
        "urn:chemical:c3",
    }, "Keep missing classifications without adding context-only chemicals"
    assert len(rows) == 4, "Duplicate triples across data graphs must not duplicate output"
    assert len(list(store.query(selected.path_query(path, include_unmatched=False)))) == 2
    assert selected.paths[0].instance_support == "not_checked", "Building a query is not a probe"
    foreign = path.model_copy(deep=True)
    foreign.steps[0].property_uri = "urn:foreign"
    with pytest.raises(ValueError, match="Choose a selected path"):
        selected.path_query(foreign)
