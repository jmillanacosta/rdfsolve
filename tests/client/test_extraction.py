"""rdfsolve.client extraction and assessment: connected views are extracted with their original
terms and graph evidence, assessed against shapes, across data graphs, and with types from companion
graphs."""

import pytest
from rdflib import SH, Dataset, Graph, Literal, URIRef

from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client
from rdfsolve.client.hydration import HydrationLimitError


def test_extract_selected_connected_records(tmp_path):
    data = Dataset().parse(
        data="""@prefix e: <urn:example:> .
      e:data { e:c a e:Chemical; e:group e:g, e:h . e:missing a e:Chemical . }
      e:labels { e:g e:label "PFAS"@en . e:h e:label "Other"@en . }
      e:types { e:g a e:Group . e:h a e:Group; e:noise "excluded" . }
    """,
        format="trig",
    )
    with SchemaMiner.from_graph(
        data,
        graph_uris=["urn:example:data", "urn:example:labels"],
        type_context_graph_uris=["urn:example:types"],
        delay=0,
    ) as miner:
        schema = miner.mine("chemical-groups")
        nav = schema.discover_paths(max_hops=2)
    path = next(p for p in nav.paths if p.steps[-1].property_uri == "urn:example:label")
    selection = schema.select(paths=[path])
    with Client(schema, data) as client:
        model = client.model("Chemical")
        records = client.sample(model, limit=2)
        result = client.extract(selection, root_class=model, roots=[str(r.uri) for r in records])
        assert len(result.roots) == 2, "Retain chemicals without the selected relationship"
        assert len(result.quads) == 8, "Two roots, two links, two labels and two group types"
        assert {q.graph for q in result.quads} == {
            "urn:example:data",
            "urn:example:labels",
            "urn:example:types",
        }
        assert all(q.predicate != "urn:example:noise" for q in result.quads)
        labels = [q.object for q in result.quads if q.predicate == "urn:example:label"]
        assert {t.value for t in labels} == {"PFAS", "Other"} and all(
            t.language == "en" for t in labels
        )
        assert result.selection.source.about.snapshot_id == schema.about.snapshot_id
        restored = type(result).model_validate_json(result.model_dump_json())
        target = tmp_path / "selected.trig"
        restored.save(target)
        saved = Dataset().parse(target, format="trig")
        assert len(saved.graph(URIRef("urn:example:labels"))) == 2
        only = client.extract(selection, root_class="Chemical", roots=["urn:example:missing"])
        assert len(only.roots) == 1 and len(only.quads) == 1
        chosen = client.extract(selection, root_class="Chemical", roots=["urn:example:c"])
        assert len(chosen.roots) == 1 and len(chosen.quads) == 7
        assert all(q.subject.value != "urn:example:missing" for q in chosen.quads)
        empty = client.extract(selection, root_class="Chemical", roots=[])
        assert not empty.roots and not empty.quads, "An empty root selection retrieves nothing"
    with Client(schema, data, max_rows=1) as client, pytest.raises(HydrationLimitError):
        client.extract(selection, root_class="urn:example:Chemical")


def test_selection_assessment_retains_evidence_and_limits():
    data = Dataset().parse(
        data="""@prefix e: <urn:example:> .
        e:data { e:a a e:Cell; e:ref e:r . e:b a e:Cell . e:r a e:Reference . }
    """,
        format="trig",
    )
    with SchemaMiner.from_graph(data, graph_uris=["urn:example:data"], delay=0) as miner:
        schema = miner.mine("references")
    with Client(schema, data) as client:
        selected = client.extract(
            schema.select(fields=[("urn:example:Cell", "urn:example:ref")]),
            root_class="urn:example:Cell",
        )
    shapes = Graph().parse(
        data="""@prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:shape> a sh:NodeShape; sh:targetClass <urn:example:Cell>;
            sh:property [sh:path <urn:example:ref>; sh:minCount 1; sh:class <urn:example:Reference>] .
    """,
        format="turtle",
    )
    shapes.parse(
        data="""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:unmatched> a sh:NodeShape; sh:targetClass <urn:example:Other>;
          sh:closed true; sh:property [sh:path <urn:irrelevant>; sh:minCount 1] .
    """,
        format="turtle",
    )
    before = set(shapes)
    checked = selected.assess(shapes)
    assert checked.state == "violations" and checked.conforms is False
    assert checked.focus_nodes == 2 and checked.focus_counts == {"urn:shape": 2, "urn:unmatched": 0}
    assert not any("urn:unmatched" in warning for warning in checked.scope_warnings)
    assert any(item.focus.value == "urn:example:b" for item in checked.violations)
    assert checked.source_conforms is None, "A selected export cannot certify its whole source"
    assert checked.selection_rows == 1 and checked.ontology_consistency == "not_checked"
    assert set(shapes) == before, "Validation must preserve declarations"
    shapes.remove((URIRef("urn:unmatched"), None, None))
    shapes.set((URIRef("urn:shape"), SH.deactivated, Literal(True)))
    inactive = selected.assess(shapes)
    assert inactive.state == "not_checked" and inactive.conforms is None
    assert inactive.deactivated_shapes == 1
    shapes.remove((URIRef("urn:shape"), SH.deactivated, None))
    prop = shapes.value(URIRef("urn:shape"), SH.property)
    shapes.set((prop, SH.path, URIRef("urn:example:unselected")))
    assert selected.assess(shapes).scope_warnings, "Name requirements outside extracted fields"

    ontology = Graph().parse(
        data="""@prefix owl: <http://www.w3.org/2002/07/owl#> .
        <urn:example:Cell> owl:disjointWith <urn:example:Reference> .
        <urn:example:a> a <urn:example:Reference> .
    """,
        format="turtle",
    )
    reasoned = selected.assess(shapes, ontology=ontology, inference="owlrl")
    assert reasoned.ontology_errors, "External OWL-RL must report incompatible asserted classes"
    assert reasoned.ontology_consistency == "contradiction_reported"

    shapes.set((URIRef("urn:shape"), SH.targetClass, URIRef("urn:example:Missing")))
    empty = selected.assess(shapes)
    assert empty.state == "not_checked" and empty.conforms is None
    assert empty.focus_nodes == 0 and "No focus nodes" in empty.message
    shapes.set((URIRef("urn:shape"), SH.targetClass, URIRef("urn:example:Parent")))
    ontology = Graph().parse(
        data="""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        <urn:example:Cell> rdfs:subClassOf <urn:example:Parent> .
    """,
        format="turtle",
    )
    inferred = selected.assess(shapes, ontology=ontology, inference="rdfs")
    assert inferred.focus_nodes == 2, "Count targets after the validator applies context"


def test_companion_types_across_client_operations():
    data = Dataset().parse(
        data="""@prefix e: <urn:example:> .
      e:data { e:c a e:Chemical; e:group e:g . e:g e:label "PFAS" . }
      e:types { e:g a e:Group; e:label "Excluded" . e:ghost a e:Group . }
    """,
        format="trig",
    )
    with SchemaMiner.from_graph(
        data,
        graph_uris=["urn:example:data"],
        type_context_graph_uris=["urn:example:types"],
        delay=0,
    ) as miner:
        schema = miner.mine("groups")
        schema.discover_paths(max_hops=2)
    with Client(schema, data) as client:
        group = client.model("urn:example:Group")
        label = client.field_name(group, "urn:example:label")
        records = client.sample(group, fields=[label])
        assert len(records) == 1 and getattr(records[0], label) == ["PFAS"], (
            "Sample data subjects only"
        )
        assert "urn:example:Group" in [t["value"] for t in records[0].rdf_terms["@type"]], (
            "Keep companion type evidence"
        )
        values = client.field_values("urn:example:Group", label)
        assert len(values) == 1
        chemical = client.model("urn:example:Chemical")
        source = client.sample(chemical)
        linked = client.follow(source, "group", group, fields=[label])
        assert len(linked) == 1 and getattr(linked[0], label) == ["PFAS"]
        paths = client.navigation()
        route = paths[(paths.Source == "urn:example:Chemical") & (paths.Target == "Literal")].iloc[
            0
        ]
        result = client.select(client.prepare_path(route.Reference))
        assert result.row_count == 1, "Generated paths must use the same typing scope as mining"
        assert all(cell.value != "Excluded" for row in result.rows for cell in row.values())

        actual = client.paths_between(source[0], "urn:example:Group", max_hops=1)
        assert len(actual) == 1, "Record paths must find targets typed in companion graphs"
        assert actual.iloc[0]["To class"] == client.type_name(group), (
            "Show the target's companion type"
        )
        connections = client.connections(source[0], max_hops=1, both_directions=False)
        assert len(connections) == 1, "Companion properties must not become data links"
        assert connections.iloc[0]["To class"] == client.type_name(group)

        found = client.find("PFAS", kind="urn:example:Group", field=label)
        searched = client.search(["PFAS"], kind="urn:example:Group", fields=[label])
        assert len(found) == len(searched) == 1, (
            "Search must use the same companion types as follow"
        )
        assert not client.find("Excluded", kind="urn:example:Group", field=label)


def test_classification_client_joins_scoped_graphs(tmp_path):
    data = Dataset(default_union=False)
    data.parse(
        data="""
        @prefix e: <urn:chemical:> .
        e:links { e:c a e:Chemical; e:group e:g1, e:g2 . }
        e:labels {
            e:g1 a e:Group; e:label "PFAS"@en .
            e:g2 a e:Group; e:label "Other"@en .
        }
        e:outside { e:g1 e:label "Outside"@en . }
    """,
        format="trig",
    )
    scope = ["urn:chemical:links", "urn:chemical:labels"]
    with SchemaMiner.from_graph(data, graph_uris=scope, delay=0) as miner:
        assert miner.helper.local.metadata()["engine"] == "oxigraph"
        schema = miner.mine("classification")
        schema.discover_paths(max_hops=2)
    with Client(schema, data) as client:
        table = client.navigation()
        ref = (
            table[(table.Source == "urn:chemical:Chemical") & (table.Target == "Literal")]
            .iloc[0]
            .Reference
        )
        query = client.prepare_path(ref)
        result = client.select(query)
        assert {r["target"].value for r in result.rows} == {"PFAS", "Other"}, (
            "Join across selected data graphs without outside labels"
        )
        assert all(r["target"].lang == "en" for r in result.rows)
        assert all(f"FROM <{g}>" in query.sparql for g in scope)
        model = client.model("urn:chemical:Chemical")
        assert model.model_json_schema()["graph_uris"] == scope
        view = client.with_paths(model, group_labels=["urn:chemical:group", "urn:chemical:label"])
        wide_view = view
        record = client.get(view, "urn:chemical:c", fields=["group_labels"])
        assert set(record.group_labels) == {"PFAS", "Other"}
        assert record.rdf_source["graph_uris"] == scope
        assert client.session_metadata()["local_backend"]["engine"] == "oxigraph"
        client.save_session(tmp_path / "session.json")
    data.serialize(tmp_path / "data.trig", format="trig")
    for opened in (
        Client.open(schema, data_file=tmp_path / "data.trig"),
        Client.from_session(tmp_path / "session.json", data_file=tmp_path / "data.trig"),
    ):
        with opened as restored:
            assert restored.graph_uris == scope, "Reopening must retain the selected named graphs"
            model = restored.model("urn:chemical:Chemical")
            view = restored.with_paths(
                model, group_labels=["urn:chemical:group", "urn:chemical:label"]
            )
            assert set(
                restored.get(view, "urn:chemical:c", fields=["group_labels"]).group_labels
            ) == {"PFAS", "Other"}
    with SchemaMiner.from_graph(data, graph_uris=[scope[0]], delay=0) as miner:
        narrow = miner.mine("classification-links")
    with Client(narrow, data) as client:
        model = client.model("urn:chemical:Chemical")
        assert model.model_json_schema()["graph_uris"] == [scope[0]]
        view = client.with_paths(model, group_labels=["urn:chemical:group", "urn:chemical:label"])
        assert client.get(view, "urn:chemical:c", fields=["group_labels"]).group_labels == []
        queries = len(client.queries)
        with pytest.raises(ValueError, match="Model graph scope"):
            client.get(wide_view, "urn:chemical:c", fields=["group_labels"])
        assert len(client.queries) == queries, (
            "Reject a model from another graph scope before querying"
        )
