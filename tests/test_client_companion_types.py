"""Retrieve selected data fields with companion type declarations."""
from rdflib import Dataset
from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client

def test_companion_types_across_client_operations():
    data = Dataset().parse(data='''@prefix e: <urn:example:> .
      e:data { e:c a e:Chemical; e:group e:g . e:g e:label "PFAS" . }
      e:types { e:g a e:Group; e:label "Excluded" . e:ghost a e:Group . }
    ''', format="trig")
    with SchemaMiner.from_graph(data, graph_uris=["urn:example:data"],
            type_context_graph_uris=["urn:example:types"], delay=0) as miner:
        schema = miner.mine("groups")
        schema.discover_paths(max_hops=2)
    with Client(schema, data) as client:
        group = client.model("urn:example:Group")
        label = client.field_name(group, "urn:example:label")
        records = client.sample(group, fields=[label])
        assert len(records) == 1 and getattr(records[0], label) == ["PFAS"], "Sample data subjects only"
        assert "urn:example:Group" in [t["value"] for t in records[0].rdf_terms["@type"]], "Keep companion type evidence"
        values = client.field_values("urn:example:Group", label)
        assert len(values) == 1
        chemical = client.model("urn:example:Chemical")
        source = client.sample(chemical)
        linked = client.follow(source, "group", group, fields=[label])
        assert len(linked) == 1 and getattr(linked[0], label) == ["PFAS"]
        paths = client.navigation()
        route = paths[(paths.Source == "urn:example:Chemical") & (paths.Target == "Literal")].iloc[0]
        result = client.select(client.prepare_path(route.Reference))
        assert result.row_count == 1, "Generated paths must use the same typing scope as mining"
        assert all(cell.value != "Excluded" for row in result.rows for cell in row.values())

        actual = client.paths_between(source[0], "urn:example:Group", max_hops=1)
        assert len(actual) == 1, "Record paths must find targets typed in companion graphs"
        assert actual.iloc[0]["To class"] == client.type_name(group), "Show the target's companion type"
        connections = client.connections(source[0], max_hops=1, both_directions=False)
        assert len(connections) == 1, "Companion properties must not become data links"
        assert connections.iloc[0]["To class"] == client.type_name(group)
