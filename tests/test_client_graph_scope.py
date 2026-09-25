"""Retrieve classifications across the data graphs represented by a model."""

import pytest
from rdflib import Dataset

from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client


def test_classification_client_joins_scoped_graphs(tmp_path):
    data = Dataset(default_union=False)
    data.parse(data="""
        @prefix e: <urn:chemical:> .
        e:links { e:c a e:Chemical; e:group e:g1, e:g2 . }
        e:labels {
            e:g1 a e:Group; e:label "PFAS"@en .
            e:g2 a e:Group; e:label "Other"@en .
        }
        e:outside { e:g1 e:label "Outside"@en . }
    """, format="trig")
    scope = ["urn:chemical:links", "urn:chemical:labels"]
    with SchemaMiner.from_graph(data, graph_uris=scope, delay=0) as miner:
        assert miner.helper.local.metadata()["engine"] == "oxigraph"
        schema = miner.mine("classification")
        schema.discover_paths(max_hops=2)
    with Client(schema, data) as client:
        table = client.navigation()
        ref = table[(table.Source == "urn:chemical:Chemical") &
                    (table.Target == "Literal")].iloc[0].Reference
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
            view = restored.with_paths(model, group_labels=["urn:chemical:group", "urn:chemical:label"])
            assert set(restored.get(view, "urn:chemical:c", fields=["group_labels"]).group_labels) == {"PFAS", "Other"}
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
        assert len(client.queries) == queries, "Reject a model from another graph scope before querying"
