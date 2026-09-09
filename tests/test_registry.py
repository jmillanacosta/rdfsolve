"""Exercise discovery and route execution against retained AOPWiki statements."""

import json

import pytest
from rdflib import Dataset, RDF, URIRef

from rdfsolve.client_api import Client
from rdfsolve.hydration import HydrationLimitError
from rdfsolve.registry import Registry
from tests.test_client_api import AOP, CHEMICAL, DATA, client


def test_registry_roundtrip_and_rejected_documents(tmp_path):
    with client() as data:
        registry = data.registry(source_id="aopwikirdf")
        assert not data.queries
        assert registry.describe("read")["arguments"]["additionalProperties"] is False
        path = tmp_path / "registry.json"
        registry.write(path)
        assert Registry.read(path) == registry
        raw = json.loads(path.read_text())
        for bad in ({**raw, "format_version": 2}, {**raw, "source_id": "changed"},
                    {**raw, "operations": raw["operations"] * 2}, {**raw, "load_code": "untrusted.py"}):
            path.write_text(json.dumps(bad))
            with pytest.raises(ValueError):
                Registry.read(path)
        data.graph_uris = ["http://aopwiki.org/"]
        assert data.registry(source_id="aopwikirdf").revision != registry.revision


def test_indirect_search_returns_real_connections_and_full_evidence():
    with client() as data:
        session = data.session(source_id="aopwikirdf")
        assert not session.search(["thyroxine"], kind=AOP)["rows"]
        found = session.search(["thyroxine"])
        from rdfsolve.schema_models.enrichment import RdfTerm
        for match in found["evidence"]:
            assert (URIRef(match["id"]), URIRef(match["predicate"]),
                    RdfTerm.model_validate(match["text"]).to_rdf()) in data.source
            assert "thyroxine" in match["text"]["value"].lower()
        before = len(data.queries)
        routes = session.paths(found["reference"], AOP)
        assert len(data.queries) == before
        read = session.read(found["reference"], paths=[r["id"] for r in routes["paths"]], fields=["title"])
        assert [row["id"] for row in read["rows"]] == ["https://identifiers.org/aop/162"]
        for match in read["evidence"]:
            for i, link in enumerate(match["links"]):
                left, right = (i + 1, i) if link["inverse"] else (i, i + 1)
                assert (URIRef(match["nodes"][left]["value"]), URIRef(link["predicate"]),
                        URIRef(match["nodes"][right]["value"])) in data.source
        before = len(data.queries)
        exported = session.export_result(read["reference"])
        assert exported["evidence"] and len(data.queries) == before
        ids = {query["id"] for query in exported["queries"]}
        assert all(match["query_id"] in ids for match in exported["evidence"])


def test_saved_enrichment_reaches_discovery_without_whole_schema():
    from pathlib import Path
    with Client.open(Path(__file__).parents[1] / "notebooks/data/aopwikirdf.schema.json", data_file=DATA) as data:
        session = data.session(source_id="aopwikirdf")
        description = session.schema(text="HUGO")
        assert any("HUGO" in (item["description"] or "") for item in description["types"])
        fields = session.schema(kind="Key Event", text="gene", limit=30)
        assert any(field["description"] for item in fields["types"] for field in item["fields"])
        assert "patterns" not in json.dumps(description) and not data.queries
        with pytest.raises(ValueError, match="Gene identifier"):
            session.plan("Adverse Outcome Pathway", ["Key Event"], ["thyroid"],
                         "Thyroid-related pathways and their linked genes")
        plan = session.plan("Adverse Outcome Pathway", ["Gene identifier"], ["thyroid"],
                            "Thyroid-related pathways and their linked genes", evidence="gene evidence")
        routes = plan["routes"][0]["paths"]
        assert {route["steps"][0]["to"] for route in routes} >= {"Key Event", "Key Event Relationship"}
        assert all(route["hops"] == 2 for route in routes)
        assert not data.queries


def test_answer_plan_does_not_confuse_text_matches_with_links():
    from rdfsolve.answer_plan import plan_status

    with client() as data:
        session = data.session(source_id="aopwikirdf")
        plan = session.plan(AOP, [CHEMICAL], ["carcinomas"], "Pathways and connected chemicals")
        source = session.search(["carcinomas"], kind=AOP)
        session.search(["Phenobarbital"], kind=CHEMICAL)
        assert plan_status(session)["pending"]
        assert plan_status(session)["coverage"][0]["status"] == "route not tried"
        with pytest.raises(ValueError, match="destination class"):
            session.read(source["reference"], paths=[p["id"] for p in plan["routes"][0]["paths"]],
                         fields=["missing_field"])
        assert plan_status(session)["pending"]
        result = session.read(source["reference"], paths=[p["id"] for p in plan["routes"][0]["paths"]])
        status = plan_status(session)
        assert not status["pending"]
        assert status["coverage"][0]["references"] == [result["reference"]]
        assert status["coverage"][0]["status"] == "linked records retrieved"


def test_read_pages_before_loading_and_reports_partial_searches():
    with client() as data:
        session = data.session(source_id="aopwikirdf", preview_rows=1)
        found = session.search(["carcinomas"], kind=AOP)
        reference = found["reference"]
        session.read(reference, fields=["C54571"])
        records = session.result(reference).records
        assert "c54571" in records[0].rdf_loaded_fields
        assert "c54571" not in records[1].rdf_loaded_fields
        before = len(data.queries)
        session.read(reference, fields=["C54571"])
        assert len(data.queries) == before
        session.read(reference, fields=["C54571"], offset=1)
        assert len(data.queries) == before + 1
        data.max_subjects = 1
        limited = session.search(["carcinomas"], kind=AOP)
        assert limited["status"] == "partial" and limited["coverage"]["limit_reached"]
        assert data.session_metadata()["operations"][-1]["status"] == "partial"


def test_route_cannot_join_across_graphs_and_limits_are_explicit():
    with client() as original:
        dataset = Dataset()
        # Split the two real edges across graphs. Their union would produce a false route.
        first, second = dataset.graph(URIRef("urn:first")), dataset.graph(URIRef("urn:second"))
        for triple in original.source:
            if triple[1] == RDF.type or str(triple[1]).endswith("title"):
                first.add(triple)
                second.add(triple)
            else:
                (second if str(triple[1]).endswith("has_chemical_entity") else first).add(triple)
        with Client(original._schema, dataset, graph_uris=["urn:first", "urn:second"]) as data:
            session = data.session(source_id="aopwikirdf")
            found = session.search(["carcinomas"], kind=AOP)
            paths = session.paths(AOP, CHEMICAL)["paths"]
            assert paths
            assert session.read(found["reference"], paths=[p["id"] for p in paths])["rows"] == []
        session = original.session(source_id="aopwikirdf", max_results=1)
        for op, args in (("delete", {}), ("search", {"terms": [" "]}),
                         ("read", {"reference": "missing"}), ("read", {"iri": "urn:x"}),
                         ("read", {"reference": "missing", "limit": "20"})):
            with pytest.raises(ValueError):
                session.call(op, args)
        assert not original.queries
        found = session.search(["Phenobarbital"], kind=CHEMICAL)
        before = len(original.queries)
        with pytest.raises(HydrationLimitError):
            session.search(["thyroid"])
        with pytest.raises(ValueError):
            session.read(found["reference"], fields=["missing"])
        original.graph_uris = ["urn:changed"]
        with pytest.raises(ValueError, match="scope"):
            session.read(found["reference"])
        assert len(original.queries) == before
