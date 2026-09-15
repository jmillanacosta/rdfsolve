"""The retained schema and generated models must determine grounding."""

import json

import pytest
from conftest import E, declare, field, insert, prepare, values
from pydantic import BaseModel
from rdflib import RDF, Graph, Literal, Namespace

from rdfsolve.client_api import Client
from rdfsolve.mcp.session import Session
from rdfsolve.schema_models.core import MinedSchema


def test_connected_schema_requires_no_endpoint_query(session):
    result = session.schema(["Adverse Outcome Pathway", "Key Event", "Taxon"])
    assert {c["label"] for c in result["connections"]["items"]} >= {
        "has Key Event",
        "applicable taxon",
    }
    assert not session.client.queries
    selected = session.schema(owners=[str(E.AOP)], targets=[str(E.Event)])
    assert len(selected["matches"][0]["items"]) == 1
    partial = session.schema(["Key Event"], owners=[str(E.AOP)], targets=["a named individual"])
    assert partial["unresolved_types"] == ["a named individual"]
    assert partial["matches"][0]["items"] and not session.client.queries
    fallback = session.inspect(
        session.catalogue.type_refs[str(E.Event)], text="undocumented concept"
    )
    index = fallback["unmatched_field_indexes"][session.catalogue.type_refs[str(E.Event)]]
    assert any(row["label"] == "measurement method" for row in index["items"])
    assert not index["more"] and not session.client.queries
    field_card = next(c for c in result["connections"]["items"] if c["label"] == "applicable taxon")
    target = next(t for t in field_card["targets"] if t["label"] == "Taxon")
    found = session.find("Human", target["ref"])
    assert found["searched_class"] == target
    assert found["items"][0]["iri"] == str(E.Human)
    with pytest.raises(ValueError, match=target["ref"]):
        session.paths(field_card["ref"], target["ref"])


def test_typed_entity_and_package_paths_anchor_the_answer(session):
    found = session.find("Human", str(E.Taxon))
    ref = found["items"][0]["ref"]
    assert isinstance(session.records[ref], BaseModel)
    assert session.records[ref].uri == str(E.Human)
    before = len(session.client.queries)
    assert session.find("Human", str(E.Taxon)) == found
    assert len(session.client.queries) == before
    path = session.paths(str(E.AOP), ref, max_hops=1)["items"][0]
    declare(session, "Pathways applicable to Human", concept="Adverse Outcome Pathway")
    rows = values(
        session,
        prepare(session, {"g1": {"pattern": insert(path["ref"], "a", "tax"), "project": ["a"]}}),
    )
    assert {r["a"]["value"] for r in rows} == {str(E.humanAOP), str(E.noChemicalAOP)}


def test_candidates_beyond_first_page_remain_usable(session):
    for i in range(7):
        session.client.source.add((E[f"member{i}"], RDF.type, E.Taxon))
        session.client.source.add(
            (
                E[f"member{i}"],
                Namespace("http://www.w3.org/2000/01/rdf-schema#").label,
                Literal("Shared name"),
            )
        )
    first = session.find("Shared name", str(E.Taxon))
    count = len(session.client.queries)
    second = session.find("Shared name", str(E.Taxon), offset=first["next_offset"])
    assert len(first["items"]) == 4 and len(second["items"]) == 3
    assert not second["more"] and len(session.client.queries) == count
    assert session.paths(str(E.AOP), second["items"][-1]["ref"], max_hops=1)["items"]


def test_shape_only_sequence_and_inverse_execute():
    schema = MinedSchema.from_shacl("""@prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix e: <urn:shape:> . e:AShape a sh:NodeShape; sh:targetClass e:A;
      sh:property [sh:path (e:link [sh:inversePath e:back]); sh:name "related"; sh:class e:B] .
    e:BShape a sh:NodeShape; sh:targetClass e:B .""")
    g = Graph().parse(
        data="""@prefix e: <urn:shape:> . e:a a e:A; e:link e:x . e:b a e:B; e:back e:x .""",
        format="turtle",
    )
    s = Session(Client(schema, g, graph_uris=[]))
    assert not schema.patterns
    path = s.paths("urn:shape:A", "urn:shape:B", max_hops=2)["items"][0]
    assert path["complexity"]["max_hops"] == 2
    declare(s, "Return connected B resources", concept="B")
    rows = values(
        s, prepare(s, {"g1": {"pattern": insert(path["ref"], "a", "b"), "project": ["b"]}})
    )
    assert rows == [{"b": {"type": "uri", "value": "urn:shape:b"}}]
    rows = values(
        s,
        prepare(s, {"g1": {"pattern": insert(path["ref"], "a", "shared", "b"), "project": ["b"]}}),
    )
    assert rows == [{"b": {"type": "uri", "value": "urn:shape:b"}}]


def test_mapping_evidence_improves_local_class_retrieval(session):
    from rdfsolve.class_derivation import derive_class_mappings
    from rdfsolve.class_index import ClassIndex, EntityClassInfo
    from rdfsolve.mapping_models.core import MappingEdge
    from rdfsolve.registry import TypeDescription

    index = ClassIndex(
        endpoint_url="urn:local",
        entities={
            str(E.Human): EntityClassInfo(
                entity_iri=str(E.Human), graph_classes={"rdf": [str(E.Taxon)]}
            ),
            "urn:peer:Human": EntityClassInfo(
                entity_iri="urn:peer:Human", graph_classes={"peer": ["urn:peer:Species"]}
            ),
        },
    )
    pairs, stats = derive_class_mappings(
        [
            MappingEdge(
                source_class=str(E.Human),
                target_class="urn:peer:Human",
                source_dataset="rdf",
                target_dataset="peer",
                predicate="http://www.w3.org/2004/02/skos/core#relatedMatch",
            )
        ],
        index,
    )
    assert stats["processed_edges"] == 1
    peer = session.catalogue.registry.model_copy(
        update={
            "source_id": "peer",
            "types": [
                TypeDescription(
                    id="urn:peer:Species",
                    label="Species",
                    fields=[],
                    description="Organism taxonomy",
                )
            ],
        }
    )
    mapped = Session(session.client, class_mappings=pairs, related_registries=[peer])
    result = mapped.schema(["Species"])
    assert any(
        item["ref"] == mapped.catalogue.type_refs[str(E.Taxon)]
        for item in result["matches"][0]["items"]
    )
    assert mapped.catalogue.mapping_status["indexed_links"] == 1
    assert "urn:peer:Species" not in mapped.catalogue.known_iris
    card = next(i for i in result["matches"][0]["items"] if i["kind"] == "type")
    evidence = mapped.inspect(card["mapped_context"][0]["ref"])
    assert evidence["related_source"] == "peer" and evidence["support"]["mapped_pairs"] == 1


def test_mcp_chains_whole_client_selections_and_returns_query_trace(session, tmp_path):
    from rdfsolve.mcp.server import dispatch

    found = dispatch(
        session, "rdf_find", {"text": "Human", "target": "Adverse Outcome Pathway", "max_hops": 1}
    )
    assert found["selection"] in session.selections
    assert found["connections"]["items"][0]["evidence"]["status"] == "matched"
    assert found["trace"]["query_ids"]
    paths = dispatch(
        session,
        "rdf_paths",
        {"source": "Adverse Outcome Pathway", "target": found["selection"], "max_hops": 1},
    )
    assert paths["items"][0]["evidence"]["matches"] == 2
    assert "Assay A" not in str(paths)
    assert session.diagnostics()["source_queries"] == len(session.client.queries)
    assert session.client.query_log().queries
    again = dispatch(
        session, "rdf_find", {"text": "Human", "target": "Adverse Outcome Pathway", "max_hops": 1}
    )
    assert again["trace"]["query_ids"] == []
    session.log_path = tmp_path / "client.json"
    dispatch(session, "rdf_inspect", {"ref": found["selection"]})
    from rdfsolve.query_log import QueryLog

    log = QueryLog.read(session.log_path)
    assert len(log.queries) == len(session.client.queries)
    assert all(q["result_retained"] for q in log.queries)
    assert not log.tools().empty


def test_ontology_evidence_unlocks_a_local_field_without_changing_scope(monkeypatch):
    from tests.test_ontology import EVENT, MMO, fixture, provider

    g = Graph().parse(data=f'<urn:event> a <{EVENT}>; <{MMO}> "Assay" .', format="turtle")
    for enabled in (False, True):
        with fixture(provider(monkeypatch) if enabled else False) as client:
            client.source += g
            s = Session(client)
            observation = s.schema(question="Return events and available measurement methods", goals=[
                {"clause": "Return events", "kind": "output", "concept": "Event"},
                {"clause": "Available measurement methods", "kind": "output", "concept": "measurement method", "owner": EVENT, "required": False},
            ])
            query = f'SELECT ?event ?method WHERE {{ ?event a <{EVENT}> . OPTIONAL {{ ?event <{MMO}> ?method }} }}'
            if not enabled:
                with pytest.raises(ValueError, match="unresolved|evidence|meaning|explain"):
                    s.prepare(query)
            else:
                cards = [c for match in observation["matches"] for c in match["items"]]
                assert any(c.get("ontology_evidence") for c in cards)
                rows = values(s, s.prepare(query))
                assert rows == [{"event": {"type": "uri", "value": "urn:event"}, "method": {"type": "literal", "value": "Assay"}}]
                assert "urn:parent" not in query
                assert "Recorded value restrictions: 0" in next(iter(s.executions.values()))["strategy"]


def test_partial_word_match_cannot_ground_a_field(monkeypatch):
    from rdfsolve.ontology import OntologyLookup, canonical_iri
    from rdfsolve.schema_models.pattern import SchemaPattern

    predicate = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C17469"
    lookup = OntologyLookup()
    monkeypatch.setattr(lookup, "_json", lambda *a, **k: {"_embedded": {"terms": [{
        "iri": canonical_iri(predicate), "label": "Taxonomy", "description": ["The science of classification."],
    }]}})
    schema = MinedSchema(about={"dataset_name": "test"}, patterns=[
        SchemaPattern(subject_class=str(E.Event), property_uri=predicate, object_class="Literal")])
    with Client(schema, Graph(), graph_uris=[], ontology_grounding=lookup) as client:
        session = Session(client)
        session.schema(question="Return event taxon values", goals=[
            {"clause": "Return event taxon values", "kind": "output", "concept": "taxon", "owner": str(E.Event)}])
        client.vocabulary(predicate)
        session.catalogue.search("taxon", owners=[str(E.Event)])
        ref = next(iter(session.catalogue.field_refs.values()))
        query = f'SELECT ?event ?value WHERE {{ ?event a <{E.Event}> ; <{predicate}> ?value }}'
        with pytest.raises(ValueError, match="explain"):
            session.prepare(query, grounding={"g1": {"evidence": [ref]}})


def test_discovery_retains_goals_and_accepts_an_added_restriction(session):
    question = 'Pathways with their events, applicable to Human'
    output = dict(clause='Return pathways', kind='output', concept='Adverse Outcome Pathway')
    session.schema(question=question, goals=[output])
    human = dict(clause='Applicable to Human', kind='entity_filter', concept='applicable taxon', value='Human', owner='a')
    session.schema(question=question, goals=[human])
    assert [r.clause for r in session.requirements.values()] == ['Return pathways', 'Applicable to Human']
    session.schema(question=question, goals=[output])
    assert session.requirements['g2'].value == 'Human'
    with pytest.raises(ValueError, match='Preserve the value restriction'):
        session.schema(question=question, goals=[dict(human, value='Mouse')])
    assert session.requirements['g2'].value == 'Human'
