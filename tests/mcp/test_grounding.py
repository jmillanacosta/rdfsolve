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
