"""The retained schema and generated models must determine grounding."""

from conftest import prepare

import json

from conftest import E, declare, field, insert, values
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


def test_typed_entity_and_package_paths_anchor_the_answer(session):
    found = session.find("Human", str(E.Taxon))
    ref = found["items"][0]["ref"]
    assert isinstance(session.records[ref], BaseModel)
    assert session.records[ref].uri == str(E.Human)
    before = len(session.client.queries)
    assert session.find("Human", str(E.Taxon)) == found
    assert len(session.client.queries) == before
    path = session.paths(str(E.AOP), ref, max_hops=1)["items"][0]
    declare(session, "Pathways applicable to Human")
    rows = values(
        session,
        prepare(session, {"g1": {"pattern": insert(path["ref"], "a", "tax"), "project": ["a"]}}),
    )
    assert {r["a"]["value"] for r in rows} == {str(E.humanAOP), str(E.noChemicalAOP)}


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
    declare(s, "Return connected B resources")
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
    import inspect

    # The fixture mappings use the package's mapping model and real registry.
    from rdfsolve.class_derivation import ClassPair, derive_class_mappings
    from rdfsolve.class_index import ClassIndex
    from rdfsolve.registry import Registry, TypeDescription

    pair = ClassPair(
        source_class=str(E.Taxon),
        target_class="urn:peer:Species",
        source_dataset="rdf",
        target_dataset="peer",
        predicate="http://www.w3.org/2004/02/skos/core#relatedMatch",
        source_entities={str(E.Human)},
        target_entities={"urn:peer:Human"},
        instance_count=1,
    )
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
    mapped = Session(session.client, class_mappings=[pair], related_registries=[peer])
    result = mapped.schema(["Species"])
    assert any(
        item["ref"] == mapped.catalogue.type_refs[str(E.Taxon)]
        for item in result["matches"][0]["items"]
    )
    assert mapped.catalogue.mapping_status["indexed_links"] == 1
    assert "urn:peer:Species" not in mapped.catalogue.known_iris
