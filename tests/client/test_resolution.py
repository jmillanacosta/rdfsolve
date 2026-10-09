"""rdfsolve.client resolution: names, IRIs and CURIEs are resolved to classes, fields and records,
and a client is opened from a saved schema."""

import json
from unittest.mock import patch

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.client.api import Client
from rdfsolve.client.resolution import ResolutionError
from rdfsolve.ontology import Ontologies
from rdfsolve.schema_models import MinedSchema
from tests.client.test_api import CHEMICAL, DATA, client

SHACL = """
@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix e: <urn:e:> .
e:Person <http://www.w3.org/2000/01/rdf-schema#label> "人物" .
e:P a sh:NodeShape; sh:targetClass e:Person; sh:name "人物";
  sh:property [sh:path e:affiliation; sh:name "member of"; sh:nodeKind sh:IRI],
              [sh:path e:name; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
e:O a sh:NodeShape; sh:targetClass e:Organisation;
  sh:property [sh:path e:title; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
"""
CLIENT_RESOLUTION_DATA = """
@prefix e: <urn:e:> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
e:data {
  e:one a e:Person, e:External; e:affiliation e:org; e:name "Ada";
        e:about <http://purl.obolibrary.org/obo/CHEBI_53289> .
  e:ext a e:External; e:affiliation e:org .
  e:org a e:Organisation; e:title "Institute" .
  e:Local a <http://www.w3.org/2002/07/owl#Class>; rdfs:label "Researcher" .
  e:loc a e:Local .
}
e:excluded { e:three a e:OtherExternal . }
"""


def test_resolution_reports_candidates_semantics_and_evidence(tmp_path):
    schema = MinedSchema.from_shacl(SHACL)
    lookup = Ontologies(offline=True)
    graph = Dataset().parse(data=CLIENT_RESOLUTION_DATA, format="trig")
    external = [
        {"iri": "urn:e:External", "label": "Researcher"},
        {"iri": "urn:e:OtherExternal", "label": "Researcher"},
    ]
    with Client(schema, graph, graph_uris=["urn:e:data"]) as client:
        with patch.object(Ontologies, "search", return_value=external):
            named = client.resolve("Researcher", kind="class", external_names=True)
        assert named.status == "ambiguous", "A schema, source or external name has no precedence"
        origins = {c.iri: (c.origin, c.use) for c in named.candidates}
        assert origins == {
            "urn:e:Local": ("source label", "used as class"),
            "urn:e:External": ("external ontology", "used as class"),
            "urn:e:OtherExternal": ("external ontology", "not used as class"),
        }
        assert client.ontology is None, "An external lookup must not enable later lookups"

        person = client.resolve("人物", kind="class")
        assert (person.status, person.iri) == ("resolved", "urn:e:Person")
        assert any("subclass" in note for note in person.notes), "State the rdf:type semantics"

        chebi = client.resolve("CHEBI:53289", kind="resource")
        assert chebi.iri == "http://purl.obolibrary.org/obo/CHEBI_53289", "CURIEs are identifiers"
        assert chebi.candidates[0].origin == "registered identifier"
        other = client.resolve("https://identifiers.org/chebi/CHEBI:53289", kind="resource")
        assert other.iri == chebi.iri, "An IRI of a registered namespace is an identifier too"
        term = client.resolve(URIRef("https://identifiers.org/chebi/CHEBI:53289"), kind="resource")
        assert term.iri == chebi.iri, "An RDFLib IRI, as tables give, resolves like a string"
        missing = client.resolve("urn:e:missing", kind="resource")
        assert (missing.status, missing.candidates[0].use) == ("unresolved", "absent")
        assert client.resolve("Ada", kind="resource").iri == "urn:e:one"

        with (
            pytest.raises(ResolutionError) as failure,
            patch.object(Ontologies, "search", return_value=external),
        ):
            client.prepare_network(
                [{"reference": "Researcher", "bindings": ["r"]}],
                outputs=["r"],
                resolve=True,
                external_names=True,
            )
        assert failure.value.resolution.status == "ambiguous"
        query = client.prepare_network(
            [
                {"reference": "urn:e:External", "bindings": ["r"]},
                {"reference": "member of", "bindings": ["r", "org"]},
            ],
            outputs=["r", "org"],
            resolve=True,
        )
        assert [row["r"].value for row in client.select(query).rows] == ["urn:e:one"]
        assert any("urn:e:Person" in w for w in query.warnings), (
            "Say that a field from another class adds its owner type"
        )
        network = query.diagnostics["network"]
        assert network["roles"]["r"] == ["urn:e:External", "urn:e:Person"], "All type constraints"
        assert network["links"] == [
            {
                "from": "r",
                "to": "org",
                "label": "Affiliation",
                "name": "member of",
                "path": "<urn:e:affiliation>",
                "optional": False,
            }
        ]

        client.save_session(tmp_path / "session.json")

        def strict(value):
            raise ValueError(value)

        saved = json.loads((tmp_path / "session.json").read_text(), parse_constant=strict)
        assert [r["status"] for r in saved["resolutions"]][:2] == ["ambiguous", "resolved"]
        assert client._schema == schema, "Resolution never rewrites the source contract"


def test_many_identifiers_are_resolved_with_one_check_query():
    """resolve_many decides as resolve does, with the candidates of all names checked together
    (WP4726: 134 identifiers were 134 check queries)."""
    schema = MinedSchema.from_shacl(SHACL)
    graph = Dataset().parse(data=CLIENT_RESOLUTION_DATA, format="trig")
    names = ["CHEBI:53289", "https://identifiers.org/chebi/CHEBI:53289", "CHEBI:15377", "urn:e:org"]
    with Client(schema, graph, graph_uris=["urn:e:data"]) as client:
        one_by_one = {n: client.resolve(n).iri for n in names}
        before = client.trace()["source_queries"]
        many = client.resolve_many(names)
        sent = client.trace()["source_queries"] - before
    assert {n: r.iri for n, r in many.items()} == one_by_one
    assert one_by_one["CHEBI:15377"] is None and one_by_one["urn:e:org"] == "urn:e:org"
    assert sent == 2, sent  # subjects, then objects for the rest


def test_the_forms_of_a_namespace_are_learned_from_a_sample():
    """The source writes every ChEBI id as obo:CHEBI_n: learned from two ids, applied to the
    others, so ten ids take a few queries; an id the learned form misses is checked in full."""
    obo = "http://purl.obolibrary.org/obo/CHEBI_"
    data = "".join(f"<{obo}{n}> <urn:e:p> <urn:e:o> .\n" for n in range(15370, 15379))
    data += "<urn:e:x> <urn:e:p> <https://identifiers.org/chebi/CHEBI:16000> .\n"
    graph = Dataset().parse(data=f"<urn:e:data> {{ {data} }}", format="trig")
    names = [f"CHEBI:{n}" for n in range(15370, 15379)] + ["CHEBI:16000", "CHEBI:99999"]
    with Client(MinedSchema.from_shacl(SHACL), graph, graph_uris=["urn:e:data"]) as client:
        one_by_one = {n: client.resolve(n).iri for n in names}
        before = client.trace()["source_queries"]
        many = client.resolve_many(names, sample=2)
        sent = client.trace()["source_queries"] - before
    assert {n: r.iri for n, r in many.items()} == one_by_one
    assert one_by_one["CHEBI:16000"] == "https://identifiers.org/chebi/CHEBI:16000", (
        "A miss, checked in full"
    )
    assert one_by_one["CHEBI:99999"] is None
    assert sent <= 6, sent
    assert many["CHEBI:15378"].coverage["forms"] == [obo + "{id}"]


def test_a_batch_over_the_value_budget_is_split_and_retried():
    """Entries with hundreds of values (UniProt citations) load in smaller batches."""
    from types import SimpleNamespace

    from rdfsolve.client.api import Results
    from rdfsolve.client.hydration import HydrationLimitError

    calls = []

    def get_many(model, iris, fields):
        calls.append(len(iris))
        if len(iris) > 2:
            raise HydrationLimitError("Value budget exceeded")
        return [f"record {i}" for i in iris]

    fake = SimpleNamespace(client=SimpleNamespace(get_many=get_many))
    fake._get_many = lambda model, iris, fields: Results._get_many(fake, model, iris, fields)
    assert Results._get_many(fake, None, [str(i) for i in range(7)], set()) == [
        f"record {i}" for i in range(7)
    ]
    assert max(c for c in calls if c <= 2) == 2 and calls[0] == 7


def test_a_field_too_large_for_one_record_is_left_out_and_reported():
    """One entry whose citations alone exceed the budget loads without them; the set is partial."""
    from types import SimpleNamespace

    from rdfsolve.client.api import Results
    from rdfsolve.client.hydration import HydrationLimitError

    def get_many(model, iris, fields):
        if "citation" in fields:
            raise HydrationLimitError("Value budget exceeded")
        return [f"{iris[0]} with {', '.join(fields)}"]

    fake = SimpleNamespace(
        client=SimpleNamespace(get_many=get_many), coverage={"status": "complete"}
    )
    fake._get_many = lambda model, iris, fields: Results._get_many(fake, model, iris, fields)
    assert Results._get_many(fake, None, ["P1"], {"citation", "label"}) == ["P1 with label"]
    assert fake.coverage["status"] == "partial" and fake.coverage["left_out"] == {
        "P1": ["citation"]
    }


def test_open_saved_formats_and_mined_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "rdfsolve.mining.miner.SchemaMiner.mine", lambda *a, **k: pytest.fail("Do not mine")
    )
    with client() as original:
        schema = original._schema
        with schema.client(original.source) as data:
            assert isinstance(data, Client)
            assert not data.queries
        exports = [
            ("schema.json", None, json.dumps(schema.to_dict())),
            ("shapes.ttl", "shacl", schema.to_shacl()),
            ("void.ttl", "void", schema.to_void_graph().serialize(format="turtle")),
        ]
        for name, format, content in exports:
            path = tmp_path / name
            path.write_text(content)
            with Client.open(path, format=format, data_file=DATA) as data:
                assert not data.queries and (not data.graph_uris)
                records = data.find("Phenobarbital", kind=CHEMICAL)
                assert records[0].uri == "https://identifiers.org/cas/50-06-6"
        with Client.open(schema, original.source) as data:
            assert not data.queries
        with pytest.raises(ValueError, match="Turtle"):
            Client.open(tmp_path / "shapes.ttl")
        with pytest.raises(ValueError, match="not both"):
            Client.open(schema, original.source, data_file=DATA)
        with pytest.raises(ValueError, match="no classes"):
            Client.open(MinedSchema(about={}), original.source)
