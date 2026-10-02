"""Resolve names into class or resource constraints without hiding alternatives."""

import json
from unittest.mock import patch

import pytest
from rdflib import Dataset, URIRef

from rdfsolve.client.api import Client
from rdfsolve.client.resolution import ResolutionError
from rdfsolve.ontology import Ontologies
from rdfsolve.schema_models import MinedSchema

SHACL = """
@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix e: <urn:e:> .
e:Person <http://www.w3.org/2000/01/rdf-schema#label> "人物" .
e:P a sh:NodeShape; sh:targetClass e:Person; sh:name "人物";
  sh:property [sh:path e:affiliation; sh:name "member of"; sh:nodeKind sh:IRI],
              [sh:path e:name; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
e:O a sh:NodeShape; sh:targetClass e:Organisation;
  sh:property [sh:path e:title; sh:name "label"; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .
"""
DATA = """
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
    graph = Dataset().parse(data=DATA, format="trig")
    external = [{"iri": "urn:e:External", "label": "Researcher"},
                {"iri": "urn:e:OtherExternal", "label": "Researcher"}]
    with Client(schema, graph, graph_uris=["urn:e:data"]) as client:
        with patch.object(Ontologies, "search", return_value=external):
            named = client.resolve("Researcher", kind="class", external_names=True)
        assert named.status == "ambiguous", "A schema, source or external name has no precedence"
        origins = {c.iri: (c.origin, c.use) for c in named.candidates}
        assert origins == {"urn:e:Local": ("source label", "used as class"),
                           "urn:e:External": ("external ontology", "used as class"),
                           "urn:e:OtherExternal": ("external ontology", "not used as class")}
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

        with pytest.raises(ResolutionError) as failure, patch.object(
            Ontologies, "search", return_value=external
        ):
            client.prepare_network([{"reference": "Researcher", "bindings": ["r"]}],
                                   outputs=["r"], resolve=True, external_names=True)
        assert failure.value.resolution.status == "ambiguous"
        query = client.prepare_network([
            {"reference": "urn:e:External", "bindings": ["r"]},
            {"reference": "member of", "bindings": ["r", "org"]},
        ], outputs=["r", "org"], resolve=True)
        assert [row["r"].value for row in client.select(query).rows] == ["urn:e:one"]
        assert any("urn:e:Person" in w for w in query.warnings), (
            "Say that a field from another class adds its owner type")
        network = query.diagnostics["network"]
        assert network["roles"]["r"] == ["urn:e:External", "urn:e:Person"], "All type constraints"
        assert network["links"] == [{"from": "r", "to": "org", "label": "Affiliation", "name": "member of",
                                     "path": "<urn:e:affiliation>", "optional": False}]

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
    graph = Dataset().parse(data=DATA, format="trig")
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
    assert one_by_one["CHEBI:16000"] == "https://identifiers.org/chebi/CHEBI:16000", "A miss, checked in full"
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
    assert Results._get_many(fake, None, [str(i) for i in range(7)], set()) == [f"record {i}" for i in range(7)]
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

    fake = SimpleNamespace(client=SimpleNamespace(get_many=get_many), coverage={"status": "complete"})
    fake._get_many = lambda model, iris, fields: Results._get_many(fake, model, iris, fields)
    assert Results._get_many(fake, None, ["P1"], {"citation", "label"}) == ["P1 with label"]
    assert fake.coverage["status"] == "partial" and fake.coverage["left_out"] == {"P1": ["citation"]}
