"""The edges to ontology terms are read only for the (class, property) pairs whose typed patterns
have owl:Class objects (Bgee run 10: a filter on every triple asked for 54 GB, and the source
failed after 7.6 h); on QLever the terms described with data properties are read from the set of
terms first. A refused term probe is recorded, the term
patterns are not probed, and the source is partial, not failed. RDFLib stands in for QLever here."""

from rdflib import Graph

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.sparql_helper import EndpointError
from tests.test_ontology_term_subsumption import FIXTURE


def mine(monkeypatch, engine, refuse=False):
    with SchemaMiner.from_graph(Graph().parse(data=FIXTURE, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        select, sent = miner.helper.select, []

        def answer(query, *args, purpose="", **kwargs):
            if purpose.startswith("ontology-terms/object"):
                sent.append(query)
                if refuse:
                    raise EndpointError("Tried to allocate 54 GB, but only 37.2 GB were available")
            return select(query, *args, purpose=purpose, **kwargs)

        monkeypatch.setattr(miner.helper, "select", answer)
        result = mine_with_ontology(miner, dataset_name="terms", ontology_as_data=True, ontology_term_budget=100)
        return result.data_schema, miner.last_report, sent


def terms(schema):
    return sorted(
        (p.subject_class, p.property_uri, p.object_class, p.datatype, p.count) for p in schema.term_patterns
    )


def test_term_edges_are_read_from_the_terms_first_on_qlever(monkeypatch):
    plain, _, _ = mine(monkeypatch, "virtuoso")
    fast, _, sent = mine(monkeypatch, "qlever")
    assert terms(fast) == terms(plain) == [
        ("urn:ex:Participant", "urn:ex:compound", "urn:term:formic", None, 1),
        ("urn:ex:Participant", "urn:ex:compound", "urn:term:propanol", None, 1),
        ("urn:term:ethanol", "urn:ex:smiles", "Literal", "http://www.w3.org/2001/XMLSchema#string", 1),
    ]
    assert len(sent) == 1 and "<urn:ex:compound>" in sent[0], "Only the typed pairs with term objects"


def test_a_refused_term_probe_leaves_the_source_partial(monkeypatch):
    schema, report, _ = mine(monkeypatch, "qlever", refuse=True)
    assert schema.term_patterns is None and schema.patterns, "Typed patterns are kept"
    assert report.completion_state == "partial"
    assert any(f.purpose == "ontology-terms" for f in report.query_failures)
