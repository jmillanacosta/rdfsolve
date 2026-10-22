"""rdfsolve.property_graph types: node types are the most specific stated classes, a name the issuer
does not give is a finding with the query for all such, and a rule says which source release and
target version it is for."""

import csv
import json
from datetime import date

import networkx as nx
import pyoxigraph as ox
import pytest

from rdfsolve.property_graph import Conversion, Fold, Identity, PropertyGraph, suggest_folds
from rdfsolve.schema_models import MinedSchema, SchemaPattern
from tests.property_graph.data import DATA, IDS, MAPPINGS, PREFIXES, RDF_TYPE, WPV, XSD, E, _pathway


def test_node_types_are_the_most_specific_stated_classes():
    """A protein stated with or without its superclasses is one node type (wp:Protein); the
    superclasses go to the type property, so the round trip holds."""
    wp = "http://vocabularies.wikipathways.org/wp#"
    data = ox.Dataset(
        ox.parse(
            f"""@prefix wp: <{wp}> .
    <urn:apob> a wp:DataNode, wp:Protein .
    <urn:ldlr> a wp:DataNode, wp:GeneProduct, wp:Protein .
    <urn:hmgcr> a wp:DataNode, wp:GeneProduct .
    <urn:c> a wp:Complex, wp:DataNode .""".encode(),
            ox.RdfFormat.TURTLE,
        )
    )
    hierarchy = {
        wp + "Protein": {wp + "GeneProduct", wp + "DataNode"},
        wp + "GeneProduct": {wp + "DataNode"},
        wp + "Complex": {wp + "DataNode"},
    }
    pg = PropertyGraph.from_rdf(data, hierarchy=hierarchy)
    report = pg.report()
    assert report["node_types"] == {"Protein": 2, "GeneProduct": 1, "Complex": 1}
    assert report["lossless"]["passed"]
    assert report["most_specific_labels"]["moved_to_type"] == {
        wp + "DataNode": 4,
        wp + "GeneProduct": 1,
    }


def test_a_name_the_issuer_does_not_give_is_a_finding_with_the_query_for_all_such():
    """WikiPathways draws an esterase as a metabolite with a ChEBI id (arecoline hydrobromide):
    the names disagree, so it is a finding, issued as a warning, with the SPARQL and SHACL that
    find every metabolite drawn as the source of a catalysis. Water drawn as "2 H₂O" agrees with
    ChEBI's synonym H2O and is not a finding."""
    import warnings

    from rdfsolve.property_graph import UpstreamWarning

    wp = "http://vocabularies.wikipathways.org/wp#"
    chebi, ido = "http://purl.obolibrary.org/obo/CHEBI_", "https://identifiers.org/chebi/CHEBI:"
    syn = "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"
    data = ox.Dataset(
        ox.parse(
            f"""@prefix wp: <{wp}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    <{ido}233150> a wp:Metabolite ; rdfs:label "Esterase" .
    <{chebi}233150> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "arecoline hydrobromide" .
    <{ido}15377> a wp:Metabolite ; rdfs:label "2 H₂O" .
    <{chebi}15377> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "water" ; <{syn}> "H2O" .
    <urn:k1> a wp:Catalysis ; wp:source <{ido}233150> ; wp:target <urn:r1> .
    <urn:r1> a wp:Conversion ; wp:source <{ido}15377> .""".encode(),
            ox.RdfFormat.TURTLE,
        )
    )
    kinds = {"chebi": ["http://www.w3.org/2002/07/owl#Class"], "wikipathways": [wp + "Metabolite"]}
    pg = PropertyGraph.from_rdf(
        data,
        identity=Identity(kinds=kinds),
        folds=[Fold(wp + "Catalysis", wp + "source", wp + "target")],
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        found = pg.findings()
    assert [f.stated for f in found] == [["Esterase"]] and found[0].issuer == [
        "arecoline hydrobromide"
    ]
    assert any(issubclass(w.category, UpstreamWarning) for w in caught)
    assert f"<{wp}Catalysis>" in found[0].query and f"<{wp}Metabolite>" in found[0].query
    assert "sh:not [ sh:class" in found[0].shape
    store = ox.Store()
    for quad in data:
        store.add(quad)
    assert [s["node"].value for s in store.query(found[0].query)] == [ido + "233150"]
    # With the source's counts: a place it often uses for that class gets no query (not rare).
    pg.patterns[(wp + "Catalysis", wp + "source", wp + "Metabolite")] = 50
    pg.patterns[(wp + "Catalysis", wp + "source", wp + "GeneProduct")] = 50
    assert pg.findings(warn=False)[0].query is None
    pg.patterns[(wp + "Catalysis", wp + "source", wp + "GeneProduct")] = 3484
    assert "rare in wikipathways: 50 of 3534" in pg.findings(warn=False)[0].pattern


def test_a_rule_says_which_source_release_and_target_version_it_is_for():
    """The SHACL of a fold carries its provenance as comments and as triples, and still parses."""
    from types import SimpleNamespace

    import rdflib

    from rdfsolve.property_graph import provenance

    about = SimpleNamespace(
        title="WikiPathways",
        dataset_name="wikipathways",
        source_version="20260610",
        source_issued=None,
        source_modified=None,
        retrieved_at=None,
        generated_at=None,
        endpoint="http://localhost:7101",
        void_uri="https://w3id.org/rdfsolve/void/wikipathways",
        generated_by="rdfsolve 0.1.0",
    )
    info = provenance(SimpleNamespace(about=about), target="Biolink Model", target_version="4.4.5")
    wp = "http://vocabularies.wikipathways.org/wp#"
    text = Fold(
        wp + "Catalysis",
        wp + "source",
        wp + "target",
        "CATALYSES",
        predicate="https://w3id.org/biolink/vocab/catalyzes",
    ).to_shacl(info)
    assert "# source release: 20260610" in text and "# generated with: rdfsolve" in text
    graph = rdflib.Graph().parse(data=text, format="turtle")
    assert (
        None,
        rdflib.URIRef("http://www.w3.org/ns/prov#wasDerivedFrom"),
        rdflib.URIRef(about.void_uri),
    ) in graph
    assert (None, rdflib.URIRef("http://purl.org/pav/version"), rdflib.Literal("4.4.5")) in graph
    assert "https://w3id.org/biolink/vocab/catalyzes" in text
    unknown = provenance(
        SimpleNamespace(
            about=SimpleNamespace(
                **{**vars(about), "source_version": None, "retrieved_at": "2026-09-30"}
            )
        )
    )
    assert unknown["source release"].startswith("not stated by the source")
