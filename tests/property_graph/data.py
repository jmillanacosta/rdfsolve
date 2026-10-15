"""Shared inputs of the property graph tests: a small RDF graph with prefixes, identifiers and
mappings, and a pathway with complexes."""

import csv
import json
from datetime import date

import networkx as nx
import pyoxigraph as ox
import pytest

from rdfsolve.property_graph import Conversion, Fold, Identity, PropertyGraph, suggest_folds
from rdfsolve.schema_models import MinedSchema, SchemaPattern

DATA = """
@prefix e: <https://pg-test.invalid/> . @prefix o: <https://other-test.invalid/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
e:asah1 a e:Protein, e:GeneProduct ; rdfs:label "ASAH1", "acid ceramidase"@en ; e:name "ASAH1" ;
    o:name "Acid ceramidase" ; e:mass "44.6"^^xsd:decimal ; e:length 395 ;
    e:seen "2026-09-03"^^xsd:date ; e:score "12a"^^xsd:integer ; e:code "x"^^xsd:string .
e:cer a e:Metabolite ; rdfs:label "ceramide" ; e:shape [ e:rings 0 ] .
e:sph a e:Metabolite ; rdfs:label "sphingosine" .
e:c1 a e:Catalysis ; e:source e:asah1 ; e:target e:cer ; e:partOf e:wp ; e:note "acid" .
e:c2 a e:Catalysis ; e:source e:asah1 ; e:target e:sph .
e:about e:refersTo e:c2 .
"""
E = "https://pg-test.invalid/"
PREFIXES = {"e": E, "o": "https://other-test.invalid/"}


XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def graph():
    """Return the test data as an Oxigraph dataset."""
    return ox.Dataset(ox.parse(DATA.encode(), ox.RdfFormat.TURTLE))


IDS = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix up: <http://purl.uniprot.org/core/> . @prefix wp: <http://vocabularies.wikipathways.org/wp#> .
<https://identifiers.org/ensembl/ENSG00000141510> a wp:Protein ; rdfs:label "TP53" ;
    wp:bdbUniprot <https://identifiers.org/uniprot/P04637>, <https://identifiers.org/uniprot/A0A087X1C5> .
<http://purl.uniprot.org/uniprot/P04637> a up:Protein ; up:mnemonic "P53_HUMAN" ;
    skos:exactMatch <http://purl.obolibrary.org/obo/PR_P04637> .
<https://identifiers.org/chebi/CHEBI:15377> skos:exactMatch <http://purl.obolibrary.org/obo/CHEBI_15377> .
<http://purl.obolibrary.org/obo/CHEBI_15377> a <http://www.w3.org/2002/07/owl#Class> ; rdfs:label "water" .
<https://identifiers.org/cas/7732-18-5> a wp:Metabolite ; rdfs:label "H2O" ;
    wp:bdbChEBI <http://purl.obolibrary.org/obo/CHEBI_15377> .
<https://identifiers.org/chebi/CHEBI:30616> a wp:Metabolite ; rdfs:label "ATP" ;
    wp:bdbChEBI <https://identifiers.org/chebi/CHEBI:30616>, <https://identifiers.org/chebi/CHEBI:15422> .
"""
WPV = "http://vocabularies.wikipathways.org/wp#"
MAPPINGS = [WPV + "bdbUniprot", WPV + "bdbChEBI", "http://www.w3.org/2004/02/skos/core#exactMatch"]


def _pathway():
    """A WikiPathways-shaped graph: two complexes (proteins and a metabolite) and two catalyses."""
    wp = "http://vocabularies.wikipathways.org/wp#"
    turtle = f"""@prefix wp: <{wp}> .
    <urn:c1> a wp:Complex ; wp:participants <urn:p1>, <urn:p2>, <urn:p3>, <urn:m1> .
    <urn:c2> a wp:Complex ; wp:participants <urn:p2>, <urn:p4> .
    <urn:k1> a wp:Catalysis ; wp:source <urn:p1> ; wp:target <urn:r1> .
    <urn:k2> a wp:Catalysis ; wp:source <urn:p4> ; wp:target <urn:r1> .
    <urn:r1> a wp:Conversion ; wp:source <urn:m1> ; wp:target <urn:m2> .
    <urn:p1> a wp:Protein . <urn:p2> a wp:Protein . <urn:p3> a wp:Protein . <urn:p4> a wp:Protein .
    <urn:m1> a wp:Metabolite . <urn:m2> a wp:Metabolite ."""
    return wp, ox.Dataset(ox.parse(turtle.encode(), ox.RdfFormat.TURTLE))
