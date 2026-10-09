"""Shared inputs of the conversion tests: a small WikiPathways-shaped dataset, a Biolink model
excerpt, and clients over local statements."""

import pyoxigraph as ox
import rdflib

from rdfsolve.conversion import Profile, Query, Rule, rules_of
from rdfsolve.property_graph import Fold, Identity, PropertyGraph

WP = "http://vocabularies.wikipathways.org/wp#"
BL = "https://w3id.org/biolink/vocab/"
DATA = f"""@prefix wp: <{WP}> .
<urn:c1> a wp:Complex ; wp:participants <urn:p1>, <urn:p2>, <urn:m1> .
<urn:k1> a wp:Catalysis ; wp:source <urn:p1> ; wp:target <urn:r1> .
<urn:k2> a wp:Catalysis ; wp:source <urn:p2> ; wp:target <urn:r2> .
<urn:r1> a wp:Conversion ; wp:source <urn:m1> ; wp:target <urn:m2> .
<urn:r2> a wp:Conversion ; wp:source <urn:m2> ; wp:target <urn:m3> .
<urn:p1> a wp:Protein . <urn:p2> a wp:Protein .
<urn:m1> a wp:Metabolite . <urn:m2> a wp:Metabolite . <urn:m3> a wp:Metabolite ."""


def store():
    found = ox.Store()
    found.load(DATA.encode(), ox.RdfFormat.TURTLE)
    return found


def biolink_client():
    """A client whose session records the steps run on a workflow's own statements."""
    from rdfsolve import MinedSchema, SchemaPattern
    from rdfsolve.client.api import Client

    schema = MinedSchema(
        about={"dataset_name": "s"},
        patterns=[
            SchemaPattern(
                subject_class=WP + "Protein", property_uri=WP + "label", object_class="Literal"
            )
        ],
    )
    return Client(schema, rdflib.Graph(), graph_uris=[])


def pairs(rule):
    return {(t.subject.value, t.object.value) for t in store().query(rule.to_construct())}


class Local:
    """A client stand-in that runs CONSTRUCTs on the test store."""

    def construct(self, query):
        out = ox.Dataset()
        for t in store().query(query):
            out.add(ox.Quad(t.subject, t.predicate, t.object))
        return out


BIOLINK_YAML = """version: 9.9
classes:
  named thing: {}
  gene: {is_a: named thing, mixins: [gene or gene product], id_prefixes: [NCBIGene, ENSEMBL]}
  protein: {is_a: named thing, mixins: [gene product mixin], id_prefixes: [UniProtKB]}
  macromolecular machine mixin: {mixin: true}
  gene or gene product: {is_a: macromolecular machine mixin, mixin: true}
  gene product mixin: {is_a: gene or gene product, mixin: true}
  molecular activity: {is_a: named thing, id_prefixes: [RHEA]}
  association: {is_a: named thing}
  chemical entity to chemical derivation association: {is_a: association}
slots:
  catalyzes: {is_a: related to}
  has input: {is_a: has participant, domain: molecular activity, range: named thing}
"""
