"""A small AOP-like source for the tests of the MCP tools."""

import json

import pytest
from rdflib import RDF, RDFS, XSD, Graph, Literal, Namespace
from rdfsolve.client.api import Client
from rdfsolve.mcp.tools import Toolbox
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import (
    PatternExample,
    RdfTerm,
    SchemaEnrichment,
    TermAnnotation,
)
from rdfsolve.schema_models.pattern import SchemaPattern

E = Namespace("https://mcp-test.invalid/")
TRIPLES = [
    (E.aop1, RDF.type, E.Pathway),
    (E.aop2, RDF.type, E.Pathway),
    (E.aop1, RDFS.label, Literal("Thyroid pathway")),
    (E.aop2, RDFS.label, Literal("Liver pathway")),
    (E.ke1, RDF.type, E.Event),
    (E.ke2, RDF.type, E.Event),
    (E.ke1, RDFS.label, Literal("Decreased thyroxine")),
    (E.ke2, RDFS.label, Literal("Liver fibrosis")),
    (E.aop1, E.event, E.ke1),
    (E.aop2, E.event, E.ke2),
    (E.ke1, E.gene, E.tpo),
    (E.tpo, RDF.type, E.Gene),
    (E.tpo, E.symbol, Literal("TPO")),
    (E.chem1, RDF.type, E.Chemical),
    (E.chem1, RDFS.label, Literal("Propylthiouracil")),
    (E.chem1, E.cas, Literal("51-52-5", datatype=XSD.string)),
    (E.aop1, E.stressor, E.chem1),
]
PATTERNS = [
    (E.Pathway, E.event, E.Event, 2),
    (E.Pathway, E.stressor, E.Chemical, 1),
    (E.Pathway, RDFS.label, "Literal", 2),
    (E.Event, RDFS.label, "Literal", 2),
    (E.Event, E.gene, E.Gene, 1),
    (E.Gene, E.symbol, "Literal", 1),
    (E.Chemical, RDFS.label, "Literal", 1),
    (E.Chemical, E.cas, "Literal", 1),
]
LABELS = {E.Pathway: "Adverse Outcome Pathway", E.Event: "Key Event", E.cas: "CAS number"}


def make_schema():
    patterns = [
        SchemaPattern(
            subject_class=str(s),
            property_uri=str(p),
            object_class=str(o),
            count=n,
            datatype=str(XSD.string) if o == "Literal" else None,
        )
        for s, p, o, n in PATTERNS
    ]
    labels = [
        TermAnnotation(term_iri=str(i), predicate=str(RDFS.label), text=RdfTerm(kind="literal", value=v))
        for i, v in LABELS.items()
    ]
    example = PatternExample(
        subject_class=str(E.Chemical),
        property_uri=str(E.cas),
        subject=RdfTerm(kind="uri", value=str(E.chem1)),
        value=RdfTerm(kind="literal", value="51-52-5"),
    )
    return MinedSchema(
        about={
            "dataset_name": "mcp-test",
            "class_entity_counts": {str(E.Pathway): 2, str(E.Event): 2, str(E.Gene): 1, str(E.Chemical): 1},
        },
        patterns=patterns,
        prefixes={"ex": str(E), "rdfs": str(RDFS)},
        enrichment=SchemaEnrichment(labels=labels, examples=[example]),
    )


def make_graph():
    graph = Graph()
    for triple in TRIPLES:
        graph.add(triple)
    return graph


@pytest.fixture
def toolbox(tmp_path):
    with Client(make_schema(), make_graph(), graph_uris=[]) as client:
        yield Toolbox(client, output_variables=["aop", "label"], artifact_dir=tmp_path / "results")


@pytest.fixture
def files(tmp_path):
    schema, data = tmp_path / "schema.json", tmp_path / "data.ttl"
    schema.write_text(json.dumps(make_schema().to_dict()))
    make_graph().serialize(destination=data, format="turtle")
    return schema, data
