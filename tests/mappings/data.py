"""Shared inputs of the mappings tests: two small datasets (genes citing UniProt accessions, and
proteins), their schemas, and the join between them."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.signatures import Link
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment

UP = "http://purl.uniprot.org/uniprot/"
CHEBI = "http://purl.obolibrary.org/obo/CHEBI_"


def schema(name, patterns=(), examples=()):
    """A schema of *name* with these patterns and (class, property, subject, value) examples."""
    rows = [
        PatternExample(
            subject_class=c,
            property_uri=p,
            subject=RdfTerm(kind="uri", value=s),
            value=RdfTerm(kind="uri" if v.startswith("http") else "literal", value=v),
        )
        for c, p, s, v in examples
    ]
    found = list(patterns) or [
        SchemaPattern(subject_class=c, property_uri=p, object_class="Resource")
        for c, p, _, _ in examples
    ]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name=name),
        patterns=found,
        enrichment=SchemaEnrichment(examples=rows),
    )


GENES = schema(
    "genes",
    examples=[
        ("urn:Gene", "urn:xref", "urn:gene/1", UP + "P04637"),
        ("urn:Gene", "urn:ligand", "urn:gene/1", "CHEBI:15377"),
        ("urn:Gene", "urn:datatype", "urn:gene/1", "http://www.w3.org/2001/XMLSchema#string"),
    ],
)
PROTEINS = schema(
    "proteins",
    examples=[
        ("urn:Protein", "urn:name", UP + "P04637", "Cellular tumor antigen p53"),
        ("urn:Protein", "urn:binds", UP + "P04637", CHEBI + "15377"),
    ],
)
JOIN = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")

# Three genes, two of whose proteins are in the target.
GENE_DATA = f"""
<urn:gene/1> a <urn:Gene> ; <urn:xref> <{UP}P04637> .
<urn:gene/2> a <urn:Gene> ; <urn:xref> <{UP}P38398> .
<urn:gene/3> a <urn:Gene> ; <urn:xref> <{UP}P99999> .
"""
PROTEIN_DATA = f"<{UP}P04637> a <urn:Protein> . <{UP}P38398> a <urn:Protein> ."


def clients(source_data, target_data, source=GENES, target=PROTEINS):
    """Two clients on local RDF: the source and the target of a link."""
    return (
        Client(source, Dataset().parse(format="turtle", data=source_data)),
        Client(target, Dataset().parse(format="turtle", data=target_data)),
    )
