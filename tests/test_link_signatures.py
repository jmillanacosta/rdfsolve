"""Links between datasets are inferred from the identifier types of their schema examples."""

from rdfsolve.mappings.signatures import infer_links, signatures
from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment

UP, CHEBI = "http://purl.uniprot.org/uniprot/", "http://purl.obolibrary.org/obo/CHEBI_"


def schema(name, rows):
    """A schema with one example for each (class, property, subject, value) row."""
    examples = [
        PatternExample(
            subject_class=c,
            property_uri=p,
            subject=RdfTerm(kind="uri", value=s),
            value=RdfTerm(kind="uri" if v.startswith("http") else "literal", value=v),
        )
        for c, p, s, v in rows
    ]
    patterns = [
        SchemaPattern(subject_class=c, property_uri=p, object_class="Resource")
        for c, p, _, _ in rows
    ]
    return MinedSchema(
        about=AboutMetadata.build(dataset_name=name),
        patterns=patterns,
        enrichment=SchemaEnrichment(examples=examples),
    )


GENES = schema(
    "genes",
    [
        ("urn:Gene", "urn:xref", "urn:gene/1", UP + "P04637"),
        ("urn:Gene", "urn:ligand", "urn:gene/1", "CHEBI:15377"),
        ("urn:Gene", "urn:datatype", "urn:gene/1", "http://www.w3.org/2001/XMLSchema#string"),
    ],
)
PROTEINS = schema(
    "proteins",
    [
        ("urn:Protein", "urn:name", UP + "P04637", "Cellular tumor antigen p53"),
        ("urn:Protein", "urn:binds", UP + "P04637", CHEBI + "15377"),
    ],
)


def test_values_of_one_dataset_join_the_subjects_of_another():
    found = signatures(GENES)
    assert found.values == {
        "uniprot": {("urn:Gene", "urn:xref")},
        "chebi": {("urn:Gene", "urn:ligand")},
    }
    assert "xsd" not in found.values, "A vocabulary term is not an identifier"
    assert signatures(PROTEINS).subjects == {"uniprot": {"urn:Protein"}}
    links = infer_links({"genes": GENES, "proteins": PROTEINS})
    joins = {
        (link.source, link.source_class, link.property, link.target, link.target_class)
        for link in links
        if link.kind == "join"
    }
    assert joins == {("genes", "urn:Gene", "urn:xref", "proteins", "urn:Protein")}
    shared = {
        (link.source, link.target, link.identifier_type) for link in links if link.kind == "shared"
    }
    assert shared == {("genes", "proteins", "chebi")}, (
        "A CURIE literal and an OBO IRI name the same entity"
    )
