"""The class shapes put no constraint on rdf:type. The schema keeps a row (C, rdf:type, owl:Class)
when a type value is declared a class, and leaves out the rows of type values without a type,
so a shape from those rows asked every type value to be an owl:Class (WikiPathways: 22,139
results of wp:Interaction instances that are also typed wp:Translocation, 2026-09-30)."""

from rdflib import RDF, SH, Graph

from rdfsolve.schema_models import AboutMetadata, MinedSchema, SchemaPattern

OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"


def test_rdf_type_has_no_property_shape():
    schema = MinedSchema(
        about=AboutMetadata.build(dataset_name="fixture"),
        patterns=[
            SchemaPattern(subject_class="urn:C", property_uri=str(RDF.type), object_class=OWL_CLASS),
            SchemaPattern(subject_class="urn:C", property_uri="urn:p", object_class="urn:D"),
        ],
    )
    shapes = Graph().parse(data=schema.to_shacl(activate_observed=True, void=False), format="turtle")
    paths = set(shapes.objects(None, SH.path))
    assert RDF.type not in paths and len(paths) == 1
