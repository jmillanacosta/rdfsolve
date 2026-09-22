from pathlib import Path

from rdflib import Graph, URIRef

from rdfsolve import MinedSchema

ROOT = "https://aopwiki.rdf.bigcat-bioinformatics.org/AOPWikiRDF"
DATASET = "http://rdfs.org/ns/void#Dataset"
DESCRIPTION = "http://purl.org/dc/elements/1.1/description"
DATA = Path(__file__).parent / "test_data/aopwikirdf_metadata_excerpt.ttl"


def schema():
    return MinedSchema.from_shacl(
        '\n        @prefix sh: <http://www.w3.org/ns/shacl#> .\n        @prefix void: <http://rdfs.org/ns/void#> .\n        @prefix dc: <http://purl.org/dc/elements/1.1/> .\n        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n        <urn:dataset> sh:targetClass void:Dataset; sh:name "Dataset";\n          sh:property [sh:path dc:description; sh:datatype xsd:string],\n                      [sh:path void:subset; sh:class void:Dataset],\n                      [sh:path (<http://rdfs.org/ns/void#subset> dc:description);\n                       sh:name "Subset descriptions"].\n    '
    )


def test_generated_classes_hydrate_paths_and_preserve_terms():
    graph = Graph().parse(DATA, format="turtle")
    with schema().hydrator(graph) as client:
        model = client.model(DATASET)
        obj = client.get(model, ROOT)
        assert isinstance(obj, model)
        assert obj.description == [str(graph.value(URIRef(ROOT), URIRef(DESCRIPTION)))]
        expected = {
            str(value)
            for subset in graph.objects(URIRef(ROOT), URIRef("http://rdfs.org/ns/void#subset"))
            for value in graph.objects(subset, URIRef(DESCRIPTION))
        }
        assert expected and set(obj.subset_descriptions) == expected
        assert obj.rdf_terms["description"][0]["kind"] == "literal"
        assert DATASET in obj.rdf_type
        assert len(client.queries) == 1
        assert obj.model_dump(mode="json")["rdf_source"]["endpoint"] is None
        selected = client.get(model, ROOT, fields=["description"])
        assert selected.subset is None
        assert selected.rdf_loaded_fields == ["description"]
        extended = client.with_paths(model, texts=["http://rdfs.org/ns/void#subset", DESCRIPTION])
        assert set(client.get(extended, ROOT, fields=["texts"]).texts) == expected
        assert "texts" not in model.model_fields
