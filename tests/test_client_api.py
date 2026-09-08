"""Exercise the researcher API on verified AOPWiki statements."""

from pathlib import Path

import pytest
from rdflib import Graph, Literal, RDF, URIRef

from rdfsolve import MinedSchema, SchemaPattern
from rdfsolve.client_api import Client

DATA = Path(__file__).parent / "test_data/aopwikirdf_phenobarbital_excerpt.ttl"
AOP = "http://aopkb.org/aop_ontology#AdverseOutcomePathway"
STRESSOR = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C54571"
CHEMICAL = "http://semanticscience.org/resource/CHEMINF_000000"


def client():
    graph = Graph().parse(DATA, format="turtle")
    patterns = {}
    for subject, predicate, obj in graph:
        if predicate == RDF.type:
            continue
        for cls in graph.objects(subject, RDF.type):
            targets = list(graph.objects(obj, RDF.type)) or ["Literal" if isinstance(obj, Literal) else "Resource"]
            for target in targets:
                pattern = SchemaPattern(subject_class=str(cls), property_uri=str(predicate),
                                        object_class=str(target), datatype=str(obj.datatype) if isinstance(obj, Literal) and obj.datatype else None)
                patterns[(str(cls), str(predicate), str(target))] = pattern
    return Client(MinedSchema(about={"dataset_name": "aopwikirdf"}, patterns=list(patterns.values())), graph, graph_uris=[])


def test_find_follow_values_and_saved_links(tmp_path):
    with client() as data:
        matches = data.find("Phenobarbital")
        chemicals = matches.of_type(CHEMICAL)
        assert len(chemicals) == 1
        assert len(data.find("50-06-6").of_type(CHEMICAL)) == 1
        stressors = chemicals.related(STRESSOR, incoming=True)
        pathways = stressors.related(AOP, incoming=True)
        assert len(stressors) == 1 and len(pathways) == 2
        titles = pathways.values("title")
        assert set(titles["Value"]) == {str(value) for record in pathways
            for value in data.source.objects(URIRef(record.uri),
                URIRef("http://purl.org/dc/elements/1.1/title"))}
        queries = len(data.queries)
        assert "title" in dir(pathways.fields)
        assert pathways.fields.title == "title"
        assert "matches" in repr(pathways)
        assert "<table" in pathways._repr_html_()
        assert not pathways.paths().empty
        assert len(data.queries) == queries
        file = tmp_path / "subset.ttl"
        data.save(file, chemicals, stressors, pathways)
        subset = Graph().parse(file)
        assert len(subset) and all(triple in data.source for triple in subset)
        assert len(pathways.without(pathways)) == 0


def test_errors_and_completion_do_not_trigger_hidden_queries():
    with client() as data:
        matches = data.find('absent" } #')
        assert len(matches) == 0
        assert matches.types().empty
        assert matches.values("title").empty
        queries = len(data.queries)
        with pytest.raises(ValueError, match="Choose a type"):
            matches.of_type("made up")
        assert len(data.queries) == queries
        pathways = data.find("thyroid", kind=AOP)
        with pytest.raises(ValueError, match="field"):
            pathways.show("misspelt")
        assert data.model("Adverse outcome pathway") is data.model(AOP)
