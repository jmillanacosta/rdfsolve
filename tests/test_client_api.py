from pathlib import Path

import pytest
from rdflib import RDF, Graph, Literal, URIRef
from rdfsolve.client.api import Client

from rdfsolve import MinedSchema, SchemaPattern

DATA = Path(__file__).parent / "test_data/aopwikirdf_phenobarbital_excerpt.ttl"
AOP = "http://aopkb.org/aop_ontology#AdverseOutcomePathway"
STRESSOR = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C54571"
CHEMICAL = "http://semanticscience.org/resource/CHEMINF_000000"


def client(data_file=DATA):
    graph = Graph().parse(data_file, format="turtle")
    patterns = {}
    for subject, predicate, obj in graph:
        if predicate == RDF.type:
            continue
        for cls in graph.objects(subject, RDF.type):
            targets = list(graph.objects(obj, RDF.type)) or [
                "Literal" if isinstance(obj, Literal) else "Resource"
            ]
            for target in targets:
                pattern = SchemaPattern(
                    subject_class=str(cls),
                    property_uri=str(predicate),
                    object_class=str(target),
                    datatype=str(obj.datatype)
                    if isinstance(obj, Literal) and obj.datatype
                    else None,
                )
                patterns[str(cls), str(predicate), str(target)] = pattern
    return Client(
        MinedSchema(about={"dataset_name": "aopwikirdf"}, patterns=list(patterns.values())),
        graph,
        graph_uris=[],
    )


def test_find_follow_values_and_saved_links(tmp_path):
    with client() as data:
        matches = data.find("Phenobarbital")
        chemicals = matches.of_type(CHEMICAL)
        assert len(chemicals) == 1
        assert len(data.find("50-06-6").of_type(CHEMICAL)) == 1
        stressors = chemicals.related(STRESSOR, incoming=True)
        pathways = stressors.related(AOP, incoming=True)
        assert len(stressors) == 1 and len(pathways) == 2
        through = pathways.related(kind=CHEMICAL, via=STRESSOR)
        assert {r.uri for r in through} == {r.uri for r in chemicals}
        named = pathways.related(value="phenobarbital", via=STRESSOR)
        assert {r.uri for r in named} == {r.uri for r in chemicals}
        assert named._table()["Class"].notna().all()
        assert "Class" in named.show("title")
        assert not pathways.related(value="Phenobarbitol", via=STRESSOR)
        assert not pathways.related(value='"} UNION { ?s ?p ?o } #', via=STRESSOR)
        titles = pathways.values("title")
        assert set(titles["Value"]) == {
            str(value)
            for record in pathways
            for value in data.source.objects(
                URIRef(record.uri), URIRef("http://purl.org/dc/elements/1.1/title")
            )
        }
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
        assert len(subset) and all((triple in data.source for triple in subset))
        assert len(pathways.without(pathways)) == 0
        data.source.query = lambda *args, **kwargs: pytest.fail("Log must not query")
        log = data.query_log()
        rendered = log._repr_html_()
        assert "Find Phenobarbital" in rendered
        assert "Phenobarbital" in rendered and "Query text" in rendered
        assert all((query["result_retained"] for query in log.queries))
        assert len(log.queries) == len(data.queries)
