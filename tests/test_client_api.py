"""Exercise the researcher API on verified AOPWiki statements."""

from pathlib import Path

import pandas as pd
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
        data.source.query = lambda *args, **kwargs: pytest.fail("Log must not query")
        log = data.query_log()
        rendered = log._repr_html_()
        assert "Find Phenobarbital" in rendered
        assert "Phenobarbital" in rendered and "Query text" in rendered
        assert all(query["result_retained"] for query in log.queries)
        assert len(log.queries) == len(data.queries)


def test_class_routes_and_actual_connections():
    from rdfsolve.hydration import HydrationLimitError

    chemical = "https://identifiers.org/cas/50-06-6"
    pathway = "https://identifiers.org/aop/162"
    stressor = "https://identifiers.org/aop.stressor/133"
    with client() as data:
        classes = data.paths_between(CHEMICAL, AOP, max_hops=2)
        assert len(classes) == 2 and classes["Direction"].tolist() == ["←", "←"]
        assert not data.queries
        assert data.paths_between(CHEMICAL, AOP, max_hops=1).empty
        assert data.paths_between(CHEMICAL, AOP, max_hops=3, both_directions=False).empty
        result = data.connections(chemical, pathway, max_hops=2)
        assert result["From"].tolist() == [chemical, stressor]
        assert result["To"].tolist() == [stressor, pathway]
        assert result["Direction"].tolist() == ["←", "←"]
        assert data.connections(chemical, pathway, max_hops=2, both_directions=False).empty
        assert data.connections(chemical, "urn:absent", max_hops=2).empty
        # Every returned step must be an actual triple, including reverse steps.
        longer = data.connections(chemical, pathway, max_hops=3)
        for route in longer.attrs["routes"]:
            b = route["bindings"]
            nodes = [b[f"n{i}"]["value"] for i in range(route["hops"] + 1)]
            assert len(set(nodes)) == len(nodes)
            for i in range(route["hops"]):
                s, p, o = (URIRef(b[k]["value"]) for k in (f"n{i}", f"p{i}", f"n{i+1}"))
                assert ((o, p, s) if b[f"back{i}"]["value"] == "true" else (s, p, o)) in data.source
        assert all(q["result_retained"] for q in data.session_metadata()["queries"])
        with pytest.raises(HydrationLimitError):
            data.connections(pathway, "https://identifiers.org/aop/107", max_hops=2, max_paths=1)
        assert data.session_metadata()["steps"][-1]["status"] == "failed"
        queries = len(data.queries)
        for args in ({"max_hops": 0}, {"max_hops": 7}, {"max_paths": 0}):
            with pytest.raises(ValueError):
                data.connections(chemical, pathway, **args)
        with pytest.raises(ValueError):
            data.connections("urn:bad> } UNION { ?s ?p ?o", pathway)
        assert len(data.queries) == queries


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
        data.source.query = lambda *args, **kwargs: pytest.fail("Display must not query")
        assert not pathways.show("title").empty
        assert not pathways.values(pathways.fields.title).empty


def test_table_input_uses_field_definitions_without_querying(tmp_path):
    with client() as data:
        data.source.query = lambda *args, **kwargs: pytest.fail("Table input must not query")
        table = pd.DataFrame({"ID": ["https://identifiers.org/cas/50-06-6"], "Name": ["Phenobarbital"]})
        records = data.from_table(CHEMICAL, table, id_column="ID", title="Name")
        assert list(records.values("title")["Value"]) == ["Phenobarbital"]
        output = tmp_path / "chemical.ttl"
        data.save(output, records)
        assert all(triple in data.source for triple in Graph().parse(output))
        with pytest.raises(ValueError, match="Missing columns"):
            data.from_table(CHEMICAL, table, id_column="missing", title="Name")
        with pytest.raises(ValueError):
            data.from_table(CHEMICAL, table.assign(ID="not an IRI"), id_column="ID", title="Name")
