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


def test_paths_show_a_shared_type_name_as_a_curie(tmp_path):
    """Two classes with one name (a drawing's node and a biological node): paths() shows
    each as a CURIE, and the name it shows finds the records again."""
    data = tmp_path / "two.ttl"
    data.write_text(
        """@prefix bio: <https://bio.example.org/> . @prefix draw: <https://draw.example.org/> .
        @prefix dcterms: <http://purl.org/dc/terms/> .
        <urn:pathway> a bio:Pathway .
        <urn:gene> a bio:Node ; dcterms:isPartOf <urn:pathway> .
        <urn:box> a draw:Node ; dcterms:isPartOf <urn:pathway> ."""
    )
    with client(data) as found:
        found.schema.prefixes = {
            "bio": "https://bio.example.org/",
            "draw": "https://draw.example.org/",
        }
        pathway = found.fetch(["urn:pathway"], kind="https://bio.example.org/Pathway")
        assert len(pathway) == 1
        shown = set(pathway.paths(incoming=True)["To"])
        assert {"bio:Node", "draw:Node"} <= shown
        assert [r.uri for r in pathway.related("draw:Node", incoming=True)] == ["urn:box"]


def test_a_schema_with_a_membership_property_finds_records_by_it(tmp_path):
    """A source whose records are classes (Rhea: a reaction rdfs:subClassOf rh:Reaction): with the
    schema's membership property, the client finds the records and follows their links."""
    from rdflib import RDFS

    rh = "urn:rh:"
    graph = Graph().parse(
        data=f"""@prefix rh: <{rh}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        rh:r1 rdfs:subClassOf rh:Reaction ; rh:side rh:r1_L ; rh:equation "A = B" .
        rh:r1_L rdfs:subClassOf rh:ReactionSide .""",
        format="turtle",
    )
    schema = MinedSchema(
        about={"dataset_name": "rhea-like", "membership_property": str(RDFS.subClassOf)},
        patterns=[
            SchemaPattern(subject_class=rh + "Reaction", property_uri=rh + "side", object_class=rh + "ReactionSide"),
            SchemaPattern(subject_class=rh + "Reaction", property_uri=rh + "equation", object_class="Literal"),
        ],
    )
    with Client(schema, graph, graph_uris=[]) as found:
        import pandas as pd

        reaction = found.from_table(rh + "Reaction", pd.DataFrame({"iri": [rh + "r1"]}), id_column="iri")
        assert [vars(r)["equation"] for r in reaction.load("equation")] == [["A = B"]]
        assert [r.uri for r in reaction.related(rh + "ReactionSide", via="side")] == [rh + "r1_L"]


def test_related_follows_a_link_that_shares_its_name_with_a_class():
    """UniProt's annotation link and its up:Annotation class have one name: via= follows the
    link of these records (one hop), not a path through the class."""
    from rdflib import RDF

    up = "urn:up:"
    graph = Graph().parse(
        data=f"""@prefix up: <{up}> .
        up:p1 a up:Protein ; up:annotation up:a1 .
        up:a1 a up:Catalytic_Activity_Annotation .
        up:x a up:Annotation .""",
        format="turtle",
    )
    schema = MinedSchema(
        about={"dataset_name": "uniprot-like"},
        patterns=[
            SchemaPattern(subject_class=up + "Protein", property_uri=up + "annotation", object_class=up + "Catalytic_Activity_Annotation"),
            SchemaPattern(subject_class=up + "Annotation", property_uri=str(RDF.type), object_class="Resource"),
        ],
    )
    with Client(schema, graph, graph_uris=[]) as found:
        import pandas as pd

        protein = found.from_table(up + "Protein", pd.DataFrame({"iri": [up + "p1"]}), id_column="iri")
        annotations = protein.related(up + "Catalytic_Activity_Annotation", via="annotation")
        assert [r.uri for r in annotations] == [up + "a1"]
