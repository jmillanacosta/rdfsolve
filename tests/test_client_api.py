from pathlib import Path

import pytest
from rdflib import RDF, Graph, Literal, URIRef

from rdfsolve import MinedSchema, SchemaPattern
from rdfsolve.client.api import Client

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
        assert len(subset) and all(triple in data.source for triple in subset)
        assert len(pathways.without(pathways)) == 0
        data.source.query = lambda *args, **kwargs: pytest.fail("Log must not query")
        log = data.query_log()
        rendered = log._repr_html_()
        assert "Find Phenobarbital" in rendered
        assert "Phenobarbital" in rendered and "Query text" in rendered
        assert all(query["result_retained"] for query in log.queries)
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
            SchemaPattern(
                subject_class=rh + "Reaction",
                property_uri=rh + "side",
                object_class=rh + "ReactionSide",
            ),
            SchemaPattern(
                subject_class=rh + "Reaction", property_uri=rh + "equation", object_class="Literal"
            ),
        ],
    )
    with Client(schema, graph, graph_uris=[]) as found:
        import pandas as pd

        reaction = found.from_table(
            rh + "Reaction", pd.DataFrame({"iri": [rh + "r1"]}), id_column="iri"
        )
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
            SchemaPattern(
                subject_class=up + "Protein",
                property_uri=up + "annotation",
                object_class=up + "Catalytic_Activity_Annotation",
            ),
            SchemaPattern(
                subject_class=up + "Annotation", property_uri=str(RDF.type), object_class="Resource"
            ),
        ],
    )
    with Client(schema, graph, graph_uris=[]) as found:
        import pandas as pd

        protein = found.from_table(
            up + "Protein", pd.DataFrame({"iri": [up + "p1"]}), id_column="iri"
        )
        annotations = protein.related(up + "Catalytic_Activity_Annotation", via="annotation")
        assert [r.uri for r in annotations] == [up + "a1"]


X = "urn:x:"
PARTS = f"""@prefix x: <{X}> .
x:P1 a x:Pathway . x:P2 a x:Pathway . x:P3 a x:Pathway .
x:n1 a x:Pathway ; x:isPartOf x:P1 ; x:hasVersion x:P2 .
x:n2 a x:Pathway ; x:isPartOf x:P2 ; x:hasVersion x:P3 .
x:n3 a x:Pathway ; x:isPartOf x:P3 ; x:hasVersion x:P1 .
x:g1 a x:Gene ; x:isPartOf x:P1 . x:m1 a x:Metabolite ; x:isPartOf x:P1 ."""


def test_related_follows_a_path_to_a_depth_and_gives_its_links(tmp_path):
    """A pathway drawn in a pathway (a node, isPartOf) stands for a pathway (hasVersion): the path
    is followed level by level; links() gives the pairs of one link (node -> pathway)."""
    import pandas as pd

    (tmp_path / "parts.ttl").write_text(PARTS)
    with client(tmp_path / "parts.ttl") as found:
        start = found.from_table(X + "Pathway", pd.DataFrame({"iri": [X + "P1"]}), id_column="iri")
        two = start.related(X + "Pathway", via=["^isPartOf", "hasVersion"], depth=2)
        assert sorted(r.uri for r in two) == [X + "P2", X + "P3"]
        three = start.related(X + "Pathway", via=["^isPartOf", "hasVersion"], depth=3)
        assert three.links(via="hasVersion") == {
            X + "n1": {X + "P2"},
            X + "n2": {X + "P3"},
            X + "n3": {X + "P1"},
        }


def test_related_without_a_kind_gives_every_type_a_link_reaches(tmp_path):
    """What a pathway holds: every record type that is part of it, with its count (types())."""
    import pandas as pd

    (tmp_path / "parts.ttl").write_text(PARTS)
    with client(tmp_path / "parts.ttl") as found:
        start = found.from_table(X + "Pathway", pd.DataFrame({"iri": [X + "P1"]}), id_column="iri")
        parts = start.related(via="isPartOf", incoming=True).types()
        assert dict(zip(parts["Class"], parts["Matches"], strict=True)) == {
            "Gene": 1,
            "Metabolite": 1,
            "Pathway": 1,
        }


def test_naming_finds_the_records_that_cite_an_identifier_in_any_form(tmp_path):
    """Proteins whose cross-reference names a gene, written in another registered form than the
    identifier given; links() gives each identifier's records."""
    (tmp_path / "xrefs.ttl").write_text(
        f"""@prefix x: <{X}> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        x:p1 a x:Protein ; rdfs:seeAlso <http://identifiers.org/ncbigene/3156> .
        x:p2 a x:Protein ; rdfs:seeAlso <http://bio2rdf.org/ncbigene:9999> .
        x:p3 a x:Protein ; rdfs:seeAlso <http://identifiers.org/ncbigene/1> ."""
    )
    with client(tmp_path / "xrefs.ttl") as found:
        named = found.naming(
            ["ncbigene:3156", "https://identifiers.org/ncbigene/9999"],
            kind=X + "Protein",
            via="see also",
        )
        assert named.links() == {
            "ncbigene:3156": {X + "p1"},
            "https://identifiers.org/ncbigene/9999": {X + "p2"},
        }


def test_a_registry_entry_opens_with_its_downloads(tmp_path, monkeypatch):
    """data_file names a registry entry: its RDF downloads are fetched once and read together."""
    import rdfsolve.sources

    (tmp_path / "dump.ttl").write_text(f'<{X}P1> a <{X}Pathway> ; <{X}title> "One" .')
    (tmp_path / "notes.txt").write_text("not RDF")
    (tmp_path / "sources.yaml").write_text(
        f"- name: demo\n  download_ttl:\n  - {(tmp_path / 'dump.ttl').as_uri()}\n"
        f"  download_txt: {(tmp_path / 'notes.txt').as_uri()}\n"
    )
    monkeypatch.setattr(rdfsolve.sources, "DEFAULT_SOURCES_YAML", tmp_path / "sources.yaml")
    monkeypatch.setenv("RDFSOLVE_DOWNLOADS", str(tmp_path / "cache"))
    schema = MinedSchema(
        about={"dataset_name": "demo"},
        patterns=[
            SchemaPattern(
                subject_class=X + "Pathway", property_uri=X + "title", object_class="Literal"
            )
        ],
    )
    with Client.open(schema=schema, data_file="demo") as found:
        import pandas as pd

        one = found.from_table(X + "Pathway", pd.DataFrame({"iri": [X + "P1"]}), id_column="iri")
        assert [vars(r)["title"] for r in one.load("title")] == [["One"]]
    assert sorted(p.name for p in (tmp_path / "cache" / "demo").iterdir()) == ["dump.ttl"]
