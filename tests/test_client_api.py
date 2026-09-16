"""Exercise the researcher API on verified AOPWiki statements."""

from pathlib import Path

import pandas as pd
import pytest
from rdflib import RDF, Dataset, Graph, Literal, URIRef

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
                patterns[(str(cls), str(predicate), str(target))] = pattern
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


def test_class_routes_and_actual_connections():
    from rdfsolve.client.hydration import HydrationLimitError

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
        assert all(result["From class"] != "No type returned")
        assert all(result["To class"] != "No type returned")
        assert data.connections(chemical, pathway, max_hops=2, both_directions=False).empty
        assert data.connections(chemical, "urn:absent", max_hops=2).empty
        # Every returned step must be an actual triple, including reverse steps.
        longer = data.connections(chemical, pathway, max_hops=3)
        for route in longer.attrs["routes"]:
            b = route["bindings"]
            nodes = [b[f"n{i}"]["value"] for i in range(route["hops"] + 1)]
            assert len(set(nodes)) == len(nodes)
            for i in range(route["hops"]):
                s, p, o = (URIRef(b[k]["value"]) for k in (f"n{i}", f"p{i}", f"n{i + 1}"))
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


def test_paths_to_value_keep_real_links_and_draw_only_selected_paths():
    from rdfsolve.client.hydration import HydrationLimitError

    with client() as data:
        paths = data.paths_between(AOP, target_value="pHENOBARBITAL", max_hops=3)
        assert paths.attrs["routes"]
        for route in paths.attrs["routes"]:
            bindings = route["bindings"]
            assert (URIRef(bindings["n0"]["value"]), RDF.type, URIRef(AOP)) in data.source
            for i in range(route["hops"]):
                s, p, o = (URIRef(bindings[k]["value"]) for k in (f"n{i}", f"p{i}", f"n{i + 1}"))
                assert p != RDF.type
                assert (
                    (o, p, s) if bindings[f"back{i}"]["value"] == "true" else (s, p, o)
                ) in data.source
        selected = paths[paths["Path"] == paths["Path"].iloc[0]]
        queries = len(data.queries)
        diagram = data.diagram(paths=selected)
        assert "https://identifiers.org/aop.stressor/133" in diagram
        assert "https://identifiers.org/cas/50-06-6" not in diagram
        assert len(data.queries) == queries
        whole = data.diagram(paths=paths, path=1)
        assert whole == diagram
        types = data.diagram(paths=paths, path=1, instances=False)
        assert STRESSOR.replace("#", "#35;") in types
        assert "https://identifiers.org/aop.stressor/133" not in types
        with pytest.raises(ValueError, match="Path number"):
            data.diagram(paths=paths, path=999)
        reverse = data.paths_between(CHEMICAL, AOP, max_hops=2)
        assert "N1 -->" in data.diagram(paths=reverse)
        assert data.paths_between(
            AOP, target_value='absent" } UNION { ?s ?p ?o } #', max_hops=1
        ).empty
        with pytest.raises(HydrationLimitError):
            data.paths_between(AOP, target_value="Phenobarbital", max_hops=2, max_paths=1)
        before = len(data.queries)
        for kwargs in (
            {},
            {"target_value": ""},
            {"target": CHEMICAL, "target_value": "Phenobarbital"},
        ):
            with pytest.raises(ValueError):
                data.paths_between(AOP, **kwargs)
        assert len(data.queries) == before


def test_connections_keep_aopwiki_relative_iris_and_read_their_classes():
    # AOPWiki returned these links and this non-absolute IRI on 2026-09-08.
    event = URIRef("https://identifiers.org/aop.events/1023")
    node = URIRef("1023_bioevent_0")
    cls = URIRef("http://aopkb.org/aop_ontology#BiologicalEvent")
    with client() as data:
        data.source.add(
            (
                URIRef("https://identifiers.org/aop/162"),
                URIRef("http://aopkb.org/aop_ontology#has_key_event"),
                event,
            )
        )
        data.source.add((event, URIRef("http://aopkb.org/aop_ontology#hasBiologicalEvent"), node))
        data.source.add((node, RDF.type, cls))
        other = URIRef("281_bioevent_0")
        other_event = URIRef("https://identifiers.org/aop.events/281")
        data.source.add(
            (
                URIRef("https://identifiers.org/aop/162"),
                URIRef("http://aopkb.org/aop_ontology#has_key_event"),
                other_event,
            )
        )
        data.source.add(
            (other_event, URIRef("http://aopkb.org/aop_ontology#hasBiologicalEvent"), other)
        )
        data.source.add((other, RDF.type, cls))
        paths = data.connections("https://identifiers.org/aop/162", max_hops=2)
        matches = paths[paths["To"] == str(node)]
        assert not matches.empty
        assert set(matches["To class"]) == {"BiologicalEvent"}
        assert str(node) in data.diagram(paths=paths)
        assert str(cls).replace("#", "#35;") in data.diagram(paths=paths, instances=False)
        assert all("<1023_bioevent_0>" not in query for query in data.queries)
        assert set(paths.attrs["unresolved_resources"]) == {str(node), str(other)}
        steps = data.session_metadata()["steps"]
        assert (
            sum(
                len(s["query_ids"])
                for s in steps
                if s["name"] == "Read classes through retained links"
            )
            == 1
        )


def test_connections_without_target_match_the_aop_neighborhood():
    from rdfsolve.client.hydration import HydrationLimitError

    with client() as data:
        iri = "https://identifiers.org/aop/162"
        data.batch_size = 1
        paths = data.connections(iri, max_hops=2)
        steps = data.session_metadata()["steps"]
        assert (
            sum(len(s["query_ids"]) for s in steps if s["name"] == "Read classes along connections")
            == 1
        )
        expected = set()
        frontier = {URIRef(iri)}
        for _ in range(2):
            neighbors = set()
            for node in frontier:
                for s, p, o in data.source:
                    if p != RDF.type and not isinstance(o, Literal) and node in (s, o):
                        expected.add((str(s), str(p), str(o)))
                        neighbors.update((s, o))
            frontier = neighbors - {URIRef(iri)}
        actual = set()
        for route in paths.attrs["routes"]:
            b = route["bindings"]
            for i in range(route["hops"]):
                s, p, o = (b[key]["value"] for key in (f"n{i}", f"p{i}", f"n{i + 1}"))
                actual.add((o, p, s) if b[f"back{i}"]["value"] in ("true", "1") else (s, p, o))
        assert actual == expected
        assert iri in data.diagram(paths=paths)
        untyped = {
            node
            for s, _, o in expected
            for node in (s, o)
            if not list(data.source.objects(URIRef(node), RDF.type))
        }
        assert untyped
        class_view = data.diagram(paths=paths, instances=False)
        assert all(node in class_view for node in untyped)
        with pytest.raises(HydrationLimitError):
            data.connections(iri, max_hops=2, max_paths=1)


def test_default_connections_return_a_marked_partial_view():
    with client() as data:
        data.max_rows = 5
        with pytest.warns(UserWarning, match="Partial connections view"):
            paths = data.connections("https://identifiers.org/aop/162", max_hops=2)
        assert len(paths.attrs["routes"]) == 5
        assert paths.attrs["status"] == "partial"
        assert data.session_metadata()["steps"][0]["status"] == "partial"
        assert data.diagram(paths=paths).startswith("Partial view:")


def test_record_paths_bind_the_source_and_verify_its_type():
    with client() as data:
        selected_iri = "https://identifiers.org/aop/162"
        record = data.get(data.model(AOP), selected_iri, fields=[])
        named = data.paths_between(source=record, target_value="Phenobarbital", max_hops=3)
        typed = data.paths_between(source=record, target=CHEMICAL, max_hops=3)
        for table in (named, typed):
            assert table.attrs["routes"]
            assert {route["bindings"]["n0"]["value"] for route in table.attrs["routes"]} == {
                selected_iri
            }
            assert all(q["result_retained"] for q in data.session_metadata()["queries"])
        assert {
            route["bindings"][f"n{route['hops']}"]["value"] for route in typed.attrs["routes"]
        } == {"https://identifiers.org/cas/50-06-6"}
        assert typed.attrs["target_class"] == CHEMICAL
        data.source.remove((URIRef(selected_iri), RDF.type, URIRef(AOP)))
        assert data.paths_between(record, target=CHEMICAL, max_hops=2).empty
        assert data.paths_between(record, target_value="Phenobarbital", max_hops=2).empty


def test_value_paths_do_not_join_graphs():
    with client() as data:
        graphs = Dataset()
        first, second = graphs.graph(URIRef("urn:first")), graphs.graph(URIRef("urn:second"))
        chemical_link = URIRef("http://aopkb.org/aop_ontology#has_chemical_entity")
        for triple in data.source:
            (second if triple[1] == chemical_link else first).add(triple)
        with Client(data._schema, graphs, graph_uris=["urn:first", "urn:second"]) as scoped:
            paths = scoped.paths_between(AOP, target_value="Phenobarbital", max_hops=2)
            assert not paths.empty
            assert "https://identifiers.org/cas/50-06-6" not in paths["To"].values


def test_errors_and_completion_do_not_trigger_hidden_queries():
    with client() as data:
        matches = data.find('absent" } #')
        assert len(matches) == 0
        assert matches.types().empty
        assert matches.values("title").empty
        queries = len(data.queries)
        with pytest.raises(ValueError):
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
        table = pd.DataFrame(
            {"ID": ["https://identifiers.org/cas/50-06-6"], "Name": ["Phenobarbital"]}
        )
        records = data.from_table(CHEMICAL, table, id_column="ID", title="Name")
        assert list(records.values("title")["Value"]) == ["Phenobarbital"]
        output = tmp_path / "chemical.ttl"
        data.save(output, records)
        assert all(triple in data.source for triple in Graph().parse(output))
        with pytest.raises(ValueError, match="Missing columns"):
            data.from_table(CHEMICAL, table, id_column="missing", title="Name")
        with pytest.raises(ValueError):
            data.from_table(CHEMICAL, table.assign(ID="not an IRI"), id_column="ID", title="Name")


def test_path_sets_preserve_all_sources_and_retained_evidence():
    from rdfsolve.client.api import Results

    with client() as data:
        selected = data.get_many(
            data.model(AOP),
            ["https://identifiers.org/aop/107", "https://identifiers.org/aop/162"],
            fields=[],
        )
        records = Results(data, selected)
        paths = records.paths_between(CHEMICAL, max_hops=2)
        assert {r["bindings"]["n0"]["value"] for r in paths.attrs["routes"]} == {
            r.uri for r in selected
        }
        assert {r["bindings"]["n2"]["value"] for r in paths.attrs["routes"]} == {
            "https://identifiers.org/cas/50-06-6"
        }
        assert sum(o["matches"] for o in paths.attrs["observations"]) == 2
        assert paths.attrs["fragments"][0].anchors[0][1].value == selected[1].uri
        assert records.summary()["records"] == 2
        assert "50-06-6" not in str(records.summary())
        assert all(o["query_ids"] for o in paths.attrs["observations"])
        before = len(data.queries)
        assert data.trace()["source_queries"] == before and data.query_log().queries
        assert len(data.queries) == before
        limited = records.paths_between(CHEMICAL, max_hops=2, max_paths=1, allow_partial=True)
        assert limited.attrs["status"] == "partial" and len(limited.attrs["routes"]) == 1
        assert Results(data, []).paths_between(CHEMICAL).empty
        with client() as other, pytest.raises(ValueError, match="this Client"):
            other.paths_between(records, CHEMICAL)


def test_shape_navigation_uses_generated_fields_without_pairwise_patterns():
    from rdfsolve.api import Client
    from rdfsolve.schema_models.core import MinedSchema

    schema = MinedSchema.from_shacl("""@prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix e: <urn:shape:> . e:AShape a sh:NodeShape; sh:targetClass e:A;
      sh:property [sh:path (e:link [sh:inversePath e:back]); sh:name "Related resource";
      sh:qualifiedValueShape [ sh:class e:B ]; sh:qualifiedMinCount 0] .
    e:BShape a sh:NodeShape; sh:targetClass e:B .""")
    graph = Graph().parse(
        data="""@prefix e: <urn:shape:> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    e:a a e:A; rdfs:label "root"; e:link e:x . e:b a e:B; e:back e:x .""",
        format="turtle",
    )
    with Client(schema, graph, graph_uris=[]) as data:
        assert not schema.patterns
        source = data.find("root")
        assert list(vars(source.fields)) == ["related_resource"]
        assert source.paths()["To"].tolist() == ["B"]
        assert [r.uri for r in source.related("B")] == ["urn:shape:b"]
        assert data.links(type(source[0])).iloc[0].basis == "SHACL qualified subset"
        schema_paths = data.paths_between("A", "B")
        assert "Intermediate resource" in data.diagram(paths=schema_paths)
        observed = source.paths_between("B", max_hops=2)
        assert observed.attrs["routes"][0]["bindings"]["n2"]["value"] == "urn:shape:b"
        assert not any("?n1 a" in q for q in data.queries)
        found = data.search(["urn:shape:b"], kind="A", fields=["Related resource"])
        assert [r.uri for r in found] == ["urn:shape:a"]
        assert found.evidence[0]["path"]["operator"] == "sequence"
        assert found.evidence[0]["predicate"] is None
        assert all(r["query_ids"] for r in data.trace()["steps"] if r["name"].startswith("Find"))


def test_generated_names_disambiguate_equal_display_labels():
    from rdfsolve.api import Client
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern

    schema = MinedSchema(
        about={"dataset_name": "duplicate-labels"},
        patterns=[
            SchemaPattern(subject_class=cls, property_uri="urn:label", object_class="Literal")
            for cls in ("urn:one:DataNode", "urn:two:DataNode")
        ],
    )
    with Client(schema, Graph(), graph_uris=[]) as data:
        for name, model in data.models.items():
            assert data.model(name) is model
        with pytest.raises(ValueError, match="ambiguous"):
            data.model("Data node")
        with pytest.raises(ValueError, match="ambiguous"):
            data.model("Data nodes")
    with client() as data:
        assert data.model("Key Events") is data.model("Key Event")


def test_prepare_chemical_fields_directly_from_client():
    from rdfsolve.api import Requirement

    number = "http://semanticscience.org/resource/CHEMINF_000446"
    title_iri = "http://purl.org/dc/elements/1.1/title"
    schema = MinedSchema(
        about={"dataset_name": "chemical-fixture"},
        patterns=[
            SchemaPattern(subject_class=c, property_uri=p, object_class="Literal")
            for c, p in ((CHEMICAL, number), (CHEMICAL, title_iri), (STRESSOR, title_iri))
        ],
    )
    graph = Graph().parse(
        data=f'<urn:c> a <{CHEMICAL}>; <{number}> "50-06-6"; <{title_iri}> "Phenobarbital" .',
        format="turtle",
    )
    with Client(schema, graph, graph_uris=[]) as data:
        fields = data.describe(owners=[CHEMICAL]).set_index("Field")
        cas = fields.loc[
            data.field_name(
                data.model(CHEMICAL), "http://semanticscience.org/resource/CHEMINF_000446"
            )
        ]
        title = fields.loc[data.field_name(data.model(CHEMICAL), "title")]
        requirements = [
            Requirement(clause=name, kind="output", concept=row["Label"], owner=CHEMICAL)
            for name, row in (("CAS registry number", cas), ("Chemical name", title))
        ]
        before = len(data.queries)
        query = data.prepare(
            f"SELECT ?CAS ?ChemicalName WHERE {{ ?c {cas['Reference']} ?CAS; {title['Reference']} ?ChemicalName }}",
            requirements=requirements,
        )
        assert len(data.queries) == before
        result = data.select(query)
        assert [(r["CAS"].value, r["ChemicalName"].value) for r in result.rows] == [
            ("50-06-6", "Phenobarbital")
        ]
        assert (
            data.describe(owners=[CHEMICAL])["Reference"].tolist() == fields["Reference"].tolist()
        )
        log = data.session_metadata()["prepared_queries"][query.ref]
        assert log["sparql"] == query.sparql and len(log["uses"]) == 2
        with pytest.raises(ValueError, match="every row"):
            data.prepare(
                f"SELECT ?ChemicalName WHERE {{ ?c a <{CHEMICAL}> . OPTIONAL {{ ?c {title['Reference']} ?ChemicalName }} }}",
                requirements=[requirements[1]],
            )
        with pytest.raises(ValueError, match="different class"):
            data.prepare(
                f"SELECT ?CAS WHERE {{ ?s a <{STRESSOR}>; <http://semanticscience.org/resource/CHEMINF_000446> ?CAS }}"
            )


def test_prepare_selected_entity_paths_without_mcp():
    with client() as data:
        found = data.find("Phenobarbital", kind=CHEMICAL)
        assert data.catalogue.records[found.references[0]] is found[0]
        paths = data.paths_between(found, AOP, max_hops=2)
        assert paths["Reference"].notna().all()
        ref = paths.iloc[0]["Reference"]
        prepared = data.prepare_path(
            ref, source="chemical", target="aop", fields={"chemical": ["title"]}
        )
        assert "50-06-6" in prepared.sparql and "OPTIONAL" in prepared.sparql
        direct = data.retrieve(ref, source="chemical", target="aop", fields={"chemical": ["title"]})
        assert {r["aop"].value for r in direct.rows} == {
            "https://identifiers.org/aop/107",
            "https://identifiers.org/aop/162",
        }
        assert all(r["chemical_title"].value == "Phenobarbital" for r in direct.rows)
        assert list(direct.table()) == ["chemical", "aop", "chemical_title"]
        data.source.remove(
            (URIRef(found[0].uri), URIRef("http://purl.org/dc/elements/1.1/title"), None)
        )
        missing = data.retrieve(
            ref, source="chemical", target="aop", fields={"chemical": ["title"]}
        )
        assert missing.row_count == 2 and all("chemical_title" not in r for r in missing.rows)
        with pytest.raises(ValueError, match="source or target"):
            data.retrieve(ref, fields={"wrong": ["title"]})
        query = data.prepare(f"SELECT DISTINCT ?aop WHERE {{ ?chemical {ref} ?aop }}")
        assert {row["aop"].value for row in data.select(query).rows} == {
            "https://identifiers.org/aop/107",
            "https://identifiers.org/aop/162",
        }
        assert "VALUES" in query.sparql and "50-06-6" in query.sparql
        assert data.select(query, limit=1).row_count == 1
        assert data.select(query).row_count == 2
        with client() as other, pytest.raises(ValueError, match="current client"):
            other.select(query)
        data.graph_uris = ["urn:different-graph"]
        with pytest.raises(ValueError, match="source scope"):
            data.select(query)
        data.graph_uris = []
        query.sparql = "SELECT ?aop WHERE { FILTER(false) }"
        with pytest.raises(ValueError, match="Prepare this query"):
            data.select(query)


def test_client_resolves_output_aliases_and_owner_qualified_fields():
    with client() as data:
        owner = data.type_name(data.model(CHEMICAL))
        title = data.describe(owner + " title", owners=[CHEMICAL]).iloc[0]
        assert title["Field"] == "title"
        goal = {
            "clause": owner + " title",
            "kind": "output",
            "concept": "display_text",
            "owner": "chemical",
        }
        query = data.prepare(
            f"SELECT ?display_text WHERE {{ ?chemical a <{CHEMICAL}>; <http://purl.org/dc/elements/1.1/title> ?display_text }}",
            requirements=[goal],
            output_variables=["display_text"],
        )
        assert data.select(query).rows[0]["display_text"].value == "Phenobarbital"
        assert query.diagnostics["grounding"]["g1"]["evidence"] == [title["Reference"]]


def test_broad_name_search_preserves_explicit_partial_evidence():
    from rdfsolve.client.hydration import HydrationLimitError

    with client() as data:
        data.max_rows = 1
        with pytest.raises(HydrationLimitError):
            data.find("Phenobarbital")
        found = data.find("Phenobarbital", allow_partial=True)
        assert found and found.coverage["status"] == "partial"
        assert found.evidence and found.summary()["coverage"]["limit_reached"]


@pytest.mark.parametrize("class_concept", ["Chemical entity", "chemical"])
def test_prepare_distinguishes_class_and_predicate_evidence(class_concept):
    from rdfsolve.api import Requirement
    from rdfsolve.client.retrieval import QueryValidationError
    from rdfsolve.schema_models.enrichment import RdfTerm, SchemaEnrichment, TermAnnotation

    c, p = "urn:C01", "urn:P01"
    schema = MinedSchema(
        about={"dataset_name": "punning"},
        patterns=[
            SchemaPattern(subject_class=c, property_uri=p, object_class="Literal"),
            SchemaPattern(subject_class=p, property_uri="urn:title", object_class="Literal"),
        ],
    )
    schema.enrichment = SchemaEnrichment(
        labels=[
            TermAnnotation(
                term_iri=iri,
                predicate="http://www.w3.org/2000/01/rdf-schema#label",
                text=RdfTerm(kind="literal", value=label),
            )
            for iri, label in ((c, "Chemical entity"), (p, "CAS registry number"))
        ],
        definitions=[
            TermAnnotation(
                term_iri=p,
                predicate="http://www.w3.org/2004/02/skos/core#definition",
                text=RdfTerm(kind="literal", value="The CAS number assigned to a chemical entity."),
            )
        ],
    )
    graph = Graph().parse(data=f'<urn:chemical> a <{c}>; <{p}> "50-06-6" .', format="turtle")
    with Client(schema, graph, graph_uris=[]) as data:
        goals = [
            Requirement(clause="all chemicals", concept=class_concept, kind="output"),
            Requirement(clause="CAS number", concept="CAS", kind="output"),
        ]
        text = f"SELECT ?chemical ?CAS WHERE {{ ?chemical a <{c}>; <{p}> ?CAS }}"
        query = data.prepare(text, requirements=goals, output_variables=["chemical", "CAS"])
        witnesses = query.diagnostics["grounding"]
        assert witnesses["g1"]["evidence"] == [data.catalogue.type_refs[c]]
        assert data.catalogue.fragments[witnesses["g2"]["evidence"][0]].kind == "field"
        assert data.select(query).rows[0]["CAS"].value == "50-06-6"
        with pytest.raises(QueryValidationError, match="every row"):
            data.prepare(
                text.replace(f"; <{p}> ?CAS", f". OPTIONAL {{ ?chemical <{p}> ?CAS }}"),
                requirements=goals,
            )
        before = len(data.queries)
        with pytest.raises(QueryValidationError, match="ChemicalName") as error:
            data.prepare(text, output_variables=["CAS", "ChemicalName"])
        assert error.value.code == "missing_outputs" and len(data.queries) == before


@pytest.mark.parametrize(
    "text, hint",
    [
        ("SELECT ?s WHERE { ?s ?p ?o LIMIT 4 }", "limit_placement"),
        ("SELECT ?s WHERE { ?s http://example.org/p ?o }", "iri_brackets"),
        ('SELECT ?s WHERE { ?s ?p ?o . FILTER(strcontains(STR(?o), "x")) }', "function_name"),
    ],
)
def test_prepare_explains_syntax_without_source_queries(text, hint):
    from rdfsolve.client.query_fragments import QuerySyntaxError

    with client() as data, pytest.raises(QuerySyntaxError) as error:
        data.prepare(text)
    assert hint in {h["code"] for h in error.value.detail["hints"]}
    assert error.value.detail["executed"] is False and not data.queries


def test_same_named_class_and_field_keep_distinct_outputs():
    from rdflib import RDFS, Graph

    from rdfsolve.api import Client, MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern

    chemical, number, predicate = "urn:Chemical", "urn:CASRegistryNumber", "urn:casRegistryNumber"
    schema = MinedSchema(
        about={"dataset_name": "names"},
        patterns=[
            SchemaPattern(subject_class=chemical, property_uri=predicate, object_class="Literal"),
            SchemaPattern(
                subject_class=chemical, property_uri=str(RDFS.label), object_class="Literal"
            ),
            SchemaPattern(
                subject_class=number, property_uri=str(RDFS.label), object_class="Literal"
            ),
        ],
    )
    from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation

    for iri in (number, predicate):
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=iri,
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value="CAS registry number"),
            )
        )
    graph = Graph().parse(
        data=f'<urn:c> a <{chemical}>; <{predicate}> "50-06-6"; <{RDFS.label}> "Name" . <urn:n> a <{number}>; <{RDFS.label}> "50-06-6" .',
        format="turtle",
    )
    with Client(schema, graph, graph_uris=[]) as client:
        goals = [
            {
                "clause": "CAS registry numbers",
                "kind": "output",
                "concept": "CAS registry number",
                "binding": "number",
            }
        ]
        result = client.prepare(
            f"SELECT ?number WHERE {{ ?c a <{chemical}>; <{predicate}> ?number }}",
            requirements=goals,
        )
        assert client.select(result).rows[0]["number"].value == "50-06-6"
        request = {
            "clause": "Registry resources",
            "kind": "output",
            "concept": "CAS registry number",
        }
        result = client.prepare(
            f"SELECT ?number WHERE {{ ?number a <{number}> }}", requirements=[request]
        )
        assert client.select(result).rows[0]["number"].value == "urn:n"


def test_connected_network_compiles_shared_roles_and_exact_entity_terms():
    from rdfsolve.api import QueryPattern

    with client() as data:
        found = data.find("Phenobarbital").of_type(CHEMICAL)
        term = data.catalogue.retain_records(found)[0]
        paths = data.paths_between(AOP, CHEMICAL)
        route = paths.attrs["references"][0]
        patterns = [QueryPattern(reference=route, bindings=["aop", "chemical"])]
        prepared = data.prepare_network(
            patterns, outputs=["aop", "chemical"], values={"chemical": term}
        )
        rows = data.select(prepared).rows
        assert {r["aop"].value for r in rows} == {
            "https://identifiers.org/aop/107",
            "https://identifiers.org/aop/162",
        }
        with pytest.raises(ValueError, match="connect"):
            data.prepare_network(
                [
                    *patterns,
                    QueryPattern(reference=data.catalogue.type_refs[AOP], bindings=["unrelated"]),
                ],
                outputs=["aop"],
            )


def test_network_optional_descendants_preserve_missing_parent():
    from rdfsolve.api import QueryPattern

    with client() as data:
        aop = URIRef("urn:empty:aop")
        data.source.add((aop, RDF.type, URIRef(AOP)))
        route = data.paths_between(AOP, CHEMICAL).attrs["references"][0]
        label = next(
            ref
            for (owner, name), ref in data.catalogue.field_refs.items()
            if owner == CHEMICAL
            and data.catalogue.fragments[ref].path.iri == "http://purl.org/dc/elements/1.1/title"
        )
        patterns = [
            QueryPattern(reference=data.catalogue.type_refs[AOP], bindings=["a"]),
            QueryPattern(reference=route, bindings=["a", "c"], optional=True),
            QueryPattern(reference=label, bindings=["c", "name"], optional=True),
        ]
        prepared = data.prepare_network(patterns, outputs=["a", "c", "name"])
        rows = data.select(prepared).rows
        assert [set(r) for r in rows if r["a"].value == str(aop)] == [{"a"}]
        assert any(r.get("name") and r["name"].value == "Phenobarbital" for r in rows)


def test_network_shares_the_requested_record_and_keeps_other_routes_possible():
    from rdfsolve.api import QueryPattern
    from rdfsolve.client.query_fragments import Fragment
    from rdfsolve.schema_models.enrichment import RdfTerm

    graph = Graph().parse(
        data="""@prefix e: <urn:jobs:> .
        e:alice a e:Person; e:job e:j1, e:j2 . e:bob a e:Person; e:job e:j3 .
        e:j1 a e:Job; e:employer e:X; e:year 2010 .
        e:j2 a e:Job; e:employer e:Y; e:year 2020 .
        e:j3 a e:Job; e:employer e:X; e:year 2020 .
        e:X a e:Org . e:Y a e:Org .""",
        format="turtle",
    )
    schema = MinedSchema(
        about={},
        patterns=[
            SchemaPattern(
                subject_class="urn:jobs:" + a,
                property_uri="urn:jobs:" + p,
                object_class=b if b == "Literal" else "urn:jobs:" + b,
            )
            for a, p, b in [
                ("Person", "job", "Job"),
                ("Job", "employer", "Org"),
                ("Job", "year", "Literal"),
            ]
        ],
    )
    data = Client(schema, graph)

    def field(owner, predicate, *roles):
        ref = next(
            r
            for (o, n), r in data.catalogue.field_refs.items()
            if o == "urn:jobs:" + owner
            and data.catalogue.fragments[r].path.iri == "urn:jobs:" + predicate
        )
        return QueryPattern(reference=ref, bindings=list(roles))

    def term(value):
        return data.catalogue._put(Fragment("term", value.value, term=value), value.model_dump())

    patterns = [
        field("Person", "job", "person", "job"),
        field("Job", "employer", "job", "org"),
        field("Job", "year", "job", "year"),
    ]
    values = {
        "org": term(RdfTerm(kind="uri", value="urn:jobs:X")),
        "year": term(
            RdfTerm(
                kind="literal", value="2020", datatype="http://www.w3.org/2001/XMLSchema#integer"
            )
        ),
    }
    query = data.prepare_network(patterns, outputs=["person"], values=values)
    assert [row["person"].value for row in data.select(query).rows] == ["urn:jobs:bob"]


def test_grounded_gene_output_uses_its_owner_and_requested_column():
    from rdflib import RDFS

    from rdfsolve.client.retrieval import QueryValidationError
    from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation

    base = "urn:grounding:"
    schema = MinedSchema(
        about={},
        patterns=[
            SchemaPattern(
                subject_class=base + a,
                property_uri=base + p,
                object_class="Literal" if b == "Literal" else base + b,
            )
            for a, p, b in [
                ("Protein", "sameAs", "Gene"),
                ("Gene", "geneId", "Literal"),
                ("Protein", "label", "Literal"),
            ]
        ],
    )
    for name, label in [
        ("Protein", "Protein"),
        ("Gene", "Gene"),
        ("sameAs", "protein gene link"),
        ("geneId", "NCBI gene identifier"),
        ("label", "protein name"),
    ]:
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=base + name,
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=label),
            )
        )
    graph = Graph().parse(
        data="""@prefix e: <urn:grounding:> .
        e:p a e:Protein; e:sameAs e:g; e:label "Name" .
        e:g a e:Gene; e:geneId "42" .""",
        format="turtle",
    )
    with Client(schema, graph, graph_uris=[]) as data:

        def field(owner, predicate, *roles):
            ref = next(
                ref
                for (o, _), ref in data.catalogue.field_refs.items()
                if o == base + owner and data.catalogue.fragments[ref].path.iri == base + predicate
            )
            return {"reference": ref, "bindings": list(roles)}

        patterns = [
            field("Protein", "sameAs", "po", "gene"),
            field("Gene", "geneId", "gene", "prot"),
            field("Protein", "label", "po", "name"),
        ]
        goal = {
            "clause": "NCBI gene identifier",
            "kind": "output",
            "concept": "prot",
            "owner": "gene",
        }
        query = data.prepare_network(patterns, outputs=["name", "prot"], requirements=[goal])
        assert data.select(query).rows[0]["prot"].value == "42"
        assert query.diagnostics["grounding"]["g1"]["evidence"] == [patterns[1]["reference"]]
        wrong = [
            patterns[0],
            {**patterns[1], "bindings": ["gene", "name"]},
            {**patterns[2], "bindings": ["po", "prot"]},
        ]
        with pytest.raises(QueryValidationError, match="project the selected"):
            data.prepare_network(wrong, outputs=["name", "prot"], requirements=[goal])
        with pytest.raises(QueryValidationError) as caught:
            data.prepare_network(
                patterns,
                outputs=["prot"],
                requirements=[{**goal, "concept": "protein gene link", "binding": "prot"}],
                grounding={"g1": {"evidence": [patterns[0]["reference"]]}},
            )
        assert caught.value.code == "goal_owner"
        assert "optional" not in str(caught.value)
        relation = {
            "clause": "protein gene link",
            "kind": "relation",
            "concept": "protein gene link",
            "owner": "po",
        }
        good = data.prepare_network(patterns, outputs=["prot"], requirements=[relation])
        assert good.diagnostics["grounding"]["g1"]["evidence"] == [patterns[0]["reference"]]
        with pytest.raises(QueryValidationError, match="class alone"):
            data.prepare_network(
                patterns,
                outputs=["prot"],
                requirements=[relation],
                grounding={"g1": {"evidence": [data.catalogue.type_refs[base + "Gene"]]}},
            )


def test_paths_rank_meaning_keep_mined_routes_and_enforce_via():
    from rdflib import Graph, RDFS
    from rdfsolve.api import Client, MinedSchema, QueryPattern
    from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation
    from rdfsolve.schema_models.navigation import NavigationPath, NavigationSummary
    from rdfsolve.schema_models.pattern import SchemaPattern

    def edge(s, p, o):
        return SchemaPattern(
            subject_class="urn:" + s, property_uri="urn:" + p, object_class="urn:" + o
        )

    schema = MinedSchema(
        about={"dataset_name": "routes"},
        patterns=[
            edge("Event", "cell", "Cell"),
            edge("Cell", "gene", "Gene"),
            edge("Event", "annotatedProtein", "Protein"),
            edge("Protein", "gene", "Gene"),
            edge("Event", "other", "Other"),
            edge("Other", "gene", "Gene"),
        ],
    )
    for iri, label in [
        ("annotatedProtein", "annotated protein object"),
        ("Protein", "protein object"),
    ]:
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri="urn:" + iri,
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=label),
            )
        )
    route = NavigationPath(
        steps=[edge("Event", "observed", "Protein"), edge("Protein", "gene", "Gene")],
        instance_support="matched",
        source_count=2,
        matched_sources=1,
    )
    schema.navigation = NavigationSummary(
        max_hops=2, max_paths_per_length=1, edge_count=6, walk_counts={2: 3}, paths=[route]
    )
    graph = Graph().parse(
        data="<urn:e> a <urn:Event>; <urn:observed> <urn:p> . <urn:p> a <urn:Protein>; <urn:gene> <urn:g> . <urn:g> a <urn:Gene> .",
        format="turtle",
    )
    with Client(schema, graph, graph_uris=[]) as data:
        table = data.paths_between(
            "urn:Event",
            "urn:Gene",
            meaning="annotated protein object",
            max_paths=1,
            allow_partial=True,
        )
        assert table.attrs["routes"][0][0][1] == "urn:annotatedProtein"
        assert table.attrs["truncated"]
        table = data.paths_between(
            "urn:Event", "urn:Gene", via=("urn:Protein",), max_paths=1, allow_partial=True
        )
        assert table.attrs["observations"][0]["basis"] == "mined snapshot"
        ref = table.attrs["references"][0]
        query = data.prepare_network(
            [QueryPattern(reference=ref, bindings=["event", "protein", "gene"])],
            outputs=["event", "gene"],
            requirements=[
                {
                    "clause": "through a protein",
                    "kind": "relation",
                    "concept": "protein",
                    "owner": "event",
                    "target": "gene",
                }
            ],
        )
        assert data.select(query).rows[0]["gene"].value == "urn:g"
        with pytest.raises(ValueError, match="connect the selected owner"):
            data.prepare_network(
                [QueryPattern(reference=ref, bindings=["event", "protein", "gene"])],
                outputs=["event", "gene"],
                requirements=[
                    {
                        "clause": "through a protein",
                        "kind": "relation",
                        "concept": "protein",
                        "owner": "event",
                        "target": "wrong",
                    }
                ],
            )
        assert (
            data.paths_between("urn:Event", "urn:Gene", via=("urn:Cell",)).attrs["routes"][0][0][2]
            == "urn:Cell"
        )
