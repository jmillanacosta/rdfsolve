"""Use declared fields without inventing observed ranges."""
from rdflib import Graph
from rdfsolve.client.api import Client
from rdfsolve.schema_models import MinedSchema

def test_declared_fields_retrieve_and_validate_original_constraints(caplog):
    source = """
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix e: <https://example.org/books/> .
        e:BookShape a sh:NodeShape; sh:targetClass e:Book;
            sh:closed true;
            sh:ignoredProperties (<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>);
            sh:property [sh:path e:title; sh:minCount 1; sh:pattern "^Book "] .
    """
    imported = MinedSchema.from_shacl(source)
    schema = MinedSchema.from_dict(imported.to_dict())
    data = Graph().parse(data="""
        @prefix e: <https://example.org/books/> .
        e:one a e:Book; e:title "Book one" .
        e:two a e:Book; e:title "Wrong prefix" .
        e:three a e:Book .
    """, format="turtle")
    with Client(schema, data) as client:
        assert client.session_metadata()["local_backend"]["engine"] == "oxigraph"
        book = client.model("https://example.org/books/Book")
        title = client.field_name(book, "https://example.org/books/title")
        records = client.sample(book, fields=[title])
        assert len(records) == 3, "A missing required field must not hide the record"
        selected = schema.select(fields=[("https://example.org/books/Book", "https://example.org/books/title")])
        assert selected.patterns == [], "A required field does not imply a class or datatype"
        result = client.extract(selected, root_class=book)
        assert len(result.roots) == 3 and len(result.quads) == 5
        report = result.assess(schema.get_metadata().to_rdf_graph())
        assert report.state == "violations" and len(report.violations) == 2
        assert {v.focus.value for v in report.violations} == {"https://example.org/books/two", "https://example.org/books/three"}
        assert not any("outside selected" in warning for warning in report.scope_warnings)
        assert report.source_conforms is None


    incomplete = MinedSchema.from_shacl(source + """
        <https://example.org/books/BookShape>
          <http://www.w3.org/ns/shacl#property> <https://example.org/books/ReferencedShape> .
    """)
    with Client(incomplete, data) as client:
        model = client.model("https://example.org/books/Book")
        title = client.field_name(model, "https://example.org/books/title")
        assert len(client.sample(model, fields=[title])) == 3
        assert client.links(model).empty, "An unresolved property supplies no invented target"
        assert "ReferencedShape" in caplog.text and "no client field" in caplog.text
