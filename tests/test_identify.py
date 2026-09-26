"""Find which source resources carry an identifier, then read what the source says about them."""

from rdflib import RDFS, Dataset, Literal, URIRef

from rdfsolve.api import Client
from rdfsolve.schema_models import MinedSchema

DATA = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<urn:g> {
  <urn:item> <urn:direct/orcid> "0000-0002-4166-7093" ; <urn:claim/orcid> <urn:statement> ;
      <urn:employer> <urn:org> ; rdfs:label "Javier"@en, "Javier"@es .
  <urn:statement> <urn:value/orcid> "0000-0002-4166-7093" .
  <urn:author> <urn:orcidId> "https://orcid.org/0000-0002-4166-7093" .
  <urn:work> <urn:doi> "10.1093/BIOINFORMATICS/BTAG064" ; <urn:cites> <https://doi.org/10.1038/x> .
  <urn:org> rdfs:label "Maastricht University"@en, "Universiteit Maastricht"@nl .
}
<urn:other> { <urn:elsewhere> <urn:doi> "10.1093/bioinformatics/btag064" . }
"""


def test_identifiers_resolve_across_spellings_then_statements_follow():
    data = Dataset().parse(data=DATA, format="trig")
    with Client(MinedSchema(), data, graph_uris=["urn:g"]) as source:
        found = source.identify(["orcid:0000-0002-4166-7093", "doi:10.1093/bioinformatics/btag064", "doi:10.1234/absent"])
        by_resource = {(m.identifier, m.resource): m for m in found}
        assert set(by_resource) == {
            ("orcid:0000-0002-4166-7093", "urn:item"),
            ("orcid:0000-0002-4166-7093", "urn:author"),
            ("doi:10.1093/bioinformatics/btag064", "urn:work"),
        }, "Every spelling is tried; the statement node is an intermediate; other graphs are out of scope"
        item = by_resource[("orcid:0000-0002-4166-7093", "urn:item")]
        assert (item.predicate, item.value) == ("urn:direct/orcid", "0000-0002-4166-7093")
        assert item.intermediates == ["urn:statement"]
        author = by_resource[("orcid:0000-0002-4166-7093", "urn:author")]
        assert author.value == "https://orcid.org/0000-0002-4166-7093", "IRI spelled as a string"
        assert not [m for m in found if m.identifier == "doi:10.1234/absent"], "No match is no row"

        graph = source.statements(["urn:item", "urn:work"], languages=["en"])
        assert (URIRef("urn:item"), URIRef("urn:employer"), URIRef("urn:org")) in graph
        assert (URIRef("urn:item"), RDFS.label, Literal("Javier", lang="es")) not in graph, "Languages"
        assert graph.value(URIRef("urn:org"), RDFS.label) == Literal("Maastricht University", lang="en")
        assert (URIRef("urn:work"), URIRef("urn:cites"), URIRef("https://doi.org/10.1038/x")) in graph

    from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, SchemaEnrichment
    from rdfsolve.schema_models.pattern import SchemaPattern

    example = PatternExample(
        subject_class="urn:Person", property_uri="urn:direct/orcid",
        subject=RdfTerm(kind="uri", value="urn:someone"),
        value=RdfTerm(kind="literal", value="0000-0001-7536-3744"),
    )
    unregistered = PatternExample(
        subject_class="urn:Person", property_uri="urn:homepage",
        subject=RdfTerm(kind="uri", value="urn:someone"),
        value=RdfTerm(kind="uri", value="http://example.org/unregistered"),
    )
    schema = MinedSchema(
        patterns=[SchemaPattern(subject_class="urn:Person", property_uri="urn:direct/orcid", object_class="Literal")],
        enrichment=SchemaEnrichment(examples=[example, unregistered]),
    )
    with Client(schema, data, graph_uris=["urn:g"]) as guided:
        found = guided.identify(["orcid:0000-0002-4166-7093"])
        sent = [q for q in guided.queries if "Identify" in q or "VALUES (?key ?o)" in q]
        assert [(m.resource, m.method) for m in found] == [("urn:item", "schema property")]
        assert sent and not [q for q in sent if "?s ?p ?o ." in q], "Ask only properties that carry ORCIDs"
