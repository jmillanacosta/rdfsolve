"""rdfsolve.client: the cross-references of a schema, read from its examples and patterns."""

from types import SimpleNamespace

from tests.mappings.test_claims import IDO, WP


def test_cross_references_of_a_schema():
    """A LIPID MAPS id that fails the pattern still counts as LIPID MAPS; a property whose
    values the source describes itself (publications) is no cross-reference."""
    from rdfsolve.client.api import Client

    def example(prop, value):
        return SimpleNamespace(property_uri=prop, value=SimpleNamespace(value=value))

    refs = "http://purl.org/dc/terms/references"
    schema = SimpleNamespace(
        enrichment=SimpleNamespace(
            examples=[
                example(WP + "bdbLipidMaps", IDO + "lipidmaps/LMPR0106010002"),
                example(WP + "bdbLipidMaps", IDO + "lipidmaps/LMSP02"),  # fails the pattern
                example(refs, IDO + "pubmed/22628558"),
                example(WP + "bdbChEBI", IDO + "chebi/CHEBI:15377"),
                example("http://purl.org/dc/terms/isPartOf", IDO + "wikipathways/WP4726"),
            ]
        ),
        patterns=[
            SimpleNamespace(property_uri=WP + "bdbLipidMaps", object_class="Resource"),
            SimpleNamespace(property_uri=WP + "bdbChEBI", object_class="Resource"),
            SimpleNamespace(property_uri=refs, object_class=WP + "PublicationReference"),
            SimpleNamespace(
                property_uri="http://purl.org/dc/terms/isPartOf", object_class="Resource"
            ),
        ],
    )
    wp = SimpleNamespace(_schema=schema, issued_kinds=lambda: {"wikipathways": [WP + "DataNode"]})
    assert Client.cross_references(wp) == [WP + "bdbChEBI", WP + "bdbLipidMaps"]
