"""rdfsolve.iris: CURIEs by the longest namespace, IRIs from CURIEs, local names and namespaces."""

from rdfsolve.iris import curie, expand, local_name, namespace

PREFIXES = {"ex": "https://example.org/", "item": "https://example.org/item/"}


def test_iri_helpers():
    assert curie("https://example.org/item/7", PREFIXES) == (
        "item:7",
        "item",
        "https://example.org/item/",
    ), "The longest namespace names the CURIE"
    assert curie("https://example.org/name", PREFIXES)[0] == "ex:name"
    assert curie("https://other.example/7", PREFIXES) is None
    assert expand("item:7", PREFIXES) == "https://example.org/item/7"
    assert expand("unknown:7", PREFIXES) == "unknown:7"
    assert local_name("https://example.org/terms#Thing") == "Thing"
    assert local_name("https://example.org/item/7") == "7"
    assert namespace("http://purl.obolibrary.org/obo/OBI_0000070") == (
        "http://purl.obolibrary.org/obo/OBI_"
    )
    assert namespace("https://example.org/item/7") == "https://example.org/item/"
