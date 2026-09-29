"""The download URLs of a registry entry are one string or a list (Bgee: download_zip is one URL;
the census script read its characters as URLs)."""

from scripts.literal_datatypes import download_urls


def test_download_urls_are_read_from_strings_and_lists():
    entry = {"name": "x", "download_zip": "https://a/x.zip", "download_owl": ["https://a/b.owl", "https://a/c.owl"]}
    assert download_urls(entry) == ["https://a/x.zip", "https://a/b.owl", "https://a/c.owl"]
    assert download_urls({"name": "y"}) == []
