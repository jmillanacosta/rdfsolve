"""rdfsolve.graph_parts: the data graphs of a source get names and settings for their own schemas,
and registry entries state per-graph settings."""

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from rdfsolve.graph_parts import graph_parts, has_graph_parts
from rdfsolve.models.source_model import SourceModel

ENDPOINT = "https://fixture.invalid/sparql/"
REGISTRY = Path(__file__).resolve().parents[1] / "data" / "sources.yaml"


def _source(**extra):
    graphs = ["https://fixture.invalid/graph/core", "urn:fixture:extra"]
    return SourceModel.model_validate(
        {
            "name": "fixture",
            "endpoint": ENDPOINT,
            "graph_uris": graphs,
            "graph_sources": {g: {"download_ttl": [f"{g}.ttl"]} for g in graphs},
            **extra,
        }
    )


def _entry(name, graphs, endpoint=ENDPOINT, **extra):
    return SourceModel.model_validate(
        {"name": name, "endpoint": endpoint, "graph_uris": graphs, **extra}
    )


def test_a_graph_is_named_after_its_graph_scope_entry_or_after_its_last_segment():
    source = _source()
    registry = [
        source,
        # Same endpoint (without its final slash), the core graph alone: a graph scope.
        _entry("fixture.core_entry", ["https://fixture.invalid/graph/core"], ENDPOINT.rstrip("/")),
        # The same graph on another endpoint is another dataset.
        _entry("elsewhere", ["urn:fixture:extra"], "https://other.invalid/sparql"),
    ]
    parts = graph_parts(source, registry)
    assert [(p.name, p.registry_entry) for p in parts] == [
        ("fixture.core_entry", "fixture.core_entry"),
        ("fixture.extra", None),
    ]
    assert not any(p.own_settings for p in parts)


def test_graph_settings_name_a_graph_and_set_how_it_is_mined():
    source = _source(
        graph_settings={
            "urn:fixture:extra": {
                "name": "fixture.records",
                "classes_as_data": True,
                "membership_properties": ["urn:category"],
            }
        }
    )
    core, extra = graph_parts(source)
    assert (core.name, core.own_settings, core.classes_as_data) == ("fixture.core", False, False)
    assert extra.name == "fixture.records" and extra.own_settings
    assert extra.classes_as_data and extra.membership_properties == ("urn:category",)
    # A setting equal to the source's is not a setting of its own.
    same = _source(
        classes_as_data=True, graph_settings={"urn:fixture:extra": {"classes_as_data": True}}
    )
    assert not any(p.own_settings for p in graph_parts(same))


def test_names_that_clash_must_be_given_in_graph_settings():
    source = _source()
    with pytest.raises(ValueError, match="another registry entry"):
        graph_parts(source, [_entry("fixture.extra", [], "https://other.invalid/sparql")])
    with pytest.raises(ValueError, match="entry or alias"):
        graph_parts(
            source, [_entry("other", [], "https://other.invalid/sparql", aliases=["fixture.extra"])]
        )
    with pytest.raises(ValueError, match="each the graph"):
        graph_parts(
            source,
            [_entry(name, ["urn:fixture:extra"]) for name in ("first", "second")],
        )
    named = _source(graph_settings={"urn:fixture:extra": {"name": "fixture.core"}})
    with pytest.raises(ValueError, match="both named"):
        graph_parts(named)


def test_graph_settings_are_for_data_graphs_and_state_something():
    with pytest.raises(ValidationError, match="not data graphs"):
        _source(graph_settings={"urn:other": {"classes_as_data": True}})
    with pytest.raises(ValidationError, match="states nothing"):
        _source(graph_settings={"urn:fixture:extra": {}})
    with pytest.raises(ValidationError, match="unusable name"):
        _source(graph_settings={"urn:fixture:extra": {"name": "a/b"}})
    with pytest.raises(ValidationError):
        _source(graph_settings={"urn:fixture:extra": {"classes_as_dat": True}})


def test_only_a_source_with_mapped_or_set_graphs_has_parts():
    assert has_graph_parts(_source())
    assert not has_graph_parts(_entry("plain", ["urn:a", "urn:b"]))
    assert has_graph_parts(
        _entry("set", ["urn:a", "urn:b"], graph_settings={"urn:b": {"classes_as_data": True}})
    )


def test_uniprot_mines_its_rhea_graph_as_classes_and_names_its_graphs_after_their_entries():
    registry = [SourceModel.model_validate(row) for row in yaml.safe_load(REGISTRY.read_text())]
    uniprot = next(entry for entry in registry if entry.name == "uniprot")
    parts = {part.graph: part for part in graph_parts(uniprot, registry)}
    rhea = parts["https://sparql.rhea-db.org/rhea"]
    assert rhea.own_settings and rhea.classes_as_data
    # The rhea entry itself keeps its records as classes too.
    assert next(entry for entry in registry if entry.name == "rhea").classes_as_data
    assert not uniprot.classes_as_data, "VoID-first stays for UniProt's other graphs"
    assert parts["http://sparql.uniprot.org/citations"].name == "uniprot.citations"
    assert parts["http://sparql.uniprot.org/citations"].registry_entry == "uniprot.citations"
    assert parts["http://sparql.uniprot.org/uniparc"].registry_entry == "uniprot.uniparc"
    assert parts["http://sparql.uniprot.org/journal"].name == "uniprot.journal"
    assert sum(part.own_settings for part in parts.values()) == 1
    pubchem = next(entry for entry in registry if entry.name == "pubchem.ftp")
    names = {part.name for part in graph_parts(pubchem, registry)}
    assert "pubchem.ftp.anatomy" in names
    assert set(pubchem.aliases) <= names, "pubchem.ftp's per-graph aliases name its parts"


def test_every_uniprot_graph_is_named_uniprot_and_its_local_name():
    """The 17 data graphs of uniprot: 6 named by their graph-scope entries, 11 derived, one rule."""
    registry = [SourceModel.model_validate(row) for row in yaml.safe_load(REGISTRY.read_text())]
    uniprot = next(entry for entry in registry if entry.name == "uniprot")
    parts = graph_parts(uniprot, registry)
    assert {p.name: p.graph.rstrip("/").rsplit("/", 1)[1] for p in parts} == {
        f"uniprot.{local}": local
        for local in [
            "citationmapping",
            "citations",
            "database",
            "diseases",
            "enzymes",
            "journal",
            "keywords",
            "locations",
            "obsolete",
            "pathways",
            "proteomes",
            "taxonomy",
            "tissues",
            "uniparc",
            "uniprot",
            "uniref",
            "rhea",
        ]
    }
    assert sorted(p.name for p in parts if p.registry_entry) == [
        "uniprot.citationmapping",
        "uniprot.citations",
        "uniprot.database",
        "uniprot.diseases",
        "uniprot.enzymes",
        "uniprot.uniparc",
    ]
    assert all(p.registry_entry in (None, p.name) for p in parts)
