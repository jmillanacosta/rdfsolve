"""Resolve dataset identity across registry entries."""

import csv
import json

import pytest
import yaml
from click.testing import CliRunner

from rdfsolve.cli import main
from rdfsolve.dataset_identity import (
    IdentityRelation,
    catalogs,
    read_overrides,
    resolve_identity,
)

EP = "https://example.org/sparql"


def entry(name, endpoint=EP, graphs=(), **extra):
    return {"name": name, "endpoint": endpoint, "graph_uris": list(graphs), **extra}


def relations(registry, overrides=()):
    result = resolve_identity(registry, overrides)
    return {
        (item.left, item.right): (item.relation, item.decided_by)
        for item in [*result.relations, *result.candidates]
    }


def test_graph_subset_is_a_graph_scope_of_the_wider_entry():
    found = relations(
        [entry("wide", graphs=["urn:g1", "urn:g2"]), entry("narrow", graphs=["urn:g1"])]
    )
    assert found == {("narrow", "wide"): ("graph_scope_of", "rule")}


def test_disjoint_graphs_on_one_endpoint_are_distinct():
    found = relations([entry("a", graphs=["urn:g1"]), entry("b", graphs=["urn:g2"])])
    assert found == {("a", "b"): ("distinct", "rule")}


def test_ambiguous_shared_endpoints_are_only_candidates():
    found = relations(
        [
            entry("same1"),
            entry("same2"),
            entry("named", "https://other.org/sparql", ["urn:g1"]),
            entry("default", "https://other.org/sparql"),
            entry("left", "https://third.org/sparql", ["urn:g1", "urn:g2"]),
            entry("right", "https://third.org/sparql", ["urn:g2", "urn:g3"]),
        ]
    )
    assert found == {
        ("same1", "same2"): ("same_dataset", "candidate"),
        ("named", "default"): ("graph_scope_of", "candidate"),
        ("left", "right"): ("distinct", "candidate"),
    }


def test_endpoint_paths_on_one_service_and_catalog_names_are_candidates():
    found = relations(
        [
            entry("oma", "https://sparql.omabrowser.org/lode/sparql"),
            entry("omabrowser", "https://sparql.omabrowser.org/sparql/"),
            entry("kg1", "https://frink.example/kg1/sparql"),
            entry("kg2", "https://frink.example/kg2/sparql"),
            entry("rhea", "https://sparql.rhea-db.org/sparql"),
            entry("rdfportal.rhea", "https://rdfportal.org/sib/sparql", ["urn:rhea"]),
        ]
    )
    assert found == {
        ("oma", "omabrowser"): ("same_dataset", "candidate"),
        ("rdfportal.rhea", "rhea"): ("distribution_of", "candidate"),
    }


def test_shared_identifier_space_needs_a_matching_name():
    found = relations(
        [
            entry("chembl", "https://a.org/sparql", bioregistry_prefix="chembl"),
            entry("chembl.bigcat", "https://b.org/sparql", bioregistry_prefix="chembl"),
            entry("pubchem.ftp.anatomy", "", bioregistry_prefix="pubchem"),
            entry("pubchem.ftp.author", "", bioregistry_prefix="pubchem"),
            entry("x", "https://c.org/sparql", kg_registry_id="kg-x"),
            entry("y", "https://d.org/sparql", kg_registry_id="kg-x"),
        ]
    )
    assert found == {
        ("chembl", "chembl.bigcat"): ("distribution_of", "candidate"),
        ("x", "y"): ("distribution_of", "candidate"),
    }


def test_overrides_decide_pairs_and_group_aliases(tmp_path):
    path = tmp_path / "overrides.yaml"
    path.write_text(
        yaml.safe_dump(
            [
                {"left": "a", "right": "b", "relation": "same_dataset", "note": "mirror"},
                {"left": "b", "right": "c", "relation": "same_dataset"},
            ]
        )
    )
    overrides = read_overrides(path)
    result = resolve_identity(
        [
            entry("a", graphs=["urn:g1"]),
            entry("b", graphs=["urn:g2"]),
            entry("c", "https://x.org/sparql"),
            entry("d", "https://y.org/sparql"),
        ],
        overrides,
    )
    assert result.datasets == {"a": ["a", "b", "c"], "d": ["d"]}
    assert all(item.decided_by == "override" for item in result.relations)
    assert result.candidates == []


def test_overrides_must_name_registry_entries():
    unknown = IdentityRelation(
        left="a",
        right="missing",
        relation="distinct",
        basis="curated override",
        decided_by="override",
    )
    with pytest.raises(ValueError, match="missing"):
        resolve_identity([entry("a")], [unknown])


def test_catalogs_come_from_names_and_hosts():
    assert catalogs("rdfportal.chembl", "https://rdfportal.org/ebi/sparql") == ["rdfportal"]
    assert catalogs("chebi", "https://idsm.elixir-czech.cz/sparql/endpoint/chebi") == ["idsm"]
    assert catalogs("pubchem.ftp.gene", "") == ["pubchem_ftp"]
    assert catalogs("rhea", "https://sparql.rhea-db.org/sparql") == []


def test_downloads_are_distributions():
    result = resolve_identity(
        [entry("a", download_nt=["https://x.org/a.nt"], local_tar_url="https://x.org/a.tar")]
    )
    assert result.entries[0].distributions == ["https://x.org/a.nt", "https://x.org/a.tar"]


def test_cli_writes_identity_and_review_table(tmp_path):
    sources = tmp_path / "sources.yaml"
    sources.write_text(
        yaml.safe_dump([entry("same1"), entry("same2"), entry("other", "https://o.org/sparql")])
    )
    output = tmp_path / "identity"
    result = CliRunner().invoke(
        main, ["registry", "identity", "--sources", str(sources), "--output", str(output)]
    )
    assert result.exit_code == 0, result.output
    assert "3 registry entries, 3 datasets, 1 candidate" in result.output
    assert json.loads((output / "identity.json").read_text())["datasets"]["other"] == ["other"]
    with (output / "identity_review.tsv").open() as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))
    assert rows == [
        {
            "left": "same1",
            "right": "same2",
            "candidate_relation": "same_dataset",
            "basis": "same endpoint and graph set",
        }
    ]
