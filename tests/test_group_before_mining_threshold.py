"""Ontology terms are grouped before mining above the same number of classes as the term budget
(300 by default), so that one threshold applies to the grouping before and after mining."""

import sys

import pytest

from scripts.pipeline_stages import cli
from scripts.pipeline_stages.config import PipelineConfig


def _config(monkeypatch, *args):
    """Return the configuration that the command line builds, without mining."""
    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--ontology-as-data", *args])
    with pytest.raises(SystemExit):
        cli.main()
    return seen["config"]


def test_the_threshold_is_the_term_budget(monkeypatch):
    assert PipelineConfig().ontology_group_before_mining == PipelineConfig().ontology_term_budget == 300
    assert _config(monkeypatch).ontology_group_before_mining == 300
    assert _config(monkeypatch, "--ontology-term-budget", "50").ontology_group_before_mining == 50


def test_an_explicit_threshold_is_kept(monkeypatch):
    assert _config(monkeypatch, "--ontology-group-before-mining", "5000").ontology_group_before_mining == 5000
    assert _config(monkeypatch, "--ontology-group-before-mining", "0").ontology_group_before_mining is None
