"""Parse every mining query builder with each supported graph scope."""

import inspect
import json
from unittest.mock import Mock

import pytest
from rdflib import Dataset
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.mining import query_builders as builders
from rdfsolve.mining.metadata_mining import MetadataMiner
from rdfsolve.mining.ontology_as_data import (
    build_term_object_query,
    build_term_subject_query,
    fetch_superclasses,
)
from rdfsolve.mining.ontology_extraction import OntologyMiner


SCOPES = [None, ["urn:g"], ["urn:g", "urn:h"]]
BUILDERS = [getattr(builders, name) for name in dir(builders) if name.startswith("_build_")]


@pytest.mark.parametrize("scope", SCOPES)
@pytest.mark.parametrize("builder", BUILDERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("paged", [False, True])
def test_pattern_query_syntax(builder, scope, paged):
    values = {
        "graph_uris": scope,
        "uris": ["urn:A"],
        "class_uris": ["urn:A"],
        "subject_class": "urn:A",
        "class_uri": "urn:A",
        "property_uri": "urn:p",
        "prop_uri": "urn:p",
        "paginated": paged,
        "drop_distinct": paged,
    }
    kwargs = {
        name: values[name] for name in inspect.signature(builder).parameters if name in values
    }
    query = builder(**kwargs)
    if "{limit}" in query or "{offset}" in query:
        query = query.format(limit=10, offset=0)
    parseQuery(query)


@pytest.mark.parametrize("scope", SCOPES)
def test_optional_query_syntax(scope):
    helper = Mock()
    helper.select.side_effect = lambda query, **kwargs: json.loads(
        Dataset().query(query).serialize(format="json")
    )
    helper.endpoint_url = "https://example.org/sparql"
    helper.construct.side_effect = lambda query: Dataset().query(query).serialize(format="turtle").decode()
    fetch_superclasses(helper, ["urn:A"])
    # FROM clauses would make rdflib fetch the named graphs, so parse only.
    parseQuery(build_term_object_query(scope))
    parseQuery(build_term_subject_query(scope))
    OntologyMiner(helper, scope).mine()
    MetadataMiner(helper, scope).mine()
    for call in helper.select.call_args_list:
        parseQuery(call.args[0])


@pytest.mark.parametrize("scope", SCOPES)
def test_class_discovery_keeps_every_type_by_default(scope):
    assert "owl#Class" not in builders._build_class_discovery_query_plain(scope)
