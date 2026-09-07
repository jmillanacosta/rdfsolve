"""Parse every mining query builder with each supported graph scope."""

import inspect
from unittest.mock import Mock

import pytest
from rdflib.plugins.sparql.parser import parseQuery

from rdfsolve.mining import _query_owl_class_superclasses
from rdfsolve.mining import query_builders as builders
from rdfsolve.mining.metadata_mining import MetadataMiner
from rdfsolve.mining.ontology_as_data import (
    detect_ontology_as_data,
    mine_ontology_as_data_patterns,
    mine_ontology_as_data_subject_patterns,
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
    helper.select.return_value = {"results": {"bindings": []}}
    _query_owl_class_superclasses(helper, scope)
    detect_ontology_as_data(helper, scope)
    mine_ontology_as_data_patterns(helper, scope, superclasses=["urn:A"])
    mine_ontology_as_data_subject_patterns(helper, scope, superclasses=["urn:A"])
    OntologyMiner(helper, scope).mine()
    MetadataMiner(helper, scope).mine()
    assert helper.select.call_count == 17
    for call in helper.select.call_args_list:
        parseQuery(call.args[0])
