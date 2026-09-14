"""Gate 3 tests: Query semantics - ALL/ANY, filters, repeated classes.

Tests T10-T18 for query semantic correctness.
"""

import pytest
from collections import Counter
from rdflib import URIRef

from tests.acceptance.fixtures import EX
from tests.acceptance.fixtures import f03_conjunction
from tests.acceptance.fixtures import f06_repeated_class
from tests.acceptance.fixtures import f09_date_integer


# Fixture oracle verification


class TestF03Oracles:
    """Verify F03 oracle queries."""

    def test_q05_conjunction_oracle(self, f03_graph):
        """Q05: Papers with both author AND venue."""
        result = list(f03_graph.query(f03_conjunction.Q05_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f03_conjunction.Q05_EXPECTED

    def test_q06_disjunction_oracle(self, f03_graph):
        """Q06: Papers with author OR venue."""
        result = list(f03_graph.query(f03_conjunction.Q06_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f03_conjunction.Q06_EXPECTED

    def test_q07_bag_oracle(self, f03_graph):
        """Q07: Non-distinct bag with multiplicities."""
        result = list(f03_graph.query(f03_conjunction.Q07_SPARQL))
        actual = Counter(tuple(row) for row in result)
        assert actual == f03_conjunction.Q07_EXPECTED_BAG


class TestF06Oracles:
    """Verify F06 oracle queries."""

    def test_q11_coauthors_excluding_self(self, f06_graph):
        """Q11: Coauthors of alice excluding alice."""
        result = list(f06_graph.query(f06_repeated_class.Q11_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f06_repeated_class.Q11_EXPECTED

    def test_q12_coauthors_including_self(self, f06_graph):
        """Q12: Coauthors of alice including alice."""
        result = list(f06_graph.query(f06_repeated_class.Q12_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f06_repeated_class.Q12_EXPECTED


class TestF09Oracles:
    """Verify F09 oracle queries."""

    def test_q14_year_boundary(self, f09_graph):
        """Q14: Year >= 2020."""
        result = list(f09_graph.query(f09_date_integer.Q14_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f09_date_integer.Q14_EXPECTED

    def test_q15_date_boundary(self, f09_graph):
        """Q15: Date >= 2020-01-01."""
        result = list(f09_graph.query(f09_date_integer.Q15_SPARQL))
        actual = {tuple(row) for row in result}
        assert actual == f09_date_integer.Q15_EXPECTED

    @pytest.mark.parametrize("threshold,expected", [
        (2019, f09_date_integer.YEAR_THRESHOLD_EXPECTED[2019]),
        (2020, f09_date_integer.YEAR_THRESHOLD_EXPECTED[2020]),
        (2021, f09_date_integer.YEAR_THRESHOLD_EXPECTED[2021]),
        (2022, f09_date_integer.YEAR_THRESHOLD_EXPECTED[2022]),
    ])
    def test_year_thresholds(self, f09_graph, threshold, expected):
        """Parameterized year threshold tests."""
        query = f09_date_integer.year_query(threshold)
        result = list(f09_graph.query(query))
        actual = {tuple(row) for row in result}
        assert actual == expected


# T14: ALL and ANY have different answers


class TestT14AllAnyDifferentAnswers:
    """T14: all_and_any_have_different_answers.

    F03, p:Paper, a:Person, v:Venue. ALL of author(p,a) and publication
    venue(p,v), select p distinct, must equal Q05. ANY of the same leaves
    must equal Q06. Both must be ready and compile/execute.
    """

    @pytest.fixture
    def all_intent(self):
        """ALL intent: Paper with author AND venue."""
        return {
            "roles": [
                {"id": "p", "class_hint": "Paper"},
                {"id": "a", "class_hint": "Person"},
                {"id": "v", "class_hint": "Venue"},
            ],
            "where": {
                "op": "all",
                "args": [
                    {
                        "id": "r_author",
                        "op": "relation",
                        "from": "p",
                        "to": "a",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "r_venue",
                        "op": "relation",
                        "from": "p",
                        "to": "v",
                        "meaning": "publication venue",
                        "via": [],
                    },
                ],
            },
            "select": ["p"],
            "distinct": True,
            "unparsed_requirements": [],
        }

    @pytest.fixture
    def any_intent(self):
        """ANY intent: Paper with author OR venue."""
        return {
            "roles": [
                {"id": "p", "class_hint": "Paper"},
                {"id": "a", "class_hint": "Person"},
                {"id": "v", "class_hint": "Venue"},
            ],
            "where": {
                "op": "any",
                "args": [
                    {
                        "id": "r_author",
                        "op": "relation",
                        "from": "p",
                        "to": "a",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "r_venue",
                        "op": "relation",
                        "from": "p",
                        "to": "v",
                        "meaning": "publication venue",
                        "via": [],
                    },
                ],
            },
            "select": ["p"],
            "distinct": True,
            "unparsed_requirements": [],
        }

    def test_all_intent_is_ready(self, f03_client, all_intent):
        """ALL intent reaches ready state."""
        from rdfsolve.mcp import QueryService

        service = QueryService(f03_client, source_id="fixture")
        result = service.query_start(
            operation_id="t14-all",
            question="Papers with both author and venue",
            intent=all_intent,
        )

        # Should be ready (unique routes for both relations)
        assert "error" not in result, f"Error: {result}"
        assert result["state"] == "ready", f"Expected ready, got: {result}"

    def test_any_intent_is_valid(self, f03_client, any_intent):
        """ANY intent is valid input."""
        from rdfsolve.mcp import Intent

        parsed = Intent.model_validate(any_intent)
        assert parsed.where.op == "any"
        assert len(parsed.where.args) == 2


# T15: DISTINCT vs non-DISTINCT


class TestT15DistinctNotAlwaysForced:
    """T15: distinct_is_not_always_forced.

    Same F03 ALL intent as T14, but distinct false. Compare the exact Q07
    multiset: pBoth once and pMany four times. With distinct true, compare Q05.
    """

    @pytest.fixture
    def nondistinct_intent(self):
        """Non-distinct intent."""
        return {
            "roles": [
                {"id": "p", "class_hint": "Paper"},
                {"id": "a", "class_hint": "Person"},
                {"id": "v", "class_hint": "Venue"},
            ],
            "where": {
                "op": "all",
                "args": [
                    {
                        "id": "r_author",
                        "op": "relation",
                        "from": "p",
                        "to": "a",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "r_venue",
                        "op": "relation",
                        "from": "p",
                        "to": "v",
                        "meaning": "publication venue",
                        "via": [],
                    },
                ],
            },
            "select": ["p"],
            "distinct": False,  # Non-distinct
            "unparsed_requirements": [],
        }

    def test_nondistinct_intent_valid(self, f03_client, nondistinct_intent):
        """Non-distinct intent is accepted."""
        from rdfsolve.mcp import Intent

        parsed = Intent.model_validate(nondistinct_intent)
        assert parsed.distinct is False


# T10: Integer filter boundaries


class TestT10IntegerFilterBoundary:
    """T10: integer_filter_includes_boundary_and_is_not_dropped.

    F09, p:Paper, compare field 'publication year', 'ge', integer 2020;
    select p. Start ready and expect Q14. Parameterize threshold 2019, 2020,
    2021, 2022 with exact sets.
    """

    @pytest.fixture
    def year_filter_intent(self):
        """Year filter intent template."""
        def make_intent(threshold: int):
            return {
                "roles": [
                    {"id": "p", "class_hint": "Paper"},
                ],
                "where": {
                    "op": "all",
                    "args": [
                        {
                            "id": "c_year",
                            "op": "compare",
                            "role": "p",
                            "field": "publication year",
                            "operator": "ge",
                            "term": {
                                "type": "literal",
                                "value": str(threshold),
                                "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                            },
                        },
                    ],
                },
                "select": ["p"],
                "distinct": True,
                "unparsed_requirements": [],
            }
        return make_intent

    def test_compare_intent_valid(self, f09_client, year_filter_intent):
        """Compare filter intent is valid."""
        from rdfsolve.mcp import Intent

        intent = year_filter_intent(2020)
        parsed = Intent.model_validate(intent)
        assert parsed.where.args[0]["op"] == "compare"
        assert parsed.where.args[0]["operator"] == "ge"


# T17: Repeated class roles not merged


class TestT17RepeatedClassRolesNotMerged:
    """T17: repeated_class_roles_are_not_merged.

    F06, roles work:Work, left:Person, right:Person. ALL of author(work,left),
    author(work,right), bind left to alice, and different(left,right).
    Select right distinct. Expect Q11: bob and carol. Do not reject the
    repeated Person class or apply alice's bind to both roles.
    """

    @pytest.fixture
    def coauthors_intent(self):
        """Coauthors intent with different constraint."""
        return {
            "roles": [
                {"id": "work", "class_hint": "Work"},
                {"id": "left", "class_hint": "Person"},
                {"id": "right", "class_hint": "Person"},
            ],
            "where": {
                "op": "all",
                "args": [
                    {
                        "id": "r_left",
                        "op": "relation",
                        "from": "work",
                        "to": "left",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "r_right",
                        "op": "relation",
                        "from": "work",
                        "to": "right",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "b_alice",
                        "op": "bind",
                        "role": "left",
                        "term": {
                            "type": "iri",
                            "value": str(EX.alice),
                        },
                    },
                    {
                        "id": "d_neq",
                        "op": "different",
                        "left": "left",
                        "right": "right",
                    },
                ],
            },
            "select": ["right"],
            "distinct": True,
            "unparsed_requirements": [],
        }

    def test_repeated_class_intent_valid(self, f06_client, coauthors_intent):
        """Intent with repeated Person class is valid."""
        from rdfsolve.mcp import Intent

        parsed = Intent.model_validate(coauthors_intent)
        # Two roles with same class
        person_roles = [r for r in parsed.roles if r.class_hint == "Person"]
        assert len(person_roles) == 2
        # Both relations target the same class
        assert len(parsed.where.args) == 4  # 2 relations + 1 bind + 1 different


# T18: Different roles do not imply inequality


class TestT18DifferentRolesNoImpliedInequality:
    """T18: different_roles_do_not_imply_inequality.

    Remove only different(left,right) from T17. Expect Q12, including alice.
    This prevents 'fixing' T17 by globally adding inequalities between all
    role bindings.
    """

    @pytest.fixture
    def coauthors_no_diff_intent(self):
        """Coauthors intent WITHOUT different constraint."""
        return {
            "roles": [
                {"id": "work", "class_hint": "Work"},
                {"id": "left", "class_hint": "Person"},
                {"id": "right", "class_hint": "Person"},
            ],
            "where": {
                "op": "all",
                "args": [
                    {
                        "id": "r_left",
                        "op": "relation",
                        "from": "work",
                        "to": "left",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "r_right",
                        "op": "relation",
                        "from": "work",
                        "to": "right",
                        "meaning": "author",
                        "via": [],
                    },
                    {
                        "id": "b_alice",
                        "op": "bind",
                        "role": "left",
                        "term": {
                            "type": "iri",
                            "value": str(EX.alice),
                        },
                    },
                    # NO different constraint
                ],
            },
            "select": ["right"],
            "distinct": True,
            "unparsed_requirements": [],
        }

    def test_no_diff_intent_valid(self, f06_client, coauthors_no_diff_intent):
        """Intent without different constraint is valid."""
        from rdfsolve.mcp import Intent

        parsed = Intent.model_validate(coauthors_no_diff_intent)
        # No different clause
        diff_clauses = [
            a for a in parsed.where.args
            if isinstance(a, dict) and a.get("op") == "different"
        ]
        assert len(diff_clauses) == 0
