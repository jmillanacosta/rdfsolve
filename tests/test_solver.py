"""Tests for the QuerySolver and compact session."""

from __future__ import annotations

import pytest

from rdfsolve.solver import (
    CompactObservation,
    Decision,
    DecisionOption,
    Intent,
    QuerySolver,
    Requirement,
    RequirementStatus,
    SolverState,
)


class TestSolverState:
    """Test solver state enums."""

    def test_state_values(self) -> None:
        assert SolverState.INIT.value == "init"
        assert SolverState.CHOOSE.value == "choose"
        assert SolverState.READY.value == "ready"
        assert SolverState.COMPLETE.value == "complete"
        assert SolverState.BLOCKED.value == "blocked"

    def test_requirement_status(self) -> None:
        assert RequirementStatus.PENDING.value == "pending"
        assert RequirementStatus.RESOLVED.value == "resolved"
        assert RequirementStatus.UNSUPPORTED.value == "unsupported"


class TestIntent:
    """Test Intent model."""

    def test_empty_intent(self) -> None:
        intent = Intent(question="test")
        assert intent.question == "test"
        assert intent.source_class is None
        assert intent.target_classes == []
        assert intent.filters == []

    def test_full_intent(self) -> None:
        intent = Intent(
            question="Find chemicals affecting thyroid",
            source_class="http://example.org/Chemical",
            target_classes=["http://example.org/AdverseOutcome"],
            filters=[{"kind": "Chemical", "terms": ["thyroid"]}],
            selection="Chemicals with thyroid effects",
        )
        assert intent.source_class == "http://example.org/Chemical"
        assert len(intent.target_classes) == 1
        assert len(intent.filters) == 1


class TestRequirement:
    """Test Requirement dataclass."""

    def test_requirement_creation(self) -> None:
        req = Requirement(
            id="req-01",
            clause="Source class: Chemical",
            kind="source",
            status=RequirementStatus.RESOLVED,
            bindings={"class": "http://example.org/Chemical"},
        )
        assert req.id == "req-01"
        assert req.status == RequirementStatus.RESOLVED
        assert req.bindings["class"] == "http://example.org/Chemical"

    def test_pending_requirement(self) -> None:
        req = Requirement(
            id="req-02",
            clause="Target: Outcome",
            kind="target",
        )
        assert req.status == RequirementStatus.PENDING
        assert req.reason is None


class TestDecision:
    """Test Decision dataclass."""

    def test_decision_creation(self) -> None:
        options = [
            DecisionOption(id="opt-01", meaning="Direct path", route_id="route-abc"),
            DecisionOption(id="opt-02", meaning="Via intermediate", route_id="route-def"),
        ]
        decision = Decision(
            id="d-01",
            clause="Route to Outcome",
            kind="route_choice",
            options=options,
            more_available=True,
        )
        assert decision.id == "d-01"
        assert len(decision.options) == 2
        assert decision.selected is None
        assert decision.more_available

    def test_decision_selection(self) -> None:
        options = [DecisionOption(id="opt-01", meaning="Test")]
        decision = Decision(
            id="d-01",
            clause="Test",
            kind="route_choice",
            options=options,
        )
        decision.selected = "opt-01"
        assert decision.selected == "opt-01"


class TestCompactObservation:
    """Test CompactObservation model."""

    def test_init_observation(self) -> None:
        obs = CompactObservation(
            plan_id="abc123",
            revision=0,
            state=SolverState.INIT,
        )
        assert obs.plan_id == "abc123"
        assert obs.revision == 0
        assert obs.state == SolverState.INIT
        assert obs.decision is None
        assert obs.reference is None

    def test_decision_observation(self) -> None:
        decision = Decision(
            id="d-01",
            clause="Test",
            kind="route_choice",
            options=[DecisionOption(id="opt-01", meaning="Test")],
        )
        obs = CompactObservation(
            plan_id="abc123",
            revision=1,
            state=SolverState.CHOOSE,
            decision=decision,
        )
        assert obs.state == SolverState.CHOOSE
        assert obs.decision is not None
        assert obs.decision.id == "d-01"

    def test_complete_observation(self) -> None:
        obs = CompactObservation(
            plan_id="abc123",
            revision=3,
            state=SolverState.COMPLETE,
            reference="result-xyz",
            execution="complete",
            rows=42,
            meaning="Test query",
        )
        assert obs.state == SolverState.COMPLETE
        assert obs.reference == "result-xyz"
        assert obs.rows == 42


class TestQuerySolverBasics:
    """Test QuerySolver without client."""

    def test_solver_init(self) -> None:
        # Create a minimal mock client
        class MockClient:
            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

            def paths_between(self, *args, **kwargs):
                import pandas as pd

                df = pd.DataFrame()
                df.attrs["routes"] = []
                return df

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        assert solver.state == SolverState.INIT
        assert solver.revision == 0
        assert len(solver.plan_id) == 8

    def test_solver_interpret(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

            def paths_between(self, *args, **kwargs):
                import pandas as pd

                df = pd.DataFrame()
                df.attrs["routes"] = []
                return df

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        intent = Intent(
            question="Test query",
            source_class="http://example.org/Source",
            target_classes=["http://example.org/Target"],
            selection="Find targets",
        )

        obs = solver.interpret(intent)

        assert solver.revision == 1
        # With no routes found, target becomes unsupported
        assert solver.state in (
            SolverState.READY,
            SolverState.BLOCKED,
            SolverState.INTERPRETING,
        )

    def test_solver_log(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

            def paths_between(self, *args, **kwargs):
                import pandas as pd

                df = pd.DataFrame()
                df.attrs["routes"] = []
                return df

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        intent = Intent(
            question="Test",
            source_class="http://example.org/Source",
            target_classes=[],
            selection="Test",
        )
        solver.interpret(intent)

        log = solver.get_log()
        assert len(log) > 0
        assert log[0]["action"] == "interpret"
        assert "plan_id" in log[0]

    def test_solver_export_state(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

            def paths_between(self, *args, **kwargs):
                import pandas as pd

                df = pd.DataFrame()
                df.attrs["routes"] = []
                return df

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        state = solver.export_state()
        assert "plan_id" in state
        assert "revision" in state
        assert "state" in state
        assert "requirements" in state
        assert "decisions" in state


class TestQuerySolverDecisions:
    """Test decision handling."""

    def test_invalid_decision_id(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        with pytest.raises(ValueError, match="Unknown decision"):
            solver.choose("invalid-id", "opt-01")

    def test_invalid_option_id(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        # Manually create a decision
        decision = Decision(
            id="d-00",
            clause="Test",
            kind="route_choice",
            options=[DecisionOption(id="opt-00", meaning="Test")],
        )
        solver._decisions["d-00"] = decision
        solver._current_decision = "d-00"

        with pytest.raises(ValueError, match="Invalid option"):
            solver.choose("d-00", "wrong-option")

    def test_stale_decision(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        # Create two decisions
        d1 = Decision(id="d-00", clause="Test1", kind="route_choice", options=[])
        d2 = Decision(id="d-01", clause="Test2", kind="route_choice", options=[])
        solver._decisions["d-00"] = d1
        solver._decisions["d-01"] = d2
        solver._current_decision = "d-01"  # d-01 is current

        with pytest.raises(ValueError, match="not current"):
            solver.choose("d-00", "opt-01")  # Try to choose on stale decision


class TestQuerySolverExecution:
    """Test execution state."""

    def test_execute_wrong_state(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")

        with pytest.raises(ValueError, match="Cannot execute"):
            solver.execute()

    def test_execute_ready_state(self) -> None:
        class MockClient:
            _schema = type(
                "Schema",
                (),
                {"enrichment": type("E", (), {"labels": [], "description": lambda s, x: ""})()},
            )()

            def registry(self, source_id: str) -> "MockRegistry":
                return MockRegistry()

        class MockRegistry:
            revision = "test-rev"
            types = []

        client = MockClient()
        solver = QuerySolver(client, source_id="test")
        solver._state = SolverState.READY

        obs = solver.execute()
        assert obs.state == SolverState.COMPLETE
        assert obs.reference is not None


class TestEmptyResults:
    """Test handling of empty results - Milestone A1."""

    def test_empty_result_is_valid(self) -> None:
        """A structurally complete query returning zero rows must be accepted."""
        obs = CompactObservation(
            plan_id="test",
            revision=1,
            state=SolverState.COMPLETE,
            reference="result-123",
            execution="complete",
            rows=0,  # Zero rows is valid
            meaning="Query with no matches",
        )
        assert obs.rows == 0
        assert obs.execution == "complete"

    def test_empty_vs_failed(self) -> None:
        """Distinguish empty success from failure."""
        success_empty = CompactObservation(
            plan_id="test",
            revision=1,
            state=SolverState.COMPLETE,
            execution="complete",
            rows=0,
        )

        failed = CompactObservation(
            plan_id="test",
            revision=1,
            state=SolverState.FAILED,
            unresolved=["Could not find route"],
        )

        assert success_empty.state == SolverState.COMPLETE
        assert failed.state == SolverState.FAILED
        assert success_empty.state != failed.state
