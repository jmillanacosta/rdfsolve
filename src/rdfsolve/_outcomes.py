"""Query results with explicit completion and failure information."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

QueryState = Literal["complete", "partial", "failed"]
FailureCategory = Literal["endpoint", "timeout", "truncated", "query", "invalid_response"]
Bindings = list[dict[str, Any]]


@dataclass
class QueryFailure:
    """Record an unresolved failure in its requested scope."""

    category: FailureCategory
    message: str
    purpose: str
    classes: list[str] = field(default_factory=list)
    graph_uris: list[str] | None = None


@dataclass
class QueryOutcome:
    """Keep observed rows separate from completion state."""

    rows: Bindings = field(default_factory=list)
    state: QueryState = "complete"
    failures: list[QueryFailure] = field(default_factory=list)

    def merge(self, other: QueryOutcome) -> QueryOutcome:
        """Combine independent query groups, including successful empty groups."""
        state: QueryState = self.state if self.state == other.state else "partial"
        return QueryOutcome(self.rows + other.rows, state, self.failures + other.failures)
