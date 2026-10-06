"""Query results with explicit completion and failure information."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

QueryState = Literal["complete", "partial", "failed"]
FailureCategory = Literal[
    "endpoint",
    "timeout",
    "truncated",
    "query",
    "invalid_response",
    "sampled",
    "rate_limited",
]
# "sampled": rows come from bounded member windows because the whole query exceeded its budget.
# "rate_limited": the endpoint asked for a pause longer than the wait budget of the client.
Bindings = list[dict[str, Any]]
# The key of a result row that a sampled query answered (rdfsolve.mining.sampling); its value is
# the provenance of the sample (QuerySample.provenance). Not a SPARQL variable.
SAMPLE_KEY = "_sampled"


@dataclass
class QueryFailure:
    """Record an unresolved failure in its requested scope."""

    category: FailureCategory
    message: str
    purpose: str
    classes: list[str] = field(default_factory=list)
    graph_uris: list[str] | None = None


@dataclass
class QuerySample:
    """A refused query asked again over a bounded sample, which the endpoint answered.

    The rows of the sample stand: each pattern they show exists, and each count they give is a
    lower bound (the sample holds *size* of the *unit*: edges, members, subjects or terms).
    *reason* is the category of the refusal of the whole query and *message* its text. A sample
    is not a failure: the item is reported as sampled, not missing.
    """

    purpose: str
    size: int
    unit: str
    reason: FailureCategory
    message: str
    classes: list[str] = field(default_factory=list)
    property_uri: str | None = None
    graph_uris: list[str] | None = None

    def provenance(self) -> dict[str, Any]:
        """Return the provenance of the rows of the sample: size, unit and refusal."""
        return {
            "size": self.size,
            "unit": self.unit,
            "reason": f"{self.reason}: {self.message[:200]}",
        }


@dataclass
class QueryOutcome:
    """Keep observed rows separate from completion state."""

    rows: Bindings = field(default_factory=list)
    state: QueryState = "complete"
    failures: list[QueryFailure] = field(default_factory=list)
    # Measures of complete rows that the engine refused (such as the distinct subjects of one
    # object group): the rows stand, and each gap is reported with its reason.
    gaps: list[QueryFailure] = field(default_factory=list)
    # Refused queries answered over a bounded sample: their rows carry SAMPLE_KEY.
    samples: list[QuerySample] = field(default_factory=list)

    def merge(self, other: QueryOutcome) -> QueryOutcome:
        """Combine independent query groups, including successful empty groups."""
        state: QueryState = self.state if self.state == other.state else "partial"
        return QueryOutcome(
            self.rows + other.rows,
            state,
            self.failures + other.failures,
            self.gaps + other.gaps,
            self.samples + other.samples,
        )
