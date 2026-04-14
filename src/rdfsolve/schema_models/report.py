"""Mining analytics report models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class QueryStats(BaseModel):
    """Cumulative statistics for one query category."""

    sent: int = Field(0, ge=0, description="Queries sent")
    failed: int = Field(
        0,
        ge=0,
        description="Queries that failed",
    )
    total_time_s: float = Field(
        0.0,
        ge=0,
        description="Wall-clock seconds for this category",
    )

    model_config = ConfigDict(extra="forbid")


class OneShotQueryResult(BaseModel):
    """Outcome of a single unbounded SELECT against a SPARQL endpoint.

    Used to record the raw performance of an unguarded one-shot
    query so it can be compared against the fallback-chain result.
    """

    query_type: str = Field(
        ...,
        description=("Pattern type queried: 'typed-object', 'literal', or 'untyped-uri'"),
    )
    success: bool = Field(
        ...,
        description="True if the endpoint returned a result set",
    )
    duration_s: float | None = Field(
        None,
        ge=0,
        description="Wall-clock seconds for the single HTTP call",
    )
    row_count: int | None = Field(
        None,
        ge=0,
        description="Number of result rows returned",
    )
    error: str | None = Field(
        None,
        description="Exception message if the query failed",
    )

    model_config = ConfigDict(extra="forbid")


class PhaseReport(BaseModel):
    """Timing and outcome for one mining phase."""

    name: str = Field(..., description="Phase identifier")
    started_at: str | None = Field(
        None,
        description="ISO-8601 start time",
    )
    finished_at: str | None = Field(
        None,
        description="ISO-8601 finish time",
    )
    duration_s: float | None = Field(
        None,
        ge=0,
        description="Wall-clock seconds",
    )
    items_discovered: int = Field(
        0,
        ge=0,
        description="Number of items produced by this phase",
    )
    error: str | None = Field(
        None,
        description="Error message if the phase failed",
    )

    model_config = ConfigDict(extra="forbid")


class MiningReport(BaseModel):
    """Analytical metadata collected during a mining run.

    Designed to be written to disk incrementally (after each phase
    completes) so that partial data is preserved even if mining
    crashes midway.
    """

    # Identification
    dataset_name: str | None = Field(
        None,
        description="Human-readable name of the mined dataset",
    )
    endpoint_url: str = Field(
        ...,
        description="SPARQL endpoint URL",
    )
    graph_uris: list[str] | None = Field(
        None,
        description="Named-graph URIs (if any)",
    )
    strategy: str = Field(
        "unknown",
        description=("Mining strategy: 'miner' or 'miner/two-phase'"),
    )

    # Versions & environment
    rdfsolve_version: str = Field(
        ...,
        description="Package version string",
    )
    python_version: str = Field(
        ...,
        description="Python interpreter version",
    )
    qlever_version: dict[str, str] | None = Field(
        None,
        description=(
            "QLever build info fetched from the endpoint's "
            '?cmd=stats: {"git_hash_server": str, '
            '"git_hash_index": str}'
        ),
    )

    # Timing
    started_at: str = Field(
        ...,
        description="ISO-8601 timestamp when mining started",
    )
    finished_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when mining finished",
    )
    total_duration_s: float | None = Field(
        None,
        ge=0,
        description="Total wall-clock seconds",
    )

    # Query statistics
    query_stats: dict[str, QueryStats] = Field(
        default_factory=dict,
        description="Per-purpose query statistics.",
    )
    total_queries_sent: int = Field(0, ge=0)
    total_queries_failed: int = Field(0, ge=0)

    # Phase breakdown
    phases: list[PhaseReport] = Field(default_factory=list)

    # Results summary
    abort_reason: str | None = Field(None)
    pattern_count: int = Field(0, ge=0)
    class_count: int = Field(0, ge=0)
    property_count: int = Field(0, ge=0)
    unique_uris_labelled: int = Field(0, ge=0)

    # Configuration snapshot
    config: dict[str, Any] = Field(default_factory=dict)

    # Benchmark / resource usage
    machine: dict[str, Any] | None = Field(None)
    benchmark: dict[str, Any] | None = Field(None)

    # One-shot baseline
    one_shot_results: list[OneShotQueryResult] | None = Field(
        None,
    )

    # Invalid URI patterns dropped during mining
    dropped_invalid_uris: int = Field(
        0,
        ge=0,
        description="Patterns dropped because subject/property/object "
        "contained non-URI values (e.g. unexpanded CURIEs).",
    )
    dropped_invalid_uri_samples: list[str] = Field(
        default_factory=list,
        description="First few examples of dropped invalid URIs.",
    )

    # Author provenance
    authors: list[dict[str, str]] | None = Field(None)

    # Captured endpoint metadata
    dataset_metadata: dict[str, Any] | None = Field(None)

    # Canonical URI
    report_uri: str | None = Field(None)

    model_config = ConfigDict(extra="allow")
