"""SPARQL endpoint registry models with health status and capabilities tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class EndpointStatus(str, Enum):
    """Health status of a SPARQL endpoint."""

    UNKNOWN = "unknown"
    """Status has not been checked."""

    HEALTHY = "healthy"
    """Endpoint is responding correctly to queries."""

    SLOW = "slow"
    """Endpoint responds but with high latency."""

    UNHEALTHY = "unhealthy"
    """Endpoint is not responding or returns errors."""

    UNREACHABLE = "unreachable"
    """Endpoint URL is not accessible (network/DNS error)."""

    TIMEOUT = "timeout"
    """Endpoint did not respond within timeout."""

    RATE_LIMITED = "rate_limited"
    """Endpoint is returning rate limit errors."""


class EndpointHealth(BaseModel):
    """Result of an endpoint health check.

    Captures the result of probing a SPARQL endpoint,
    including latency, error messages, and capabilities.
    """

    status: EndpointStatus = Field(
        default=EndpointStatus.UNKNOWN,
        description="Current health status",
    )
    checked_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the health check was performed",
    )
    latency_ms: float | None = Field(
        None,
        ge=0,
        description="Response latency in milliseconds",
    )
    error_message: str | None = Field(
        None,
        description="Error message if unhealthy",
    )

    # Capabilities discovered during health check
    supports_service_description: bool = Field(
        default=False,
        description="Whether endpoint exposes SPARQL Service Description",
    )
    supports_void: bool = Field(
        default=False,
        description="Whether endpoint has VoID metadata",
    )
    is_qlever: bool = Field(
        default=False,
        description="Whether endpoint is a QLever instance",
    )
    qlever_version: dict[str, str] | None = Field(
        None,
        description="QLever version info if available",
    )
    triple_count: int | None = Field(
        None,
        ge=0,
        description="Estimated triple count if available",
    )


class Endpoint(BaseModel):
    """SPARQL endpoint registry entry.

    Represents a SPARQL endpoint in the LOD cloud with its
    metadata, health status, and mining configuration.

    Example
    -------
    >>> endpoint = Endpoint(
    ...     name="uniprot",
    ...     url="https://sparql.uniprot.org/sparql",
    ...     description="UniProt protein database",
    ... )
    """

    # Identity ---
    name: str = Field(
        ...,
        description="Short identifier for the endpoint (e.g., 'uniprot')",
    )
    url: str = Field(
        ...,
        description="SPARQL endpoint URL",
    )
    description: str | None = Field(
        None,
        description="Human-readable description",
    )

    # Categorization ---
    domain: str | None = Field(
        None,
        description="Domain category (e.g., 'life_sciences', 'geography')",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Tags for filtering/grouping endpoints",
    )

    # Source metadata ---
    homepage: str | None = Field(
        None,
        description="Homepage URL for the dataset",
    )
    license_uri: str | None = Field(
        None,
        description="License URI for the data",
    )
    maintainer: str | None = Field(
        None,
        description="Maintainer contact or organization",
    )

    # Named graphs ---
    named_graphs: list[str] | None = Field(
        None,
        description="Named graph URIs to query (None = default graph)",
    )
    default_graph_uri: str | None = Field(
        None,
        description="Default graph URI if needed for queries",
    )

    # Health ---
    health: EndpointHealth = Field(
        default_factory=EndpointHealth,
        description="Latest health check result",
    )

    # Mining configuration ---
    mining_strategy: str = Field(
        default="auto",
        description="Preferred mining strategy: 'auto', 'qlever_oneshot', 'paginated', 'two_phase'",
    )
    query_timeout_s: int = Field(
        default=300,
        ge=1,
        description="Query timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for failed queries",
    )
    enabled: bool = Field(
        default=True,
        description="Whether this endpoint is enabled for mining",
    )

    # Timestamps ---
    added_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When endpoint was added to registry",
    )
    last_mined_at: datetime | None = Field(
        None,
        description="When endpoint was last successfully mined",
    )

    @property
    def is_healthy(self) -> bool:
        """Check if endpoint is in a healthy state."""
        return self.health.status in (EndpointStatus.HEALTHY, EndpointStatus.SLOW)


__all__ = [
    "Endpoint",
    "EndpointHealth",
    "EndpointStatus",
]
