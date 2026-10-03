"""SPARQL endpoint health checking, status tracking, and rate limiting."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from rdfsolve.models.source_model import SourceModel
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    SparqlHelper,
)

__all__ = [
    "EndpointHealthCheck",
    "check_endpoint_health",
    "get_polite_delay",
    "update_endpoint_status",
]


@dataclass
class EndpointHealthCheck:
    """Result of endpoint health check."""

    endpoint_url: str
    status: str  # "up", "down", "timeout", "rate_limited"
    response_time: float | None  # seconds
    error_message: str
    timestamp: str


# Default delays for different endpoint types (seconds)
DEFAULT_DELAYS = {
    "public": 2.0,  # Public SPARQL endpoints need Delays
    "local": 0.0,  # Local QLever - no delay needed
    "institutional": 1.0,  # University/research institute endpoints
}

# Timeout for health checks (shorter than mining timeout)
HEALTH_CHECK_TIMEOUT = 1.0

# Simple ASK query for health check
HEALTH_CHECK_QUERY = "ASK WHERE { ?s ?p ?o }"


def check_endpoint_health(
    endpoint_url: str,
    timeout: float = HEALTH_CHECK_TIMEOUT,
) -> EndpointHealthCheck:
    """Check SPARQL endpoint health with simple ASK query.

    Args:
        endpoint_url: SPARQL endpoint URL.
        timeout: Request timeout in seconds.

    Returns:
        EndpointHealthCheck with status and response time.
    """
    helper = SparqlHelper(
        endpoint_url=endpoint_url,
        timeout=timeout,
        max_retries=1,  # Single attempt for health check
    )

    timestamp = datetime.now(timezone.utc).isoformat()
    start_time = time.time()

    try:
        # Try a simple ASK query
        helper.ask(HEALTH_CHECK_QUERY)
        response_time = time.time() - start_time

        return EndpointHealthCheck(
            endpoint_url=endpoint_url,
            status="up",
            response_time=response_time,
            error_message="",
            timestamp=timestamp,
        )

    except EndpointTimeoutError as e:
        response_time = time.time() - start_time
        return EndpointHealthCheck(
            endpoint_url=endpoint_url,
            status="timeout",
            response_time=response_time,
            error_message=str(e),
            timestamp=timestamp,
        )

    except EndpointError as e:
        # Check for rate limiting indicators
        error_str = str(e).lower()
        if any(pattern in error_str for pattern in ["429", "rate limit", "too many requests"]):
            status = "rate_limited"
        else:
            status = "down"

        response_time = time.time() - start_time
        return EndpointHealthCheck(
            endpoint_url=endpoint_url,
            status=status,
            response_time=response_time,
            error_message=str(e)[:500],  # Truncate long errors
            timestamp=timestamp,
        )

    except (ConnectionError, OSError, ValueError) as e:
        response_time = time.time() - start_time
        return EndpointHealthCheck(
            endpoint_url=endpoint_url,
            status="down",
            response_time=response_time,
            error_message=str(e)[:500],
            timestamp=timestamp,
        )

    finally:
        helper.close()


def update_endpoint_status(
    source: SourceModel,
    health: EndpointHealthCheck,
) -> SourceModel:
    """Update source model with health check results.

    Args:
        source: SourceModel to update.
        health: Health check result.

    Returns:
        Updated SourceModel.
    """
    source.endpoint_status = health.status
    source.last_checked = health.timestamp

    if health.status == "up":
        source.last_success = health.timestamp
        source.failure_count = 0
        source.endpoint_down = False
    else:
        source.failure_count += 1
        source.last_error = health.error_message
        if source.failure_count >= 3:
            source.endpoint_down = True

    if health.response_time is not None:
        # Exponential moving average
        if source.avg_response_time is None:
            source.avg_response_time = health.response_time
        else:
            alpha = 0.3  # Weight for new measurement
            source.avg_response_time = (
                alpha * health.response_time + (1 - alpha) * source.avg_response_time
            )

    return source


def get_polite_delay(source: SourceModel) -> float:
    """Get inter-request delay for source based on status and configuration.

    Args:
        source: SourceModel to check.

    Returns:
        Delay in seconds.
    """
    # Use explicit delay if configured
    if source.delay is not None:
        return source.delay

    # A download provider does not make a remote endpoint local.
    if not source.endpoint:
        return DEFAULT_DELAYS["local"]

    # Rate limited endpoints need longer delays
    if source.endpoint_status == "rate_limited":
        return 5.0

    # Slow endpoints need more time between requests
    if source.avg_response_time and source.avg_response_time > 10.0:
        return 3.0

    # Check endpoint type by domain
    endpoint_lower = source.endpoint.lower()

    # Local QLever instances
    if "localhost" in endpoint_lower or "127.0.0.1" in endpoint_lower:
        return DEFAULT_DELAYS["local"]

    # Institutional endpoints
    if any(domain in endpoint_lower for domain in [".edu", ".ac.", ".uni-", "rdfportal.org"]):
        return DEFAULT_DELAYS["institutional"]

    # Default to public endpoint delay
    return DEFAULT_DELAYS["public"]
