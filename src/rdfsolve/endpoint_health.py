"""SPARQL endpoint health checking, status tracking, and rate limiting."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from rdfsolve.models.source_model import SourceModel, SourcesRegistry
from rdfsolve.sparql_helper import (
    EndpointError,
    EndpointTimeoutError,
    SparqlHelper,
)

logger = logging.getLogger(__name__)

__all__ = [
    "EndpointHealthCheck",
    "check_endpoint_health",
    "get_polite_delay",
    "health_check_all_endpoints",
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
HEALTH_CHECK_TIMEOUT = 3.0

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
    if source.delay is not None and source.delay > 0:
        return source.delay

    # Local sources (no endpoint or local provider) need no delay
    if not source.endpoint or source.local_provider:
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


def health_check_all_endpoints(
    sources_yaml: str | Path,
    save: bool = True,
) -> dict[str, EndpointHealthCheck]:
    """Check health of all endpoints and optionally save status to sources.yaml.

    Args:
        sources_yaml: Path to sources.yaml file.
        save: Save updated status to file if True.

    Returns:
        Dict mapping source name to health check result.
    """
    registry = SourcesRegistry.from_yaml(sources_yaml)
    results = {}

    # Only check sources with endpoints (skip local-only sources)
    sources_with_endpoints = [s for s in registry.sources if s.endpoint]

    logger.info(f"Checking health of {len(sources_with_endpoints)} endpoints...")

    for i, source in enumerate(sources_with_endpoints, 1):
        logger.info(f"[{i}/{len(sources_with_endpoints)}] {source.name}")

        health = check_endpoint_health(source.endpoint)
        results[source.name] = health

        logger.info(
            f"  Status: {health.status}, Response time: {health.response_time:.2f}s"
            if health.response_time
            else f"  Status: {health.status}"
        )

        # Update source model
        update_endpoint_status(source, health)

        # Delay between health checks
        time.sleep(0.5)

    if save:
        # Write back to YAML
        sources_yaml = Path(sources_yaml)
        with open(sources_yaml, encoding="utf-8") as fh:
            sources_data = yaml.safe_load(fh)

        # Update each source with health info
        by_name = {s["name"]: s for s in sources_data}
        for source in registry.sources:
            if source.name in by_name:
                by_name[source.name].update(
                    {
                        "endpoint_status": source.endpoint_status,
                        "last_checked": source.last_checked,
                        "last_success": source.last_success,
                        "last_error": source.last_error,
                        "failure_count": source.failure_count,
                        "endpoint_down": source.endpoint_down,
                        "avg_response_time": source.avg_response_time,
                    }
                )

        with open(sources_yaml, "w", encoding="utf-8") as fh:
            yaml.dump(
                sources_data,
                fh,
                default_flow_style=False,
                sort_keys=False,
                width=200,
                allow_unicode=True,
            )

        logger.info(f"Updated endpoint health status in {sources_yaml}")

    # Summary
    status_counts: dict[str, int] = {}
    for health in results.values():
        status_counts[health.status] = status_counts.get(health.status, 0) + 1

    logger.info("Health check summary:")
    for status, count in sorted(status_counts.items()):
        logger.info(f"  {status}: {count}")

    return results
