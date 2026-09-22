"""Execute saved scientific checks and retain independent observations."""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

from rdfsolve.sparql_helper import EndpointTimeoutError, PaginationTruncatedError, SparqlHelper

from .scientific_validation import PatternSpotCheckPlan, RouteCheckPlan, ScientificValidationPlan


class ScientificCheckResult(BaseModel):
    """One observation and its comparison with the saved evidence."""

    check_id: str
    snapshot_id: str
    observed_at: str
    duration_s: float = 0
    state: Literal["matched", "no_match", "partial", "timeout", "error"] = "error"
    comparison: Literal["agrees", "differs", "not_comparable"] = "not_comparable"
    source_count: int | None = None
    matched_sources: int | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


class ScientificValidationResults(BaseModel):
    """Check observations tied to an exact plan and an explicit query target."""

    release_id: str
    plan_sha256: str
    dataset_id: str
    extraction_mode: str
    endpoint: str
    index_reference: str | None = None
    results: list[ScientificCheckResult] = Field(default_factory=list)


def _observe(
    check: PatternSpotCheckPlan | RouteCheckPlan, helper: SparqlHelper
) -> ScientificCheckResult:
    result = ScientificCheckResult(
        check_id=check.check_id,
        snapshot_id=check.snapshot_id,
        observed_at=datetime.now(timezone.utc).isoformat(),
    )
    started = time.monotonic()
    try:
        raw = helper.select(check.query, purpose=f"scientific-check/{check.check_id}")
        bindings = raw.get("results")
        rows = bindings.get("bindings") if isinstance(bindings, dict) else None
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("Expected SELECT bindings")
        if isinstance(check, PatternSpotCheckPlan):
            if len(rows) > 1 or any(
                not isinstance(row.get(key), dict) or not isinstance(row[key].get("value"), str)
                for row in rows
                for key in ("s", "o")
            ):
                raise ValueError("Expected at most one subject/object witness")
            result.state = "matched" if rows else "no_match"
            result.comparison = "agrees" if rows else "differs"
        else:
            if len(rows) != 1:
                raise ValueError("Expected one route aggregate row")
            sources = int(rows[0]["sources"]["value"])
            matched = int(rows[0]["matched"]["value"]) if sources else 0
            if sources < 0 or not 0 <= matched <= sources:
                raise ValueError("Invalid route support counts")
            result.source_count, result.matched_sources = sources, matched
            result.state = "matched" if matched else "no_match"
            if check.expected_instance_support in {"matched", "no_match"}:
                agrees = (
                    result.state == check.expected_instance_support
                    and (
                        check.expected_source_count is None
                        or sources == check.expected_source_count
                    )
                    and (
                        check.expected_matched_sources is None
                        or matched == check.expected_matched_sources
                    )
                )
                result.comparison = "agrees" if agrees else "differs"
        result.rows = rows
    except PaginationTruncatedError as error:
        result.state, result.error = "partial", str(error)
        result.rows = error.partial_rows
    except EndpointTimeoutError as error:
        result.state, result.error = "timeout", str(error)
    except Exception as error:
        result.state, result.error = "error", f"{type(error).__name__}: {error}"
    result.duration_s = time.monotonic() - started
    return result


def execute_scientific_validation_plan(
    plan: ScientificValidationPlan,
    helper: SparqlHelper,
    *,
    dataset_id: str,
    extraction_mode: Literal["remote", "local", "grouped"],
    index_reference: str | None = None,
) -> ScientificValidationResults:
    """Run one dataset/channel selection; local targets require an index reference.

    The caller supplies the frozen index identity. A later remote observation
    can differ because the endpoint changed; it does not invalidate the snapshot.
    """
    candidates: list[PatternSpotCheckPlan | RouteCheckPlan] = list(plan.pattern_checks)
    candidates.extend(plan.route_checks)
    checks = [
        check
        for check in candidates
        if check.dataset_id == dataset_id and check.extraction_mode == extraction_mode
    ]
    if not checks:
        raise ValueError("No checks for the selected dataset and extraction mode")
    expected_kind = "remote_endpoint" if extraction_mode == "remote" else "frozen_local_index"
    if any(check.target_kind != expected_kind for check in checks):
        raise ValueError("Check target does not match the extraction mode")
    if extraction_mode == "remote":
        if any(
            not check.endpoint or check.endpoint.rstrip("/") != helper.endpoint_url.rstrip("/")
            for check in checks
        ):
            raise ValueError("Endpoint differs from the saved plan")
    elif not index_reference or not index_reference.strip():
        raise ValueError("Local checks require the frozen index reference")
    return ScientificValidationResults(
        release_id=plan.release_id,
        plan_sha256=hashlib.sha256(plan.model_dump_json().encode()).hexdigest(),
        dataset_id=dataset_id,
        extraction_mode=extraction_mode,
        endpoint=helper.endpoint_url,
        index_reference=index_reference,
        results=[_observe(check, helper) for check in checks],
    )
