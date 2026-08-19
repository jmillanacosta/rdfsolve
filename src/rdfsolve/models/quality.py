"""Schema validation and quality metrics models."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class ValidationSeverity(str, Enum):
    """Severity level of a validation issue."""

    ERROR = "error"
    """Critical issue that makes the schema invalid."""

    WARNING = "warning"
    """Issue that should be addressed but doesn't invalidate schema."""

    INFO = "info"
    """Informational note about potential improvements."""


class ValidationIssue(BaseModel):
    """A single validation finding.

    Represents an issue found during schema validation,
    with location information and suggested fixes.
    """

    severity: ValidationSeverity = Field(
        ...,
        description="Severity level of this issue",
    )
    code: str = Field(
        ...,
        description="Machine-readable issue code (e.g., 'MISSING_LABEL')",
    )
    message: str = Field(
        ...,
        description="Human-readable description of the issue",
    )
    location: str | None = Field(
        None,
        description="Location in schema (e.g., class URI, pattern index)",
    )
    suggestion: str | None = Field(
        None,
        description="Suggested fix for the issue",
    )


class ValidationResult(BaseModel):
    """Complete validation report for a schema.

    Aggregates all validation issues found during schema
    validation, with summary counts by severity.
    """

    # Identity
    schema_id: str | None = Field(
        None,
        description="ID of the validated schema",
    )
    dataset_name: str | None = Field(
        None,
        description="Name of the dataset",
    )

    # Validation status
    is_valid: bool = Field(
        default=True,
        description="Whether schema passes validation (no errors)",
    )
    validated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When validation was performed",
    )
    validator_version: str = Field(
        default="unknown",
        description="Version of the validator used",
    )

    # Issues
    issues: list[ValidationIssue] = Field(
        default_factory=list,
        description="List of validation issues found",
    )

    # Summary counts
    error_count: int = Field(
        default=0,
        ge=0,
        description="Number of error-level issues",
    )
    warning_count: int = Field(
        default=0,
        ge=0,
        description="Number of warning-level issues",
    )
    info_count: int = Field(
        default=0,
        ge=0,
        description="Number of info-level issues",
    )

    def add_issue(self, issue: ValidationIssue) -> None:
        """Add an issue and update counts."""
        self.issues.append(issue)
        if issue.severity == ValidationSeverity.ERROR:
            self.error_count += 1
            self.is_valid = False
        elif issue.severity == ValidationSeverity.WARNING:
            self.warning_count += 1
        else:
            self.info_count += 1

    @property
    def total_issues(self) -> int:
        """Total number of issues across all severities."""
        return self.error_count + self.warning_count + self.info_count


class QualityMetrics(BaseModel):
    """Aggregated quality scores for a schema or connection set.

    Provides normalized scores (0.0-1.0) for various quality
    dimensions, plus supporting statistics.
    """

    # Identity
    dataset_name: str | None = Field(
        None,
        description="Name of the dataset",
    )
    computed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When metrics were computed",
    )

    # Core quality scores (0.0-1.0)
    completeness_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="How complete the schema is (patterns, labels, datatypes)",
    )
    consistency_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Internal consistency (no conflicts, valid URIs)",
    )
    coverage_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Fraction of triples covered by schema patterns",
    )
    confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Overall confidence in schema accuracy",
    )

    # Supporting statistics
    pattern_count: int = Field(
        default=0,
        ge=0,
        description="Number of schema patterns",
    )
    class_count: int = Field(
        default=0,
        ge=0,
        description="Number of distinct classes",
    )
    property_count: int = Field(
        default=0,
        ge=0,
        description="Number of distinct properties",
    )
    labeled_fraction: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Fraction of URIs with labels",
    )
    typed_fraction: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Fraction of patterns with explicit types/datatypes",
    )

    # Derived metrics
    @property
    def overall_score(self) -> float:
        """Weighted average of all quality scores."""
        weights = {
            "completeness": 0.25,
            "consistency": 0.25,
            "coverage": 0.25,
            "confidence": 0.25,
        }
        return (
            self.completeness_score * weights["completeness"]
            + self.consistency_score * weights["consistency"]
            + self.coverage_score * weights["coverage"]
            + self.confidence_score * weights["confidence"]
        )

    @property
    def quality_tier(self) -> str:
        """Quality tier based on overall score.

        Returns one of: 'gold', 'silver', 'bronze', 'needs_review'
        """
        score = self.overall_score
        if score >= 0.9:
            return "gold"
        if score >= 0.7:
            return "silver"
        if score >= 0.5:
            return "bronze"
        return "needs_review"


__all__ = [
    "QualityMetrics",
    "ValidationIssue",
    "ValidationResult",
    "ValidationSeverity",
]
