"""Unified models for schemas, mappings, connections, endpoints, quality, and sources."""

# URI helper (used by a few internal callers via models)
from rdfsolve._uri import (
    _build_br_prefix_map,
    _ns_from_uri,
    _prefix_from_ns,
    uri_to_curie,
)
from rdfsolve._uri import (
    make_expander as _make_expander,
)

# Mapping models
from rdfsolve.mapping_models import *  # noqa: F403

# Connection models
from rdfsolve.models.connection import (
    Connection,
    ConnectionType,
    EvidenceSource,
    EvidenceSourceType,
)

# Endpoint models
from rdfsolve.models.endpoint import (
    Endpoint,
    EndpointHealth,
    EndpointStatus,
)

# Quality models
from rdfsolve.models.quality import (
    QualityMetrics,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
)

# Source model
from rdfsolve.models.source_model import (
    PublicationRef,
    SourceModel,
    SourcesRegistry,
)

# Schema models
from rdfsolve.schema_models import *  # noqa: F403
