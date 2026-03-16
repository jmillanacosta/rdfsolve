"""Backwards-compatible shim.

Re-exports all public symbols from ``schema_models`` and
``mapping_models`` so that existing ``from rdfsolve.models import X``
statements keep working unchanged.
"""

# URI helper (used by a few internal callers via models)
from rdfsolve._uri import (  # noqa: F401
    _build_br_prefix_map,
    _ns_from_uri,
    _prefix_from_ns,
    uri_to_curie,
)
from rdfsolve._uri import (  # noqa: F401
    make_expander as _make_expander,
)

# Mapping models
from rdfsolve.mapping_models import *  # noqa: F403

# Schema models
from rdfsolve.schema_models import *  # noqa: F403
