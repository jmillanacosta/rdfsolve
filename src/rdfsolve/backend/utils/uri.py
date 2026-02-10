"""URI utility functions — re-exports from :mod:`rdfsolve.utils`.

Kept for backward compatibility with existing backend code.
"""

from rdfsolve.utils import (  # noqa: F401
    compact_uri,
    expand_curie,
    get_local_name,
    shorten_for_display,
)
