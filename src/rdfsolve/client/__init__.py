"""Typed RDF discovery, retrieval and query preparation."""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "Client": ("api", "Client"),
    "Results": ("api", "Results"),
    "DatasetClient": ("exploration", "DatasetClient"),
    "Hydrator": ("hydration", "Hydrator"),
    "HydrationLimitError": ("hydration", "HydrationLimitError"),
    "OntologyLookup": ("ontology", "OntologyLookup"),
    "PreparedQuery": ("query_fragments", "PreparedQuery"),
    "Requirement": ("retrieval", "Requirement"),
    "QueryResult": ("query", "QueryResult"),
    "QueryLog": ("query_log", "QueryLog"),
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(f".{module}", __name__), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
