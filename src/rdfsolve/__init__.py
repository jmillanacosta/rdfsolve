"""RDF schema tools. Expensive optional subsystems load only when requested."""

import logging
from importlib import import_module
from typing import Any

logging.getLogger(__name__).addHandler(logging.NullHandler())
_EXPORTS = {
    "discover_all_graphs": ("api", "discover_all_graphs"),
    "discover_void_graphs": ("api", "discover_void_graphs"),
    "discover_void_source": ("api", "discover_void_source"),
    "enrich_source": ("api", "enrich_source"),
    "enrich_source_with_bioregistry": ("api", "enrich_source_with_bioregistry"),
    "execute_sparql": ("api", "execute_sparql"),
    "export_schema_artifacts": ("api", "export_schema_artifacts"),
    "get_bioregistry_metadata": ("api", "get_bioregistry_metadata"),
    "load_sources": ("api", "load_sources"),
    "mine_schema": ("api", "mine_schema"),
    "query_metadata": ("api", "query_metadata"),
    "ClassPair": ("mappings.derivation", "ClassPair"),
    "derive_class_mappings": ("mappings.derivation", "derive_class_mappings"),
    "ClassIndex": ("mappings.index", "ClassIndex"),
    "EntityClassInfo": ("mappings.index", "EntityClassInfo"),
    "DatasetClient": ("client.exploration", "DatasetClient"),
    "HydrationLimitError": ("client.hydration", "HydrationLimitError"),
    "Hydrator": ("client.hydration", "Hydrator"),
    "SchemaMiner": ("mining.miner", "SchemaMiner"),
    "AboutMetadata": ("schema_models.about", "AboutMetadata"),
    "MappingEdge": ("mappings.models", "MappingEdge"),
    "MinedSchema": ("schema_models.core", "MinedSchema"),
    "SchemaPattern": ("schema_models.pattern", "SchemaPattern"),
    "QueryResult": ("client.query", "QueryResult"),
    "ResultCell": ("client.query", "ResultCell"),
    "VoidSchema": ("schema_models.void_schema", "VoidSchema"),
    "classify_source_mode": ("sources", "classify_source_mode"),
    "VERSION": ("version", "VERSION"),
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
