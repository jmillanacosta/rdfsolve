"""Update sources.yaml with discovered graph information.

This module provides utilities to update sources.yaml based on graph discovery results.
It uses the SourceEntry schema defined in sources.py to ensure consistency.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from rdfsolve.sources import SourceEntry

logger = logging.getLogger(__name__)


def update_sources_yaml_with_graphs(
    sources_file: str | Path,
    source_name: str,
    discovered_graphs: dict[str, Any],
    void_partition_graphs: list[str] | None = None,
    backup: bool = True,
) -> bool:
    """Update sources.yaml with discovered graph information.

    Updates a source entry with:
    - void_graphs: list of discovered VoID metadata graph URIs
    - void_schema: list of VoID graphs with mineable partitions
    - Replaces old void_iri field

    Args:
        sources_file: Path to sources.yaml
        source_name: Name of source to update
        discovered_graphs: Results from discover_all_graphs()
        void_partition_graphs: VoID graphs that have mineable partitions
        backup: Create .yaml.bak backup before modifying

    Returns:
        True if update succeeded, False otherwise

    Example:
        >>> from rdfsolve import discover_all_graphs
        >>> graphs = discover_all_graphs("https://example.org/sparql")
        >>> update_sources_yaml_with_graphs(
        ...     "data/sources.yaml",
        ...     "mydata",
        ...     graphs,
        ...     void_partition_graphs=["http://example.org/void/"]
        ... )
    """
    sources_path = Path(sources_file)

    if not sources_path.exists():
        logger.error(f"Sources file not found: {sources_path}")
        return False

    # Load sources
    try:
        with open(sources_path) as f:
            sources = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load sources file: {e}")
        return False

    if not isinstance(sources, list):
        logger.error(f"Expected list of sources, got {type(sources)}")
        return False

    # Find source to update
    source_index = None
    for i, source in enumerate(sources):
        if source.get("name") == source_name:
            source_index = i
            break

    if source_index is None:
        logger.warning(f"Source '{source_name}' not found in {sources_path}")
        return False

    source = sources[source_index]

    # Extract void graphs from discovery
    void_graphs = discovered_graphs.get("void_graphs", [])

    # Update fields
    updates_made = False

    # Remove old void_iri if present
    if "void_iri" in source:
        old_void_iri = source.pop("void_iri")
        logger.info(f"Removed old void_iri: {old_void_iri}")
        updates_made = True

    # Add void_graphs if discovered
    if void_graphs:
        source["void_graphs"] = void_graphs
        logger.info(f"Added {len(void_graphs)} VoID graph(s): {void_graphs}")
        updates_made = True
    elif "void_graphs" in source:
        # Remove if no longer present
        source.pop("void_graphs")
        updates_made = True

    # Add void_schema if VoID partitions are mineable
    if void_partition_graphs:
        source["void_schema"] = void_partition_graphs
        logger.info(f"Added {len(void_partition_graphs)} VoID schema graph(s): {void_partition_graphs}")
        updates_made = True
    elif "void_schema" in source:
        # Remove if no longer present
        source.pop("void_schema")
        updates_made = True

    if not updates_made:
        logger.info(f"No updates needed for source '{source_name}'")
        return True

    # Create backup if requested
    if backup:
        backup_path = sources_path.with_suffix(".yaml.bak")
        try:
            import shutil

            shutil.copy2(sources_path, backup_path)
            logger.info(f"Created backup: {backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")

    # Write updated sources
    try:
        with open(sources_path, "w") as f:
            yaml.dump(sources, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        logger.info(f"Successfully updated {sources_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write sources file: {e}")
        return False


def update_multiple_sources(
    sources_file: str | Path,
    updates: dict[str, dict[str, Any]],
    backup: bool = True,
) -> dict[str, bool]:
    """Update multiple sources in sources.yaml.

    Args:
        sources_file: Path to sources.yaml
        updates: Dict mapping source_name to update data:
            {
                "source1": {
                    "discovered_graphs": {...},
                    "void_partition_graphs": [...]
                },
                ...
            }
        backup: Create backup before modifying

    Returns:
        Dict mapping source_name to success status
    """
    sources_path = Path(sources_file)

    if not sources_path.exists():
        logger.error(f"Sources file not found: {sources_path}")
        return dict.fromkeys(updates.keys(), False)

    # Load sources
    try:
        with open(sources_path) as f:
            sources = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load sources file: {e}")
        return dict.fromkeys(updates.keys(), False)

    if not isinstance(sources, list):
        logger.error(f"Expected list of sources, got {type(sources)}")
        return dict.fromkeys(updates.keys(), False)

    # Create backup if requested
    if backup:
        backup_path = sources_path.with_suffix(".yaml.bak")
        try:
            import shutil

            shutil.copy2(sources_path, backup_path)
            logger.info(f"Created backup: {backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")

    # Apply updates
    results = {}
    modified = False

    for source_name, update_data in updates.items():
        # Find source
        source = None
        for s in sources:
            if s.get("name") == source_name:
                source = s
                break

        if not source:
            logger.warning(f"Source '{source_name}' not found")
            results[source_name] = False
            continue

        # Apply updates
        discovered_graphs = update_data.get("discovered_graphs", {})
        void_partition_graphs = update_data.get("void_partition_graphs")

        void_graphs = discovered_graphs.get("void_graphs", [])

        # Remove old void_iri
        if "void_iri" in source:
            old_void_iri = source.pop("void_iri")
            logger.info(f"[{source_name}] Removed old void_iri: {old_void_iri}")
            modified = True

        # Add void_graphs
        if void_graphs:
            source["void_graphs"] = void_graphs
            logger.info(f"[{source_name}] Added {len(void_graphs)} VoID graph(s)")
            modified = True

        # Add void_schema
        if void_partition_graphs:
            source["void_schema"] = void_partition_graphs
            logger.info(f"[{source_name}] Added {len(void_partition_graphs)} VoID schema graph(s)")
            modified = True

        results[source_name] = True

    if not modified:
        logger.info("No updates to write")
        return results

    # Write updated sources
    try:
        with open(sources_path, "w") as f:
            yaml.dump(sources, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        logger.info(f"Successfully updated {sources_path}")
        return results
    except Exception as e:
        logger.error(f"Failed to write sources file: {e}")
        return dict.fromkeys(updates.keys(), False)


__all__ = ["update_multiple_sources", "update_sources_yaml_with_graphs"]
