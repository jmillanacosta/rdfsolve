"""QLever utilities — Qleverfile generation and source helpers.

This sub-package is used by the ``rdfsolve qleverfile`` CLI command (for batch
Qleverfile generation) and ``rdfsolve qlever-boot`` (for standalone
endpoint booting).

All Qleverfile-building logic lives here so that parameterisation (memory
limits, timeouts, ports, parser settings) is consistent across entry points.

Usage::

    from rdfsolve.qlever import QleverConfig, build_qleverfile

"""

from rdfsolve.qlever.utils import (
    FORMAT_REGISTRY,
    QLEVERFILE_TEMPLATE,
    FormatSpec,
    QleverConfig,
    SourceAnalysis,
    analyse_source,
    build_provider_qleverfile,
    build_qleverfile,
    detect_data_format,
    graph_uri_to_tar_folder,
    tar_source_qleverfile_parts,
    urls_from_field,
)

__all__ = [
    "QleverConfig",
    "QLEVERFILE_TEMPLATE",
    "FORMAT_REGISTRY",
    "FormatSpec",
    "SourceAnalysis",
    "analyse_source",
    "detect_data_format",
    "urls_from_field",
    "graph_uri_to_tar_folder",
    "tar_source_qleverfile_parts",
    "build_qleverfile",
    "build_provider_qleverfile",
]
