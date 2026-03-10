"""Command line interface for :mod:`rdfsolve`."""

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from .api import (
    count_instances_per_class,
    discover_void_graphs,
    generate_void_from_endpoint,
    graph_to_schema,
    load_parser_from_file,
    load_parser_from_jsonld,
    to_jsonld_from_file,
    to_linkml_from_file,
    to_rdfconfig_from_file,
    to_shacl_from_file,
)

if TYPE_CHECKING:
    from .parser import VoidParser

__all__ = [
    "main",
]


# ═══════════════════════════════════════════════════════════════════
# Top-level group
# ═══════════════════════════════════════════════════════════════════


@click.group()
@click.version_option()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    r"""RDFSolve — RDF Schema Extraction and Analysis Toolkit.

    \b
    Pipeline commands (schema mining):
        rdfsolve pipeline discover     Discover VoID from remote endpoints
        rdfsolve pipeline mine         Mine schemas from remote endpoints
        rdfsolve pipeline local-mine   Mine from a local QLever endpoint
        rdfsolve pipeline qleverfile   Generate Qleverfiles for QLever

    \b
    Analysis commands:
        rdfsolve export    Convert schemas to LinkML / SHACL / CSV / …
        rdfsolve count     Count instances per class

    \b
    Mapping commands:
        rdfsolve instance-match   Cross-dataset class matching
        rdfsolve semra            Import external SeMRA mappings
        rdfsolve inference        Derive new mappings from existing ones
    """
    import logging

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)s %(levelname)s: %(message)s",
            force=True,
        )
        logging.getLogger("rdfsolve").setLevel(logging.DEBUG)
    else:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(levelname)s: %(message)s",
            force=True,
        )


# ═══════════════════════════════════════════════════════════════════
# Pipeline group — wraps scripts/mine_local.py routes
# ═══════════════════════════════════════════════════════════════════

# We add mine_local.py's ``src/`` dir to sys.path lazily so the
# helper functions can be imported.  The script already does this
# internally, but we repeat for safety.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SCRIPTS = _REPO_ROOT / "scripts"


def _ensure_scripts_importable() -> None:
    """Make ``scripts/`` importable so we can call mine_local helpers."""
    s = str(_SCRIPTS)
    if s not in sys.path:
        sys.path.insert(0, s)


@main.group()
def pipeline() -> None:
    """Schema-mining pipeline: discover, mine, local-mine, qleverfile.

    These commands replace the old ``rdfsolve discover``, ``mine``, and
    ``mine-all`` top-level commands.  Each route can target remote SPARQL
    endpoints or a local QLever instance.

    \b
    Quick-start:
        # Discover VoID from all registered endpoints
        rdfsolve pipeline discover

        # Mine schemas from all remote endpoints
        rdfsolve pipeline mine

        # Generate Qleverfiles then mine locally
        rdfsolve pipeline qleverfile --data-dir /data/rdf
        rdfsolve pipeline local-mine --name drugbank \\
            --endpoint http://localhost:7026

    All pipeline commands accept --sources, --output-dir, --filter,
    --timeout, and --benchmark.  Use ``rdfsolve pipeline <cmd> --help``
    for full details.
    """
    _ensure_scripts_importable()


# ── Shared option decorators ─────────────────────────────────────

_DEFAULT_SOURCES = str(_REPO_ROOT / "data" / "sources.yaml")
_DEFAULT_OUTPUT = str(_REPO_ROOT / "mined_schemas")


def _common_options(fn):
    """Shared options for all pipeline subcommands."""
    fn = click.option(
        "--sources", default=_DEFAULT_SOURCES,
        help="Path to sources YAML/JSON-LD/CSV.",
    )(fn)
    fn = click.option(
        "--output-dir", default=_DEFAULT_OUTPUT,
        help="Output directory for schemas/reports.",
    )(fn)
    fn = click.option(
        "--format", "fmt",
        type=click.Choice(["jsonld", "void", "all"]),
        default="all", help="Export format.",
    )(fn)
    fn = click.option(
        "--timeout", type=float, default=120.0,
        help="HTTP timeout per SPARQL request (seconds).",
    )(fn)
    fn = click.option(
        "--filter", "name_filter", default=None,
        help="Regex to select sources by name.",
    )(fn)
    fn = click.option(
        "--benchmark", is_flag=True,
        help="Collect per-run benchmarks (timing, memory, CPU).",
    )(fn)
    return fn


def _mining_options(fn):
    """Options shared by mine + local-mine."""
    fn = click.option(
        "--chunk-size", type=int, default=10_000,
        help="SPARQL pagination page size.",
    )(fn)
    fn = click.option(
        "--class-chunk-size", type=int, default=None,
        help=(
            "Page size for Phase-1 class discovery "
            "(two-phase mode only). Default: no pagination."
        ),
    )(fn)
    fn = click.option(
        "--class-batch-size", type=int, default=15,
        help="Classes per VALUES query in two-phase mining.",
    )(fn)
    fn = click.option(
        "--no-counts", is_flag=True,
        help="Skip triple-count queries (faster).",
    )(fn)
    fn = click.option(
        "--untyped-as-classes", is_flag=True,
        help=(
            "Treat untyped URI objects as owl:Class "
            "references instead of rdfs:Resource."
        ),
    )(fn)
    fn = click.option(
        "--author", "authors_raw",
        multiple=True,
        metavar="NAME|ORCID",
        help=(
            "Credit an author in provenance metadata. "
            "Format: 'Full Name|0000-0000-0000-0000'. "
            "ORCID is optional. Repeat for multiple authors."
        ),
    )(fn)
    fn = click.option(
        "--one-shot", "one_shot", is_flag=True,
        help=(
            "Mine using a single unbounded SELECT per pattern "
            "type (no LIMIT/OFFSET, no fallback chain). "
            "Recommended for local QLever endpoints. "
            "Records per-query timing and row count in the "
            "report for comparison with the fallback-chain run."
        ),
    )(fn)
    return fn


def _build_namespace(ctx, **overrides):
    """Build an argparse.Namespace from Click params + overrides.

    This bridges Click → argparse so we can call mine_local.py's
    route functions directly.

    Safe defaults are injected for attributes that route functions
    access directly (not via getattr) but that Click never sets
    (e.g. ``verbose``, ``route``).
    """
    import argparse
    # Safe defaults for attributes only the argparse __main__ sets.
    defaults = {
        "verbose": False,
        "route": None,
    }
    defaults.update(ctx.params)
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _print_result(result: dict) -> None:
    """Pretty-print a route result dict."""
    click.echo(f"\n{'═' * 60}")
    for key in (
        "succeeded", "generated", "discovered",
        "empty", "failed", "skipped",
    ):
        val = result.get(key)
        if val is not None:
            label = key.capitalize()
            click.echo(f"  {label:12s}: {len(val)}")
            if key == "failed" and val:
                for entry in val[:10]:
                    ds = (
                        entry["dataset"]
                        if isinstance(entry, dict) else entry
                    )
                    err = (
                        entry.get("error", "")[:80]
                        if isinstance(entry, dict) else ""
                    )
                    click.echo(f"    • {ds}: {err}")
    click.echo(f"{'═' * 60}")


# ── pipeline discover ────────────────────────────────────────────

@pipeline.command("discover")
@_common_options
@click.pass_context
def pipeline_discover(ctx, **kwargs) -> None:
    """Discover VoID descriptions from remote SPARQL endpoints.

    Iterates all sources in the registry and queries each endpoint
    for existing VoID partitions.  Fast and non-invasive.

    \b
    Example:
        rdfsolve pipeline discover
        rdfsolve pipeline discover --filter "uniprot|chembl"
    """
    from mine_local import _route_discover
    args = _build_namespace(ctx)
    result = _route_discover(args)
    _print_result(result)


# ── pipeline mine ─────────────────────────────────────────────────

@pipeline.command("mine")
@_common_options
@_mining_options
@click.pass_context
def pipeline_mine(ctx, **kwargs) -> None:
    """Mine schemas from remote SPARQL endpoints.

    Standard mining workflow: iterate endpoints, extract schema
    patterns, write JSON-LD schemas, VoID turtle, and analytics
    reports.  Reports are always written.

    \b
    Example:
        rdfsolve pipeline mine
        rdfsolve pipeline mine --filter "drugbank"
        rdfsolve pipeline mine --benchmark
    """
    from mine_local import _route_mine
    args = _build_namespace(ctx)
    result = _route_mine(args)
    _print_result(result)


# ── pipeline local-mine ──────────────────────────────────────────

@pipeline.command("local-mine")
@_common_options
@_mining_options
@click.option(
    "--endpoint", default="http://localhost:7001",
    help="Local QLever SPARQL endpoint URL.",
)
@click.option(
    "--name", default=None,
    help="Dataset name (required for single-dataset mode).",
)
@click.option(
    "--discover-first", is_flag=True,
    help="Run VoID discovery before mining.",
)
@click.option(
    "--void-uri-base", default=None,
    help=(
        "Base URI for generated VoID partition IRIs "
        "(default: sources.yaml value or built-in template)."
    ),
)
@click.option(
    "--test", is_flag=True,
    help="Process only the 3 smallest downloadable sources.",
)
@click.pass_context
def pipeline_local_mine(ctx, **kwargs) -> None:
    """Mine schemas from a local QLever endpoint.

    Use after downloading data and running ``qlever index && qlever
    start`` for a dataset.  Connects to the local endpoint and runs
    the full mining pipeline.

    \b
    Example:
        rdfsolve pipeline local-mine \\
            --endpoint http://localhost:7026 \\
            --name drugbank --discover-first --benchmark
    """
    from mine_local import _route_local_mine
    args = _build_namespace(ctx)
    result = _route_local_mine(args)
    _print_result(result)


# ── pipeline qleverfile ──────────────────────────────────────────

@pipeline.command("qleverfile")
@_common_options
@click.option(
    "--data-dir", required=True,
    help="Root directory where RDF dumps live (required).",
)
@click.option(
    "--base-port", type=int, default=7019,
    help="First port number for allocation.",
)
@click.option(
    "--test", is_flag=True,
    help="Generate only for 3 smallest downloadable sources.",
)
@click.option(
    "--runtime", type=click.Choice(["docker", "native"]),
    default="docker", help="QLever runtime.",
)
@click.pass_context
def pipeline_qleverfile(ctx, **kwargs) -> None:
    """Generate Qleverfiles for local QLever mining.

    Creates a Qleverfile for each source that has download URLs
    in the sources registry.  Each Qleverfile includes a GET_DATA_CMD
    that downloads and preprocesses the data.

    \b
    Example:
        rdfsolve pipeline qleverfile --data-dir /data/rdf
        rdfsolve pipeline qleverfile --data-dir /data/rdf --test
    """
    from mine_local import _route_generate_qleverfile
    args = _build_namespace(ctx)
    result = _route_generate_qleverfile(args)
    _print_result(result)


# Export helper functions
def _export_csv(parser: "VoidParser", output_path: Path, dataset_name: str) -> None:
    """Export schema to CSV format."""
    schema_df = parser.to_schema(filter_void_admin_nodes=True)
    schema_csv = output_path / f"{dataset_name}_schema.csv"
    schema_df.to_csv(schema_csv, index=False)
    click.echo(f"OK CSV:      {schema_csv} ({len(schema_df)} triples)")


def _export_jsonld(
    void_file: str,
    output_path: Path,
    dataset_name: str,
    endpoint_url: Optional[str] = None,
    graph_uri: Optional[str] = None,
) -> None:
    """Export schema to JSON-LD format."""
    graph_uris = [graph_uri] if graph_uri else None
    schema_jsonld = to_jsonld_from_file(
        void_file,
        filter_void_admin_nodes=True,
        endpoint_url=endpoint_url,
        dataset_name=dataset_name,
        graph_uris=graph_uris,
    )
    jsonld_file = output_path / f"{dataset_name}_schema.jsonld"
    with open(jsonld_file, "w") as f:
        json.dump(schema_jsonld, f, indent=2)
    click.echo(f"OK JSON-LD:  {jsonld_file}")


def _export_linkml(
    void_file: str,
    output_path: Path,
    dataset_name: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
) -> None:
    """Export schema to LinkML format."""
    linkml_schema_name = schema_name or dataset_name
    linkml_yaml = to_linkml_from_file(
        void_file,
        filter_void_nodes=True,
        schema_name=linkml_schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_uri,
    )
    linkml_file = output_path / f"{dataset_name}_linkml_schema.yaml"
    with open(linkml_file, "w") as f:
        f.write(linkml_yaml)
    click.echo(f"OK LinkML:   {linkml_file}")
    if schema_uri:
        click.echo(f"            Schema URI: {schema_uri}")


def _export_shacl(
    void_file: str,
    output_path: Path,
    dataset_name: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
    shacl_closed: bool,
    shacl_suffix: Optional[str],
) -> None:
    """Export schema to SHACL format."""
    shacl_schema_name = schema_name or dataset_name
    shacl_ttl = to_shacl_from_file(
        void_file,
        filter_void_nodes=True,
        schema_name=shacl_schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_uri,
        closed=shacl_closed,
        suffix=shacl_suffix,
    )
    shacl_file = output_path / f"{dataset_name}_schema.shacl.ttl"
    with open(shacl_file, "w") as f:
        f.write(shacl_ttl)
    shape_type = "closed" if shacl_closed else "open"
    click.echo(f"OK SHACL:    {shacl_file} ({shape_type} shapes)")
    if shacl_suffix:
        click.echo(f"            Shape suffix: {shacl_suffix}")


def _export_rdfconfig(
    void_file: str,
    output_path: Path,
    dataset_name: str,
    endpoint_url: Optional[str],
    endpoint_name: str,
    graph_uri: Optional[str],
) -> None:
    """Export schema to RDF-config format."""
    rdfconfig = to_rdfconfig_from_file(
        void_file,
        filter_void_nodes=True,
        endpoint_url=endpoint_url,
        endpoint_name=endpoint_name,
        graph_uri=graph_uri,
    )

    # Create config directory: $dataset_config
    # This is required by the rdf-config tool to read the files
    config_dir = output_path / f"{dataset_name}_config"
    config_dir.mkdir(parents=True, exist_ok=True)

    # Save model.yaml (standard name required by rdf-config)
    model_file = config_dir / "model.yaml"
    with open(model_file, "w") as f:
        f.write(rdfconfig["model"])

    # Save prefix.yaml (standard name required by rdf-config)
    prefix_file = config_dir / "prefix.yaml"
    with open(prefix_file, "w") as f:
        f.write(rdfconfig["prefix"])

    # Save endpoint.yaml if endpoint_url provided
    # (standard name required by rdf-config)
    if endpoint_url:
        endpoint_file = config_dir / "endpoint.yaml"
        with open(endpoint_file, "w") as f:
            f.write(rdfconfig["endpoint"])
        click.echo(f"OK RDF-config: {config_dir}/")
        click.echo("              model.yaml")
        click.echo("              prefix.yaml")
        click.echo("              endpoint.yaml")
    else:
        click.echo(f"OK RDF-config: {config_dir}/")
        click.echo("              model.yaml")
        click.echo("              prefix.yaml")
        click.echo("              (endpoint.yaml not created: use --endpoint-url)")


def _export_linkml_from_parser(
    parser: "VoidParser",
    output_path: Path,
    dataset_name: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
) -> None:
    """Export schema to LinkML format from a pre-built parser."""
    linkml_schema_name = schema_name or dataset_name
    linkml_yaml = parser.to_linkml_yaml(
        filter_void_nodes=True,
        schema_name=linkml_schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_uri,
    )
    linkml_file = output_path / f"{dataset_name}_linkml_schema.yaml"
    with open(linkml_file, "w") as f:
        f.write(linkml_yaml)
    click.echo(f"OK LinkML:   {linkml_file}")
    if schema_uri:
        click.echo(f"            Schema URI: {schema_uri}")


def _export_shacl_from_parser(
    parser: "VoidParser",
    output_path: Path,
    dataset_name: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
    shacl_closed: bool,
    shacl_suffix: Optional[str],
) -> None:
    """Export schema to SHACL format from a pre-built parser."""
    shacl_schema_name = schema_name or dataset_name
    shacl_ttl = parser.to_shacl(
        filter_void_nodes=True,
        schema_name=shacl_schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_uri,
        closed=shacl_closed,
        suffix=shacl_suffix,
    )
    shacl_file = output_path / f"{dataset_name}_schema.shacl.ttl"
    with open(shacl_file, "w") as f:
        f.write(shacl_ttl)
    shape_type = "closed" if shacl_closed else "open"
    click.echo(f"OK SHACL:    {shacl_file} ({shape_type} shapes)")
    if shacl_suffix:
        click.echo(f"            Shape suffix: {shacl_suffix}")


def _export_rdfconfig_from_parser(
    parser: "VoidParser",
    output_path: Path,
    dataset_name: str,
    endpoint_url: Optional[str],
    endpoint_name: str,
    graph_uri: Optional[str],
) -> None:
    """Export schema to RDF-config format from a pre-built parser."""
    rdfconfig = parser.to_rdfconfig(
        filter_void_nodes=True,
        endpoint_url=endpoint_url,
        endpoint_name=endpoint_name,
        graph_uri=graph_uri,
    )
    config_dir = output_path / f"{dataset_name}_config"
    config_dir.mkdir(parents=True, exist_ok=True)

    model_file = config_dir / "model.yaml"
    with open(model_file, "w") as f:
        f.write(rdfconfig["model"])

    prefix_file = config_dir / "prefix.yaml"
    with open(prefix_file, "w") as f:
        f.write(rdfconfig["prefix"])

    if endpoint_url:
        endpoint_file = config_dir / "endpoint.yaml"
        with open(endpoint_file, "w") as f:
            f.write(rdfconfig["endpoint"])
        click.echo(f"OK RDF-config: {config_dir}/")
        click.echo("              model.yaml")
        click.echo("              prefix.yaml")
        click.echo("              endpoint.yaml")
    else:
        click.echo(f"OK RDF-config: {config_dir}/")
        click.echo("              model.yaml")
        click.echo("              prefix.yaml")
        click.echo(
            "              (endpoint.yaml not created:"
            " use --endpoint-url)"
        )


@main.command()
@click.option(
    "--void-file",
    default=None,
    help="Path to VoID file (Turtle format)",
)
@click.option(
    "--schema-file",
    default=None,
    help="Path to mined-schema JSON-LD file (alternative to --void-file)",
)
@click.option("--output-dir", default=".", help="Output directory for exports")
@click.option(
    "--format",
    type=click.Choice(["csv", "jsonld", "linkml", "shacl", "rdfconfig", "coverage", "all"]),
    default="all",
    help="Export format (default: all)",
)
@click.option(
    "--schema-name",
    help="Custom name for LinkML schema (default: derived from filename)",
)
@click.option(
    "--schema-description",
    help="Description for LinkML schema",
)
@click.option(
    "--schema-uri",
    help="Base URI for LinkML schema (e.g., http://example.org/schemas/myschema)",
)
@click.option(
    "--shacl-closed/--shacl-open",
    default=True,
    help="Generate closed SHACL shapes (default: closed)",
)
@click.option(
    "--shacl-suffix",
    help="Suffix for SHACL shape names (e.g., 'Shape' -> PersonShape)",
)
@click.option(
    "--endpoint-url",
    help="SPARQL endpoint URL for RDF-config export",
)
@click.option(
    "--endpoint-name",
    default="endpoint",
    help="Endpoint name for RDF-config (default: 'endpoint')",
)
@click.option(
    "--graph-uri",
    help="Named graph URI for RDF-config export",
)
def export(
    void_file: Optional[str],
    schema_file: Optional[str],
    output_dir: str,
    format: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
    shacl_closed: bool,
    shacl_suffix: Optional[str],
    endpoint_url: Optional[str],
    endpoint_name: str,
    graph_uri: Optional[str],
) -> None:
    r"""Export RDF schema to various formats.

    Takes a VoID description file (--void-file) or a mined-schema
    JSON-LD file (--schema-file) and exports the schema in multiple
    formats for different use cases: analysis (CSV), semantic web
    (JSON-LD), data modeling (LinkML), validation (SHACL), RDF-config
    schema standard, and coverage analysis.

    Exactly one of --void-file or --schema-file must be provided.

    Formats:
      - csv:       Schema patterns as CSV table
      - jsonld:    Semantic web JSON-LD format
      - linkml:    LinkML YAML schema for data modeling
      - shacl:     SHACL shapes for RDF validation
      - rdfconfig: RDF-config YAML files (model, prefix, endpoint)
      - coverage:  Pattern coverage analysis
      - all:       Export all formats (default)


    SHACL Export:
      SHACL (Shapes Constraint Language) shapes can be used to validate
      RDF data against the extracted schema. Use --shacl-closed for
      strict validation (only allows defined properties) or --shacl-open
      for flexible validation.

    RDF-config Export:
      RDF-config is a schema standard consisting of YAML configuration
      files that describe RDF data models. Exports are saved in a
      directory named {dataset}_config/ containing:

        - model.yaml: Class and property structure
        - prefix.yaml: Namespace prefix definitions
        - endpoint.yaml: SPARQL endpoint configuration

      This directory structure is required by the rdf-config tool to
      read the configuration files.

    Example using a VoID file:
        >>> rdfsolve export --void-file void_description.ttl \\
            --format rdfconfig \\
            --endpoint-url https://example.org/sparql \\
            --output-dir ./exports

    Example using a mined-schema JSON-LD file:
        >>> rdfsolve export --schema-file dataset_schema.jsonld \\
            --format all \\
            --output-dir ./exports
    """
    if not void_file and not schema_file:
        raise click.UsageError(
            "One of --void-file or --schema-file is required."
        )
    if void_file and schema_file:
        raise click.UsageError(
            "Provide only one of --void-file or --schema-file, not both."
        )

    input_file: str = void_file or schema_file  # type: ignore[assignment]
    click.echo(f"Exporting schema from: {input_file}")

    try:
        if schema_file:
            parser = load_parser_from_jsonld(schema_file)
            dataset_name = (
                Path(schema_file).stem.replace("_schema", "")
            )
        else:
            parser = load_parser_from_file(void_file)  # type: ignore[arg-type]
            dataset_name = (
                Path(void_file).stem.replace("_void", "")  # type: ignore[arg-type]
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # CSV export
        if format in ["csv", "all"]:
            _export_csv(parser, output_path, dataset_name)

        # JSON-LD export — only meaningful when input is a VoID file;
        # skip when the input is already a JSON-LD schema file.
        if format in ["jsonld", "all"]:
            if void_file:
                _export_jsonld(
                    void_file,
                    output_path,
                    dataset_name,
                    endpoint_url=endpoint_url,
                    graph_uri=graph_uri,
                )
            else:
                click.echo(
                    "OK JSON-LD:  skipped "
                    "(input is already a JSON-LD schema file)"
                )

        # LinkML export
        if format in ["linkml", "all"]:
            if void_file:
                _export_linkml(
                    void_file,
                    output_path,
                    dataset_name,
                    schema_name,
                    schema_description,
                    schema_uri,
                )
            else:
                _export_linkml_from_parser(
                    parser,
                    output_path,
                    dataset_name,
                    schema_name,
                    schema_description,
                    schema_uri,
                )

        # SHACL export
        if format in ["shacl", "all"]:
            if void_file:
                _export_shacl(
                    void_file,
                    output_path,
                    dataset_name,
                    schema_name,
                    schema_description,
                    schema_uri,
                    shacl_closed,
                    shacl_suffix,
                )
            else:
                _export_shacl_from_parser(
                    parser,
                    output_path,
                    dataset_name,
                    schema_name,
                    schema_description,
                    schema_uri,
                    shacl_closed,
                    shacl_suffix,
                )

        # RDF-config export
        if format in ["rdfconfig", "all"]:
            if void_file:
                _export_rdfconfig(
                    void_file,
                    output_path,
                    dataset_name,
                    endpoint_url,
                    endpoint_name,
                    graph_uri,
                )
            else:
                _export_rdfconfig_from_parser(
                    parser,
                    output_path,
                    dataset_name,
                    endpoint_url,
                    endpoint_name,
                    graph_uri,
                )

        # Pattern coverage export
        if format in ["coverage", "all"]:
            click.echo(
                "  Coverage: Skipped (requires instance data, "
                "not available from schema file)"
            )

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


# -------------------------------------------------------------------
# NOTE: The old standalone ``mine`` and ``mine-all`` commands have been
# removed.  Their functionality is now available under the ``pipeline``
# sub-group  (``rdfsolve pipeline mine``, ``rdfsolve pipeline local-mine``).
# -------------------------------------------------------------------


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option(
    "--graph-uri",
    multiple=True,
    help="Specific graph URI(s) to query (optional)",
)
@click.option("--sample-limit", type=int, help="Limit number of results")
@click.option("--output", help="Output CSV file (prints to console if omitted)")
@click.option(
    "--exclude-service-graphs/--include-service-graphs",
    default=True,
    help="Exclude service/system graphs (default: excluded)",
)
def count(
    endpoint: str,
    graph_uri: tuple[str, ...],
    sample_limit: Optional[int],
    output: Optional[str],
    exclude_service_graphs: bool,
) -> None:
    r"""Count instances per class in a SPARQL endpoint.

    Queries the endpoint to count how many instances (subjects) exist
    for each rdf:type class in the dataset. Useful for understanding
    dataset size and composition before schema extraction.

    By default, service and system graphs (Virtuoso, well-known URIs,
    OWL ontology) are excluded from counting. Use
    --include-service-graphs to include them.

    Example:
      >>> rdfsolve count --endpoint https://sparql.uniprot.org/sparql \
                     --output class_counts.csv
    """
    click.echo(f"Counting instances at: {endpoint}")
    if not exclude_service_graphs:
        click.echo("  Including service/system graphs in counting")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        counts = count_instances_per_class(
            endpoint,
            graph_uris=graph_uris,
            sample_limit=sample_limit,
            exclude_graphs=exclude_service_graphs,
        )

        if isinstance(counts, dict):
            click.echo(f"\nFound {len(counts)} classes:")
            click.echo("=" * 60)

            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)

            for class_uri, count in sorted_counts:
                click.echo(f"{count}  {class_uri}")

            if output:
                import pandas as pd

                df = pd.DataFrame(sorted_counts, columns=["class_uri", "instance_count"])
                df.to_csv(output, index=False)
                click.echo(f"\nFull results saved to: {output}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


# ---------------------------------------------------------------------------
# instance-match command group
# ---------------------------------------------------------------------------

@main.group("instance-match")
def instance_match_group() -> None:
    """Instance-based matching: discover cross-dataset class links.

    Probes SPARQL endpoints for classes whose instances match bioregistry
    URI patterns and writes skos:narrowMatch mapping JSON-LD files.

    \b
    Typical workflow:
        rdfsolve instance-match probe --prefix ensembl -o ensembl_mapping.jsonld
        rdfsolve instance-match seed  --prefixes ensembl uniprot chebi
    """


@instance_match_group.command("probe")
@click.option(
    "--prefix", "-p", required=True,
    help="Bioregistry prefix to probe (e.g. 'ensembl').",
)
@click.option(
    "--sources", default=None, show_default=False,
    help=(
        "Path to sources file (JSON-LD or CSV). "
        "Default: auto-detect data/sources.jsonld."
    ),
)
@click.option(
    "--sources-csv", default=None, hidden=True,
    help="Deprecated alias for --sources.",
)
@click.option(
    "--predicate",
    default="http://www.w3.org/2004/02/skos/core#narrowMatch",
    show_default=True,
    help="Mapping predicate URI.",
)
@click.option(
    "--dataset", "-d", "datasets", multiple=True,
    help="Restrict to this dataset name (repeatable).",
)
@click.option(
    "--timeout", default=60.0, show_default=True, type=float,
    help="SPARQL request timeout in seconds.",
)
@click.option(
    "--output", "-o", default=None,
    help="Write JSON-LD to this file (default: stdout).",
)
def probe_cmd(
    prefix: str,
    sources: Optional[str],
    sources_csv: Optional[str],
    predicate: str,
    datasets: tuple[str, ...],
    timeout: float,
    output: Optional[str],
) -> None:
    """Probe endpoints for a single bioregistry resource.

    Queries every endpoint in SOURCES for RDF classes whose instances
    match the URI patterns registered in bioregistry for PREFIX and emits
    a JSON-LD mapping document.
    """
    import json

    from rdfsolve.api import probe_instance_mapping

    try:
        result = probe_instance_mapping(
            prefix=prefix,
            sources=sources or sources_csv,
            predicate=predicate,
            dataset_names=list(datasets) if datasets else None,
            timeout=timeout,
        )
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    text = json.dumps(result, indent=2)
    if output:
        Path(output).write_text(text)
        edge_count = len(result.get("@graph", []))
        click.echo(f"Written to {output} ({edge_count} source nodes)")
    else:
        click.echo(text)


@instance_match_group.command("seed")
@click.option(
    "--prefixes", "-p", "prefix_list", required=True, multiple=True,
    help="Bioregistry prefix (repeatable).",
)
@click.option(
    "--sources", default=None, show_default=False,
    help=(
        "Path to sources file (JSON-LD or CSV). "
        "Default: auto-detect data/sources.jsonld."
    ),
)
@click.option(
    "--sources-csv", default=None, hidden=True,
    help="Deprecated alias for --sources.",
)
@click.option(
    "--output-dir",
    default="docker/mappings/instance_matching",
    show_default=True,
    help="Directory to write JSON-LD mapping files.",
)
@click.option(
    "--predicate",
    default="http://www.w3.org/2004/02/skos/core#narrowMatch",
    show_default=True,
    help="Mapping predicate URI.",
)
@click.option(
    "--dataset", "-d", "datasets", multiple=True,
    help="Restrict to this dataset name (repeatable).",
)
@click.option(
    "--timeout", default=60.0, show_default=True, type=float,
    help="SPARQL request timeout in seconds.",
)
@click.option(
    "--no-skip-existing", is_flag=True, default=False,
    help="Re-probe even if the output file already exists.",
)
def seed_cmd(
    prefix_list: tuple[str, ...],
    sources: Optional[str],
    sources_csv: Optional[str],
    output_dir: str,
    predicate: str,
    datasets: tuple[str, ...],
    timeout: float,
    no_skip_existing: bool,
) -> None:
    """Seed mapping files for multiple bioregistry resources.

    Writes {PREFIX}_instance_mapping.jsonld to OUTPUT_DIR for each
    supplied PREFIX.  Existing files are skipped unless --no-skip-existing
    is passed.
    """
    from rdfsolve.api import seed_instance_mappings

    try:
        result = seed_instance_mappings(
            prefixes=list(prefix_list),
            sources=sources or sources_csv,
            output_dir=output_dir,
            predicate=predicate,
            dataset_names=list(datasets) if datasets else None,
            timeout=timeout,
            skip_existing=not no_skip_existing,
        )
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for p in result["succeeded"]:
        click.echo(f"  OK {p}")
    for f in result["failed"]:
        click.echo(f"  FAIL {f['prefix']}: {f['error']}", err=True)

    if result["failed"]:
        raise SystemExit(1)


# ── semra command group ──────────────────────────────────────────


@main.group("semra")
def semra_group() -> None:
    """SeMRA integration: import external mappings from semra sources.

    Downloads mappings from community sources (biomappings, Gilda, etc.)
    and writes one JSON-LD file per (source, bioregistry-prefix) pair.

    \b
    Typical workflow:
        rdfsolve semra import --source biomappings
        rdfsolve semra seed --sources biomappings gilda
    """


@semra_group.command("import")
@click.option(
    "--source", "-s", required=True,
    help="SeMRA source key (e.g. 'biomappings', 'gilda').",
)
@click.option(
    "--prefix", "-p", "prefixes", multiple=True,
    help=(
        "Keep only these bioregistry prefixes (repeatable). "
        "Default: keep all."
    ),
)
@click.option(
    "--output-dir",
    default="docker/mappings/semra",
    show_default=True,
    help="Directory to write JSON-LD files.",
)
def semra_import_cmd(
    source: str,
    prefixes: tuple[str, ...],
    output_dir: str,
) -> None:
    """Import mappings from a single SeMRA source.

    Writes {source}_{prefix}.jsonld for each unique subject prefix
    found in the downloaded mappings.
    """
    from rdfsolve.api import import_semra_source

    try:
        result = import_semra_source(
            source=source,
            keep_prefixes=list(prefixes) if prefixes else None,
            output_dir=output_dir,
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in result["succeeded"]:
        click.echo(f"  OK {s}")
    for f in result["failed"]:
        click.echo(
            f"  FAIL {f.get('source')}/{f.get('prefix')}: "
            f"{f.get('error')}",
            err=True,
        )
    if result["failed"]:
        raise SystemExit(1)


@semra_group.command("seed")
@click.option(
    "--sources", "-s", "source_list", required=True, multiple=True,
    help="SeMRA source key (repeatable).",
)
@click.option(
    "--prefix", "-p", "prefixes", multiple=True,
    help=(
        "Keep only these bioregistry prefixes (repeatable)."
    ),
)
@click.option(
    "--output-dir",
    default="docker/mappings/semra",
    show_default=True,
    help="Directory to write JSON-LD files.",
)
def semra_seed_cmd(
    source_list: tuple[str, ...],
    prefixes: tuple[str, ...],
    output_dir: str,
) -> None:
    """Seed mapping files from multiple SeMRA sources."""
    from rdfsolve.api import seed_semra_mappings

    try:
        result = seed_semra_mappings(
            sources=list(source_list),
            keep_prefixes=list(prefixes) if prefixes else None,
            output_dir=output_dir,
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in result["succeeded"]:
        click.echo(f"  OK {s}")
    for f in result["failed"]:
        click.echo(
            f"  FAIL {f.get('source')}/{f.get('prefix')}: "
            f"{f.get('error')}",
            err=True,
        )
    if result["failed"]:
        raise SystemExit(1)


# ── inference command group ──────────────────────────────────────


@main.group("inference")
def inference_group() -> None:
    """Mapping inference: derive new mappings from existing ones.

    Uses SeMRA inference operations (inversion, transitivity,
    generalisation) to expand a set of mapping JSON-LD files.

    \b
    Typical workflow:
        rdfsolve inference run --input file1.jsonld file2.jsonld \\
            --output docker/mappings/inferenced/inferred.jsonld
        rdfsolve inference seed
    """


@inference_group.command("run")
@click.option(
    "--input", "-i", "input_paths", required=True, multiple=True,
    help="Input mapping JSON-LD file (repeatable).",
)
@click.option(
    "--output", "-o", "output_path", required=True,
    help="Output JSON-LD file path.",
)
@click.option(
    "--no-inversion", is_flag=True, default=False,
    help="Disable inversion inference.",
)
@click.option(
    "--no-transitivity", is_flag=True, default=False,
    help="Disable transitivity (chain) inference.",
)
@click.option(
    "--generalisation", is_flag=True, default=False,
    help="Enable generalisation inference (off by default).",
)
@click.option(
    "--chain-cutoff", default=3, show_default=True, type=int,
    help="Maximum chain length for transitivity.",
)
@click.option(
    "--name", "dataset_name", default=None,
    help="Override @about.dataset_name in the output.",
)
def inference_run_cmd(
    input_paths: tuple[str, ...],
    output_path: str,
    no_inversion: bool,
    no_transitivity: bool,
    generalisation: bool,
    chain_cutoff: int,
    dataset_name: Optional[str],
) -> None:
    """Infer new mappings from the given input files."""
    from rdfsolve.api import infer_mappings

    try:
        result = infer_mappings(
            input_paths=list(input_paths),
            output_path=output_path,
            inversion=not no_inversion,
            transitivity=not no_transitivity,
            generalisation=generalisation,
            chain_cutoff=chain_cutoff,
            dataset_name=dataset_name,
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    click.echo(
        f"  OK {result['output_edges']} edges written to "
        f"{result['output_path']} "
        f"(from {result['input_edges']} inputs, "
        f"ops: {result['inference_types']})"
    )


@inference_group.command("seed")
@click.option(
    "--input-dir",
    default="docker/mappings",
    show_default=True,
    help="Directory containing instance_matching/ and semra/ subdirs.",
)
@click.option(
    "--output-dir",
    default="docker/mappings/inferenced",
    show_default=True,
    help="Directory for the inferenced output.",
)
@click.option(
    "--name", "output_name",
    default="inferenced_mappings",
    show_default=True,
    help="Output file stem (without .jsonld).",
)
@click.option(
    "--no-inversion", is_flag=True, default=False,
    help="Disable inversion inference.",
)
@click.option(
    "--no-transitivity", is_flag=True, default=False,
    help="Disable transitivity inference.",
)
@click.option(
    "--generalisation", is_flag=True, default=False,
    help="Enable generalisation inference.",
)
@click.option(
    "--chain-cutoff", default=3, show_default=True, type=int,
    help="Maximum chain length for transitivity.",
)
def inference_seed_cmd(
    input_dir: str,
    output_dir: str,
    output_name: str,
    no_inversion: bool,
    no_transitivity: bool,
    generalisation: bool,
    chain_cutoff: int,
) -> None:
    """Infer over all mappings found under INPUT_DIR."""
    from rdfsolve.api import seed_inferenced_mappings

    try:
        result = seed_inferenced_mappings(
            input_dir=input_dir,
            output_dir=output_dir,
            output_name=output_name,
            inversion=not no_inversion,
            transitivity=not no_transitivity,
            generalisation=generalisation,
            chain_cutoff=chain_cutoff,
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    if result["output_path"]:
        click.echo(
            f"  OK {result['output_edges']} edges → "
            f"{result['output_path']}"
        )
    else:
        click.echo("  ⚠ No input files found.", err=True)


if __name__ == "__main__":
    main()
