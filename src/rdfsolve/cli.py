"""Command line interface for :mod:`rdfsolve`."""

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from .api import (
    load_parser_from_file,
    load_parser_from_jsonld,
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
    r"""RDFSolve - RDF Schema Extraction, Export and Analysis Toolkit.

    Pipeline commands (schema mining)::

        rdfsolve pipeline mine         Mine schemas from remote endpoints
        rdfsolve pipeline local-mine   Mine from a local QLever endpoint
        rdfsolve pipeline qleverfile   Generate Qleverfiles for QLever

    Analysis commands::

        rdfsolve export    Interconvert schemas between JSON-LD, LinkML, SHACL, CSV, ...

    Mapping commands::

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
# Pipeline group - wraps scripts/mine_local.py routes
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
    r"""Schema-mining pipeline: mine, local-mine, qleverfile.

    These commands replace the old ``rdfsolve discover``, ``mine``, and
    ``mine-all`` top-level commands.  Each route can target remote SPARQL
    endpoints or a local QLever instance.

    Quick-start examples::

        # Mine schemas from all remote endpoints
        rdfsolve pipeline mine

        # Generate Qleverfiles then mine locally
        rdfsolve pipeline qleverfile --data-dir /data/rdf
        rdfsolve pipeline local-mine --name drugbank \
            --endpoint http://localhost:7026

    All pipeline commands accept ``--sources``, ``--output-dir``,
    ``--filter``, ``--timeout``, and ``--benchmark``.
    Use ``rdfsolve pipeline <cmd> --help`` for full details.
    """
    _ensure_scripts_importable()


# ── Shared option decorators ─────────────────────────────────────

_DEFAULT_SOURCES = str(_REPO_ROOT / "data" / "sources.yaml")
_DEFAULT_OUTPUT = str(_REPO_ROOT / "mined_schemas")


def _common_options(fn: Any) -> Any:
    """Shared options for all pipeline subcommands."""
    fn = click.option(
        "--sources",
        default=_DEFAULT_SOURCES,
        help="Path to sources YAML/JSON-LD/CSV.",
    )(fn)
    fn = click.option(
        "--output-dir",
        default=_DEFAULT_OUTPUT,
        help="Output directory for schemas/reports.",
    )(fn)
    fn = click.option(
        "--format",
        "fmt",
        type=click.Choice(["jsonld", "void", "all"]),
        default="all",
        help="Export format.",
    )(fn)
    fn = click.option(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout per SPARQL request (seconds).",
    )(fn)
    fn = click.option(
        "--filter",
        "name_filter",
        default=None,
        help="Regex to select sources by name.",
    )(fn)
    fn = click.option(
        "--benchmark",
        is_flag=True,
        help="Collect per-run benchmarks (timing, memory, CPU).",
    )(fn)
    return fn


def _mining_options(fn: Any) -> Any:
    """Options shared by mine + local-mine."""
    fn = click.option(
        "--chunk-size",
        type=int,
        default=10_000,
        help="SPARQL pagination page size.",
    )(fn)
    fn = click.option(
        "--class-chunk-size",
        type=int,
        default=None,
        help=(
            "Page size for Phase-1 class discovery (two-phase mode only). Default: no pagination."
        ),
    )(fn)
    fn = click.option(
        "--class-batch-size",
        type=int,
        default=15,
        help="Classes per VALUES query in two-phase mining.",
    )(fn)
    fn = click.option(
        "--no-counts",
        is_flag=True,
        help="Skip triple-count queries (faster).",
    )(fn)
    fn = click.option(
        "--untyped-as-classes",
        is_flag=True,
        help=("Treat untyped URI objects as owl:Class references instead of rdfs:Resource."),
    )(fn)
    fn = click.option(
        "--author",
        "authors_raw",
        multiple=True,
        metavar="NAME|ORCID",
        help=(
            "Credit an author in provenance metadata. "
            "Format: 'Full Name|0000-0000-0000-0000'. "
            "ORCID is optional. Repeat for multiple authors."
        ),
    )(fn)
    fn = click.option(
        "--one-shot",
        "one_shot",
        is_flag=True,
        help=(
            "Mine using a single unbounded SELECT per pattern "
            "type (no LIMIT/OFFSET, no fallback chain). "
            "Recommended for local QLever endpoints. "
            "Records per-query timing and row count in the "
            "report for comparison with the fallback-chain run."
        ),
    )(fn)
    return fn


def _build_namespace(ctx: click.Context, **overrides: Any) -> Any:
    """Build an argparse.Namespace from Click params + overrides.

    This bridges Click -> argparse so we can call mine_local.py's
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


def _print_result(result: dict[str, Any]) -> None:
    """Pretty-print a route result dict."""
    click.echo(f"\n{'═' * 60}")
    for key in (
        "succeeded",
        "generated",
        "discovered",
        "empty",
        "failed",
        "skipped",
    ):
        val = result.get(key)
        if val is not None:
            label = key.capitalize()
            click.echo(f"  {label:12s}: {len(val)}")
            if key == "failed" and val:
                for entry in val[:10]:
                    ds = entry["dataset"] if isinstance(entry, dict) else entry
                    err = entry.get("error", "")[:80] if isinstance(entry, dict) else ""
                    click.echo(f"    • {ds}: {err}")
    click.echo(f"{'═' * 60}")


# ── pipeline mine ─────────────────────────────────────────────────


@pipeline.command("mine")
@_common_options
@_mining_options
@click.pass_context
def pipeline_mine(ctx: click.Context, **kwargs: Any) -> None:
    r"""Mine schemas from remote SPARQL endpoints.

    Standard mining workflow: iterate endpoints, extract schema
    patterns, write JSON-LD schemas, VoID turtle, and analytics
    reports.  Reports are always written.

    Examples::

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
    "--endpoint",
    default="http://localhost:7001",
    help="Local QLever SPARQL endpoint URL.",
)
@click.option(
    "--name",
    default=None,
    help="Dataset name (required for single-dataset mode).",
)
@click.option(
    "--discover-first",
    is_flag=True,
    help="Run VoID discovery before mining.",
)
@click.option(
    "--void-uri-base",
    default=None,
    help=(
        "Base URI for generated VoID partition IRIs "
        "(default: sources.yaml value or built-in template)."
    ),
)
@click.option(
    "--test",
    is_flag=True,
    help="Process only the 3 smallest downloadable sources.",
)
@click.pass_context
def pipeline_local_mine(ctx: click.Context, **kwargs: Any) -> None:
    r"""Mine schemas from a local QLever endpoint.

    Use after downloading data and running ``qlever index && qlever
    start`` for a dataset.  Connects to the local endpoint and runs
    the full mining pipeline.

    Example::

        rdfsolve pipeline local-mine \
            --endpoint http://localhost:7026 \
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
    "--data-dir",
    required=True,
    help="Root directory where RDF dumps live (required).",
)
@click.option(
    "--base-port",
    type=int,
    default=7019,
    help="First port number for allocation.",
)
@click.option(
    "--test",
    is_flag=True,
    help="Generate only for 3 smallest downloadable sources.",
)
@click.option(
    "--runtime",
    type=click.Choice(["docker", "native"]),
    default="docker",
    help="QLever runtime.",
)
@click.pass_context
def pipeline_qleverfile(ctx: click.Context, **kwargs: Any) -> None:
    r"""Generate Qleverfiles for local QLever mining.

    Creates a Qleverfile for each source that has download URLs
    in the sources registry.  Each Qleverfile includes a GET_DATA_CMD
    that downloads and preprocesses the data.

    Examples::

        rdfsolve pipeline qleverfile --data-dir /data/rdf
        rdfsolve pipeline qleverfile --data-dir /data/rdf --test
    """
    from mine_local import _route_generate_qleverfile

    args = _build_namespace(ctx)
    result = _route_generate_qleverfile(args)
    _print_result(result)


# ═══════════════════════════════════════════════════════════════════
# Export group - interconvert between schema formats
# ═══════════════════════════════════════════════════════════════════

# Supported formats (input -> model -> output):
#   VoID (.ttl)    ─┐
#                    ├-> VoidParser -> JSON-LD dict ─┬-> csv
#   JSON-LD (.jsonld)┘                             ├-> jsonld
#                                                  ├-> void (.ttl)
#                                                  ├-> linkml (.yaml)
#                                                  ├-> shacl  (.shacl.ttl)
#                                                  └-> rdfconfig (dir/)

_INPUT_HELP = (
    "Input schema file. Accepts VoID Turtle (.ttl) or "
    "rdfsolve JSON-LD (.jsonld).  Format is auto-detected "
    "from the file extension."
)


def _load_input(input_file: str) -> tuple["VoidParser", str]:
    """Load *input_file*, return ``(parser, dataset_name)``.

    Auto-detects the format from the file extension:
    - ``.jsonld`` / ``.json`` -> mined-schema JSON-LD
    - anything else (typically ``.ttl``) -> VoID Turtle
    """
    path = Path(input_file)
    if not path.exists():
        raise click.BadParameter(f"File not found: {input_file}", param_hint="INPUT")

    ext = path.suffix.lower()
    if ext in (".jsonld", ".json"):
        parser = load_parser_from_jsonld(str(path))
        dataset_name = path.stem.replace("_schema", "")
    else:
        parser = load_parser_from_file(str(path))
        dataset_name = path.stem.replace("_void", "")

    return parser, dataset_name


@main.group(invoke_without_command=True)
@click.pass_context
def export(ctx: click.Context) -> None:
    r"""Convert RDF schemas between formats.

    All subcommands accept a VoID Turtle (.ttl) or rdfsolve JSON-LD
    (.jsonld) file as INPUT and produce the requested output format.
    The input format is auto-detected from the file extension.

    Supported conversions (any-to-any via the internal model)::

        VoID (.ttl)      <->  JSON-LD (.jsonld)
        JSON-LD (.jsonld) ->  CSV, LinkML, SHACL, RDF-config, VoID

    Subcommands::

        csv        Export schema patterns as a CSV table
        jsonld     Export schema as JSON-LD
        void       Export schema as VoID Turtle
        linkml     Export schema as LinkML YAML
        shacl      Export schema as SHACL shapes (Turtle)
        rdfconfig  Export schema as RDF-config YAML files

    Examples::

        rdfsolve export csv       dataset_schema.jsonld
        rdfsolve export linkml    dataset_void.ttl -o ./out
        rdfsolve export shacl     dataset_schema.jsonld --closed
        rdfsolve export rdfconfig dataset_void.ttl --endpoint-url http://...
        rdfsolve export void      dataset_schema.jsonld
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ── export csv ───────────────────────────────────────────────────


@export.command("csv")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
def export_csv(input_file: str, output_dir: str) -> None:
    r"""Export schema patterns as a CSV table.

    Example::

        rdfsolve export csv dataset_schema.jsonld -o ./exports
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        df = parser.to_schema(filter_void_admin_nodes=True)
        csv_path = out / f"{name}_schema.csv"
        df.to_csv(csv_path, index=False)
        click.echo(f"OK  {csv_path}  ({len(df)} rows)")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ── export jsonld ────────────────────────────────────────────────


@export.command("jsonld")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
@click.option("--endpoint-url", default=None, help="SPARQL endpoint URL for @about.")
@click.option("--graph-uri", default=None, help="Named graph URI for @about.")
def export_jsonld(
    input_file: str,
    output_dir: str,
    endpoint_url: str | None,
    graph_uri: str | None,
) -> None:
    r"""Export schema as JSON-LD.

    Useful for converting a VoID Turtle file into the rdfsolve JSON-LD
    format. If the input is already JSON-LD, it is re-serialised (which
    can be used to refresh @about metadata).

    Example::

        rdfsolve export jsonld dataset_void.ttl -o ./exports
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        graph_uris = [graph_uri] if graph_uri else None
        schema_jsonld = parser.to_jsonld(
            filter_void_admin_nodes=True,
            endpoint_url=endpoint_url,
            dataset_name=name,
            graph_uris=graph_uris,
        )
        jsonld_path = out / f"{name}_schema.jsonld"
        with open(jsonld_path, "w") as f:
            json.dump(schema_jsonld, f, indent=2)
        click.echo(f"OK  {jsonld_path}")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ── export void ──────────────────────────────────────────────────


@export.command("void")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
def export_void(input_file: str, output_dir: str) -> None:
    r"""Export schema as VoID Turtle.

    Converts a JSON-LD schema back to VoID RDF (Turtle).  Also works
    with VoID input (round-trip).

    Example::

        rdfsolve export void dataset_schema.jsonld -o ./exports
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        jsonld_dict = parser.to_jsonld(filter_void_admin_nodes=True)

        from rdfsolve.models import MinedSchema

        schema = MinedSchema.from_dict(jsonld_dict)
        g = schema.to_void_graph()
        void_path = out / f"{name}_void.ttl"
        g.serialize(destination=str(void_path), format="turtle")
        click.echo(f"OK  {void_path}  ({len(g)} triples)")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ── export linkml ────────────────────────────────────────────────


@export.command("linkml")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
@click.option("--schema-name", default=None, help="Schema name (default: from filename).")
@click.option("--schema-description", default=None, help="Schema description.")
@click.option(
    "--schema-uri",
    default=None,
    help="Base URI for the schema (e.g. http://example.org/schemas/myschema).",
)
def export_linkml(
    input_file: str,
    output_dir: str,
    schema_name: str | None,
    schema_description: str | None,
    schema_uri: str | None,
) -> None:
    r"""Export schema as LinkML YAML.

    Generates a LinkML schema definition for data modelling,
    validation, and code generation.

    Example::

        rdfsolve export linkml dataset_schema.jsonld --schema-name myds
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        linkml_yaml = parser.to_linkml_yaml(
            filter_void_nodes=True,
            schema_name=schema_name or name,
            schema_description=schema_description,
            schema_base_uri=schema_uri,
        )
        linkml_path = out / f"{name}_linkml_schema.yaml"
        linkml_path.write_text(linkml_yaml)
        click.echo(f"OK  {linkml_path}")
        if schema_uri:
            click.echo(f"    Schema URI: {schema_uri}")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ── export shacl ─────────────────────────────────────────────────


@export.command("shacl")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
@click.option("--schema-name", default=None, help="Schema name (default: from filename).")
@click.option("--schema-description", default=None, help="Schema description.")
@click.option(
    "--schema-uri",
    default=None,
    help="Base URI for the schema.",
)
@click.option(
    "--closed/--open",
    "shacl_closed",
    default=True,
    help="Generate closed shapes (default) or open shapes.",
)
@click.option(
    "--suffix",
    default=None,
    help="Suffix for shape names (e.g. 'Shape' -> PersonShape).",
)
def export_shacl(
    input_file: str,
    output_dir: str,
    schema_name: str | None,
    schema_description: str | None,
    schema_uri: str | None,
    shacl_closed: bool,
    suffix: str | None,
) -> None:
    r"""Export schema as SHACL shapes (Turtle).

    SHACL shapes validate RDF data against the extracted schema.
    Use --closed (default) for strict validation or --open for flexible.

    Examples::

        rdfsolve export shacl dataset_schema.jsonld --open
        rdfsolve export shacl dataset_schema.jsonld --suffix Shape
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        shacl_ttl = parser.to_shacl(
            filter_void_nodes=True,
            schema_name=schema_name or name,
            schema_description=schema_description,
            schema_base_uri=schema_uri,
            closed=shacl_closed,
            suffix=suffix,
        )
        shacl_path = out / f"{name}_schema.shacl.ttl"
        shacl_path.write_text(shacl_ttl)
        shape_type = "closed" if shacl_closed else "open"
        click.echo(f"OK  {shacl_path}  ({shape_type} shapes)")
        if suffix:
            click.echo(f"    Shape suffix: {suffix}")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ── export rdfconfig ─────────────────────────────────────────────


@export.command("rdfconfig")
@click.argument("input_file", metavar="INPUT")
@click.option("-o", "--output-dir", default=".", help="Output directory.")
@click.option(
    "--endpoint-url",
    default=None,
    help="SPARQL endpoint URL (generates endpoint.yaml when provided).",
)
@click.option(
    "--endpoint-name",
    default="endpoint",
    help="Endpoint name (default: 'endpoint').",
)
@click.option("--graph-uri", default=None, help="Named graph URI.")
def export_rdfconfig(
    input_file: str,
    output_dir: str,
    endpoint_url: str | None,
    endpoint_name: str,
    graph_uri: str | None,
) -> None:
    r"""Export schema as RDF-config YAML files.

    RDF-config is a schema standard for describing RDF data models.
    Produces a {dataset}_config/ directory with model.yaml,
    prefix.yaml, and optionally endpoint.yaml.

    Example::

        rdfsolve export rdfconfig dataset_void.ttl \
            --endpoint-url https://example.org/sparql
    """
    try:
        parser, name = _load_input(input_file)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        rdfconfig = parser.to_rdfconfig(
            filter_void_nodes=True,
            endpoint_url=endpoint_url,
            endpoint_name=endpoint_name,
            graph_uri=graph_uri,
        )
        config_dir = out / f"{name}_config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "model.yaml").write_text(rdfconfig["model"])
        (config_dir / "prefix.yaml").write_text(rdfconfig["prefix"])
        click.echo(f"OK  {config_dir}/")
        click.echo("    model.yaml")
        click.echo("    prefix.yaml")
        if endpoint_url:
            (config_dir / "endpoint.yaml").write_text(rdfconfig["endpoint"])
            click.echo("    endpoint.yaml")
        else:
            click.echo("    (endpoint.yaml skipped - use --endpoint-url)")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()


# ---------------------------------------------------------------------------
# instance-match command group
# ---------------------------------------------------------------------------


@main.group("instance-match")
def instance_match_group() -> None:
    r"""Instance-based matching: discover cross-dataset class links.

    Probes SPARQL endpoints for classes whose instances match bioregistry
    URI patterns and writes skos:narrowMatch mapping JSON-LD files.

    Typical workflow::

        rdfsolve instance-match probe --prefix ensembl -o ensembl_mapping.jsonld
        rdfsolve instance-match seed  --prefixes ensembl uniprot chebi
    """


@instance_match_group.command("probe")
@click.option(
    "--prefix",
    "-p",
    required=True,
    help="Bioregistry prefix to probe (e.g. 'ensembl').",
)
@click.option(
    "--sources",
    default=None,
    show_default=False,
    help=("Path to sources file (JSON-LD or CSV). Default: auto-detect data/sources.jsonld."),
)
@click.option(
    "--sources-csv",
    default=None,
    hidden=True,
    help="Deprecated alias for --sources.",
)
@click.option(
    "--predicate",
    default="http://www.w3.org/2004/02/skos/core#narrowMatch",
    show_default=True,
    help="Mapping predicate URI.",
)
@click.option(
    "--dataset",
    "-d",
    "datasets",
    multiple=True,
    help="Restrict to this dataset name (repeatable).",
)
@click.option(
    "--timeout",
    default=60.0,
    show_default=True,
    type=float,
    help="SPARQL request timeout in seconds.",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Write JSON-LD to this file (default: stdout).",
)
def probe_cmd(
    prefix: str,
    sources: str | None,
    sources_csv: str | None,
    predicate: str,
    datasets: tuple[str, ...],
    timeout: float,
    output: str | None,
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
    "--prefixes",
    "-p",
    "prefix_list",
    required=True,
    multiple=True,
    help="Bioregistry prefix (repeatable).",
)
@click.option(
    "--sources",
    default=None,
    show_default=False,
    help=("Path to sources file (JSON-LD or CSV). Default: auto-detect data/sources.jsonld."),
)
@click.option(
    "--sources-csv",
    default=None,
    hidden=True,
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
    "--dataset",
    "-d",
    "datasets",
    multiple=True,
    help="Restrict to this dataset name (repeatable).",
)
@click.option(
    "--timeout",
    default=60.0,
    show_default=True,
    type=float,
    help="SPARQL request timeout in seconds.",
)
@click.option(
    "--no-skip-existing",
    is_flag=True,
    default=False,
    help="Re-probe even if the output file already exists.",
)
def seed_cmd(
    prefix_list: tuple[str, ...],
    sources: str | None,
    sources_csv: str | None,
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
    r"""SeMRA integration: import external mappings from semra sources.

    Downloads mappings from community sources (biomappings, Gilda, etc.)
    and writes one JSON-LD file per (source, bioregistry-prefix) pair.

    Typical workflow::

        rdfsolve semra import --source biomappings
        rdfsolve semra seed --sources biomappings gilda
    """


@semra_group.command("import")
@click.option(
    "--source",
    "-s",
    required=True,
    help="SeMRA source key (e.g. 'biomappings', 'gilda').",
)
@click.option(
    "--prefix",
    "-p",
    "prefixes",
    multiple=True,
    help=("Keep only these bioregistry prefixes (repeatable). Default: keep all."),
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
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in result["succeeded"]:
        click.echo(f"  OK {s}")
    for f in result["failed"]:
        click.echo(
            f"  FAIL {f.get('source')}/{f.get('prefix')}: {f.get('error')}",
            err=True,
        )
    if result["failed"]:
        raise SystemExit(1)


@semra_group.command("seed")
@click.option(
    "--sources",
    "-s",
    "source_list",
    required=True,
    multiple=True,
    help="SeMRA source key (repeatable).",
)
@click.option(
    "--prefix",
    "-p",
    "prefixes",
    multiple=True,
    help=("Keep only these bioregistry prefixes (repeatable)."),
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
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in result["succeeded"]:
        click.echo(f"  OK {s}")
    for f in result["failed"]:
        click.echo(
            f"  FAIL {f.get('source')}/{f.get('prefix')}: {f.get('error')}",
            err=True,
        )
    if result["failed"]:
        raise SystemExit(1)


# ── inference command group ──────────────────────────────────────


@main.group("inference")
def inference_group() -> None:
    r"""Derive new mappings from existing ones.

    Uses SeMRA inference operations (inversion, transitivity,
    generalisation) to expand a set of mapping JSON-LD files.

    Typical workflow::

        rdfsolve inference run --input file1.jsonld file2.jsonld \
            --output docker/mappings/inferenced/inferred.jsonld
        rdfsolve inference seed
    """


@inference_group.command("run")
@click.option(
    "--input",
    "-i",
    "input_paths",
    required=True,
    multiple=True,
    help="Input mapping JSON-LD file (repeatable).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    required=True,
    help="Output JSON-LD file path.",
)
@click.option(
    "--no-inversion",
    is_flag=True,
    default=False,
    help="Disable inversion inference.",
)
@click.option(
    "--no-transitivity",
    is_flag=True,
    default=False,
    help="Disable transitivity (chain) inference.",
)
@click.option(
    "--generalisation",
    is_flag=True,
    default=False,
    help="Enable generalisation inference (off by default).",
)
@click.option(
    "--chain-cutoff",
    default=3,
    show_default=True,
    type=int,
    help="Maximum chain length for transitivity.",
)
@click.option(
    "--name",
    "dataset_name",
    default=None,
    help="Override @about.dataset_name in the output.",
)
def inference_run_cmd(
    input_paths: tuple[str, ...],
    output_path: str,
    no_inversion: bool,
    no_transitivity: bool,
    generalisation: bool,
    chain_cutoff: int,
    dataset_name: str | None,
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
    except Exception as exc:
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
    "--name",
    "output_name",
    default="inferenced_mappings",
    show_default=True,
    help="Output file stem (without .jsonld).",
)
@click.option(
    "--no-inversion",
    is_flag=True,
    default=False,
    help="Disable inversion inference.",
)
@click.option(
    "--no-transitivity",
    is_flag=True,
    default=False,
    help="Disable transitivity inference.",
)
@click.option(
    "--generalisation",
    is_flag=True,
    default=False,
    help="Enable generalisation inference.",
)
@click.option(
    "--chain-cutoff",
    default=3,
    show_default=True,
    type=int,
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
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    if result["output_path"]:
        click.echo(f"  OK {result['output_edges']} edges -> {result['output_path']}")
    else:
        click.echo("  ⚠ No input files found.", err=True)


if __name__ == "__main__":
    main()
