"""Command line interface for :mod:`rdfsolve`."""

import json
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

    Mining commands::

        rdfsolve discover      Discover VoID descriptions from remote endpoints
        rdfsolve mine          Mine schemas from remote SPARQL endpoints
        rdfsolve local-mine    Mine from a local QLever endpoint
        rdfsolve qleverfile    Generate Qleverfiles for QLever

    Export commands::

        rdfsolve export    Interconvert schemas (JSON-LD, LinkML, SHACL, CSV, ...)

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
# Paths & helpers
# ═══════════════════════════════════════════════════════════════════

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_SOURCES = str(_REPO_ROOT / "data" / "sources.yaml")
_DEFAULT_OUTPUT = str(_REPO_ROOT / "mined_schemas")


def _parse_authors(
    raw: tuple[str, ...] | None,
) -> list[dict[str, str]] | None:
    """Parse ``--author`` values into ``[{name, orcid}]``."""
    if not raw:
        return None
    result: list[dict[str, str]] = []
    for item in raw:
        if "|" in item:
            name, orcid = item.split("|", 1)
            result.append({"name": name.strip(), "orcid": orcid.strip()})
        else:
            result.append({"name": item.strip()})
    return result or None


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
                    err = (
                        entry.get("error", "")[:80]
                        if isinstance(entry, dict)
                        else ""
                    )
                    click.echo(f"    • {ds}: {err}")
    click.echo(f"{'═' * 60}")


# ── Shared option decorators ─────────────────────────────────────


def _common_options(fn: Any) -> Any:
    """Shared options for mining / discover / qleverfile commands."""
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
            "Page size for Phase-1 class discovery "
            "(two-phase mode only). Default: no pagination."
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
        help="Treat untyped URI objects as owl:Class references.",
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
            "Recommended for local QLever endpoints."
        ),
    )(fn)
    return fn


# ═══════════════════════════════════════════════════════════════════
# Top-level mining commands (discover, mine, local-mine, qleverfile)
# ═══════════════════════════════════════════════════════════════════


@main.command("discover")
@_common_options
def cmd_discover(
    sources: str,
    output_dir: str,
    fmt: str,
    timeout: float,
    name_filter: str | None,
    benchmark: bool,
) -> None:
    r"""Discover existing VoID descriptions from remote endpoints.

    Queries every source endpoint for pre-existing VoID partitions
    and exports VoID / JSON-LD / LinkML / SHACL artefacts.

    Examples::

        rdfsolve discover
        rdfsolve discover --filter "chembl|drugbank"
        rdfsolve discover --output-dir ./discovered
    """
    from .api import discover_void_source, load_sources

    entries = load_sources(sources, name_filter=name_filter)
    discovered: list[str] = []
    empty: list[str] = []
    failed: list[dict[str, str]] = []
    skipped: list[str] = []

    total = len(entries)
    for idx, entry in enumerate(entries, 1):
        ename = entry.get("name", "")
        endpoint = entry.get("endpoint", "")
        if not endpoint:
            skipped.append(ename)
            continue
        try:
            res = discover_void_source(
                endpoint, ename, output_dir,
                tag="discovered_remote",
                entry=entry,
                fmt=fmt,
            )
            if res["partitions_found"]:
                discovered.append(ename)
                click.echo(
                    f"  [{idx}/{total}] {ename}: "
                    f"{res['partitions_found']} partitions"
                )
            else:
                empty.append(ename)
        except Exception as exc:
            msg = str(exc)[:120]
            click.echo(f"  [{idx}/{total}] {ename}: FAIL {msg}")
            failed.append({"dataset": ename, "error": msg})

    _print_result({
        "discovered": discovered,
        "empty": empty,
        "failed": failed,
        "skipped": skipped,
    })


@main.command("mine")
@_common_options
@_mining_options
def cmd_mine(
    sources: str,
    output_dir: str,
    fmt: str,
    timeout: float,
    name_filter: str | None,
    benchmark: bool,
    chunk_size: int,
    class_chunk_size: int | None,
    class_batch_size: int,
    no_counts: bool,
    untyped_as_classes: bool,
    authors_raw: tuple[str, ...],
    one_shot: bool,
) -> None:
    r"""Mine schemas from remote SPARQL endpoints.

    Standard mining workflow: iterate endpoints, extract schema
    patterns, write JSON-LD schemas, VoID turtle, and analytics
    reports.

    Examples::

        rdfsolve mine
        rdfsolve mine --filter "drugbank"
        rdfsolve mine --benchmark
    """
    from .api import mine_all_sources

    authors = _parse_authors(authors_raw)

    # Apply name_filter by writing a temporary filtered sources file.
    sources_path: str = sources
    if name_filter:
        from .api import load_sources

        entries = load_sources(sources, name_filter=name_filter)
        if not entries:
            click.echo("No sources match the filter.")
            return

        import tempfile
        import yaml

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, prefix="rdfsolve_filtered_",
        )
        yaml.dump(
            [dict(e) for e in entries], tmp,
            default_flow_style=False, allow_unicode=True,
        )
        tmp.close()
        sources_path = tmp.name

    def _on_progress(
        name: str, idx: int, total: int, error: str | None,
    ) -> None:
        if error and error != "skipped":
            click.echo(f"  [{idx}/{total}] {name}: FAIL {error[:120]}")
        elif not error:
            click.echo(f"  [{idx}/{total}] {name}: OK")

    result = mine_all_sources(
        sources=sources_path,
        output_dir=output_dir,
        fmt=fmt,
        chunk_size=chunk_size,
        class_chunk_size=class_chunk_size,
        class_batch_size=class_batch_size,
        timeout=timeout,
        counts=not no_counts,
        reports=True,
        untyped_as_classes=untyped_as_classes,
        authors=authors,
        on_progress=_on_progress,
    )
    _print_result(result)


@main.command("local-mine")
@_common_options
@_mining_options
@click.option(
    "--graph-uri",
    "graph_uris",
    multiple=True,
    default=None,
    help=(
        "Named graph URI to scope mining queries to (repeatable). "
        "When omitted, graph_uris from sources.yaml are used. "
        "Pass '--graph-uri none' to mine all graphs."
    ),
)
@click.option(
    "--endpoint",
    default="http://localhost:7001",
    help="Local QLever SPARQL endpoint URL.",
)
@click.option("--name", default=None, help="Dataset name (single-dataset mode).")
@click.option(
    "--discover-first", is_flag=True, help="Run VoID discovery before mining.",
)
@click.option(
    "--void-uri-base",
    default=None,
    help="Base URI for generated VoID partition IRIs.",
)
@click.option(
    "--test", is_flag=True, help="Process only the 3 smallest downloadable sources.",
)
def cmd_local_mine(
    sources: str,
    output_dir: str,
    fmt: str,
    timeout: float,
    name_filter: str | None,
    benchmark: bool,
    chunk_size: int,
    class_chunk_size: int | None,
    class_batch_size: int,
    no_counts: bool,
    untyped_as_classes: bool,
    authors_raw: tuple[str, ...],
    one_shot: bool,
    graph_uris: tuple[str, ...],
    endpoint: str,
    name: str | None,
    discover_first: bool,
    void_uri_base: str | None,
    test: bool,
) -> None:
    r"""Mine schemas from a local QLever endpoint.

    Use after downloading data and running ``qlever index && qlever
    start`` for a dataset.

    Examples::

        rdfsolve local-mine --name drugbank \
            --endpoint http://localhost:7026 --discover-first
        rdfsolve local-mine --test
    """
    from .api import (
        _select_test_sources,
        fetch_qlever_stats,
        load_sources,
        mine_local_source,
    )

    authors = _parse_authors(authors_raw)

    # Normalise graph_uris from Click's tuple
    _graph_uris: list[str] | None = None
    if graph_uris:
        if len(graph_uris) == 1 and graph_uris[0].lower() == "none":
            _graph_uris = None
        else:
            _graph_uris = list(graph_uris)

    succeeded: list[str] = []
    failed: list[dict[str, str]] = []

    def _mine_one(
        ds_name: str, ep: str,
        entry: dict[str, Any] | None = None,
        guris: list[str] | None = None,
    ) -> None:
        qlever_v = fetch_qlever_stats(ep, timeout=min(timeout, 10.0))
        mine_local_source(
            ep, ds_name, output_dir,
            graph_uris=guris or _graph_uris,
            void_uri_base=void_uri_base,
            entry=entry,
            chunk_size=chunk_size,
            class_batch_size=class_batch_size,
            class_chunk_size=class_chunk_size,
            timeout=timeout,
            counts=not no_counts,
            one_shot=one_shot,
            untyped_as_classes=untyped_as_classes,
            fmt=fmt,
            authors=authors,
            discover_first=discover_first,
            qlever_version=qlever_v,
        )

    # ── Single-dataset mode ──────────────────────────────────────
    if name and not test:
        try:
            _mine_one(name, endpoint)
            succeeded.append(name)
            click.echo(f"  {name}: OK")
        except Exception as exc:
            msg = str(exc)[:120]
            click.echo(f"  {name}: FAIL {msg}")
            failed.append({"dataset": name, "error": msg})

        _print_result({"succeeded": succeeded, "failed": failed, "skipped": []})
        return

    # ── Batch mode ───────────────────────────────────────────────
    entries = load_sources(sources, name_filter=name_filter)

    if test:
        entries = _select_test_sources(entries)
        if not entries:
            click.echo("No downloadable sources for test mode.")
            return

    batch = []
    for e in entries:
        ep = e.get("local_endpoint", "")
        if ep:
            batch.append((e, ep))
        elif test:
            batch.append((e, endpoint))

    total = len(batch)
    for idx, (entry, ep) in enumerate(batch, 1):
        ename = entry.get("name", "")
        try:
            _mine_one(ename, ep, entry=entry)
            succeeded.append(ename)
            click.echo(f"  [{idx}/{total}] {ename}: OK")
        except Exception as exc:
            msg = str(exc)[:120]
            click.echo(f"  [{idx}/{total}] {ename}: FAIL {msg}")
            failed.append({"dataset": ename, "error": msg})

    _print_result({"succeeded": succeeded, "failed": failed, "skipped": []})


@main.command("qleverfile")
@_common_options
@click.option("--data-dir", required=True, help="Root directory where RDF dumps live.")
@click.option("--base-port", type=int, default=7019, help="First port number.")
@click.option(
    "--test", is_flag=True, help="Generate only for 3 smallest downloadable sources.",
)
@click.option(
    "--runtime",
    type=click.Choice(["docker", "native"]),
    default="docker",
    help="QLever runtime.",
)
@click.option(
    "--server-memory",
    "server_memory",
    default="40G",
    show_default=True,
    help=(
        "MEMORY_FOR_QUERIES written into every Qleverfile (-m flag for qlever-server). "
        "Lower this when many servers run concurrently on one node, e.g. '8G'."
    ),
)
def cmd_qleverfile(
    sources: str,
    output_dir: str,
    fmt: str,
    timeout: float,
    name_filter: str | None,
    benchmark: bool,
    data_dir: str,
    base_port: int,
    test: bool,
    runtime: str,
    server_memory: str,
) -> None:
    r"""Generate Qleverfiles for local QLever mining.

    Creates a Qleverfile for each source with download URLs.

    Examples::

        rdfsolve qleverfile --data-dir /data/rdf
        rdfsolve qleverfile --data-dir /data/rdf --test
        rdfsolve qleverfile --data-dir /data/rdf --server-memory 8G
    """
    from .api import generate_qleverfiles

    result = generate_qleverfiles(
        sources_path=sources,
        data_dir=data_dir,
        base_port=base_port,
        runtime=runtime,
        name_filter=name_filter,
        test=test,
        server_memory=server_memory,
    )
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
# bioregistry command
# ---------------------------------------------------------------------------


@main.command("bioregistry-enrich")
@click.option(
    "--sources",
    "-s",
    default="data/sources.yaml",
    show_default=True,
    help="Path to sources YAML file.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print resolved prefixes but do not write the file.",
)
@click.option(
    "--name",
    "-n",
    "names",
    multiple=True,
    help="Only process entries whose 'name' matches (repeatable).",
)
def bioregistry_enrich_cmd(
    sources: str,
    dry_run: bool,
    names: tuple[str, ...],
) -> None:
    """Enrich sources.yaml with Bioregistry metadata in-place.

    Reads the sources YAML, resolves the canonical Bioregistry prefix
    for each entry, and writes the resolved ``bioregistry_*`` fields
    back into the file, preserving existing keys and order.
    """
    import yaml as _yaml

    from rdfsolve.sources import SourceEntry, enrich_source_with_bioregistry

    _BIOREGISTRY_KEYS = [
        "bioregistry_prefix",
        "bioregistry_name",
        "bioregistry_description",
        "bioregistry_homepage",
        "bioregistry_license",
        "bioregistry_domain",
        "bioregistry_uri_prefix",
        "bioregistry_uri_prefixes",
        "bioregistry_logo",
        "keywords",
        "bioregistry_synonyms",
        "bioregistry_publications",
        "bioregistry_extra_providers",
        "bioregistry_mappings",
    ]

    sources_path = Path(sources)
    if not sources_path.exists():
        click.echo(f"Error: file not found: {sources_path}", err=True)
        raise click.Abort()

    nodes: list[dict] = _yaml.safe_load(sources_path.read_text(encoding="utf-8")) or []
    if not isinstance(nodes, list):
        click.echo("Error: expected a YAML list of source mappings.", err=True)
        raise click.Abort()

    name_filter: set[str] | None = set(names) if names else None

    resolved = 0
    skipped = 0
    for node in nodes:
        name = node.get("name", "")
        if name_filter and name not in name_filter:
            continue

        # Build a minimal SourceEntry for the resolver
        entry: SourceEntry = {}  # type: ignore[assignment]
        for key in ("name", "void_iri", "endpoint", "graph_uris"):
            if key in node:
                entry[key] = node[key]  # type: ignore[literal-required]

        prefix = enrich_source_with_bioregistry(entry)

        if prefix:
            # Copy resolved fields back into the raw YAML node
            for key in _BIOREGISTRY_KEYS:
                if key in entry:
                    node[key] = entry[key]  # type: ignore[literal-required]
                else:
                    node.pop(key, None)
            resolved += 1
            click.echo(f"  {name!r:40s} → {prefix}")
        else:
            skipped += 1

    click.echo(f"\nResolved: {resolved}  |  No match: {skipped}")

    if dry_run:
        click.echo("(dry-run: file not written)")
        return

    with open(sources_path, "w", encoding="utf-8") as fh:
        _yaml.dump(
            nodes,
            fh,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    click.echo(f"Written: {sources_path}")


# ---------------------------------------------------------------------------
# build-graphs command
# ---------------------------------------------------------------------------


@main.command("build-graphs")
@click.option(
    "--schemas-dir",
    default="docker/schemas",
    show_default=True,
    help="Root schemas directory.",
)
@click.option(
    "--mappings-dir",
    default="docker/mappings",
    show_default=True,
    help="Root mappings directory.",
)
@click.option(
    "--output-dir",
    default="results/paper_data",
    show_default=True,
    help="paper_data output root.",
)
@click.option(
    "--datasets",
    multiple=True,
    help="Only process these dataset names (shell-style globs, repeatable).",
)
@click.option(
    "--schema-only",
    is_flag=True,
    default=False,
    help="Run schema selection only; skip graph construction.",
)
@click.option(
    "--no-copy-schemas",
    is_flag=True,
    default=False,
    help="Skip copying selected schemas to output-dir/schemas/.",
)
def build_graphs_cmd(
    schemas_dir: str,
    mappings_dir: str,
    output_dir: str,
    datasets: tuple[str, ...],
    schema_only: bool,
    no_copy_schemas: bool,
) -> None:
    """Select canonical schemas and build dataset-level connectivity graphs.

    Performs pipeline step 4b (schema selection) and step 12 (graph
    construction).  Exports edge/node Parquet tables, strategy/predicate
    counts, and metadata.json.
    """
    from rdfsolve.api import run_graph_pipeline

    try:
        result = run_graph_pipeline(
            schemas_dir=schemas_dir,
            mappings_dir=mappings_dir,
            output_dir=output_dir,
            datasets=list(datasets) if datasets else None,
            schema_only=schema_only,
            copy_schemas=not no_copy_schemas,
        )
    except FileNotFoundError as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    meta = result.get("metadata", {})
    click.echo(f"  Datasets: {meta.get('n_datasets', len(meta.get('datasets', [])))}")
    if not schema_only:
        for gname in ("G_schema", "G_raw", "G_inferred"):
            g = meta.get(gname, {})
            click.echo(f"  {gname}: {g.get('nodes', '?')} nodes, {g.get('edges', '?')} edges")
    click.echo(f"  Output: {output_dir}")


# ---------------------------------------------------------------------------
# ontology-index command
# ---------------------------------------------------------------------------


@main.command("ontology-index")
@click.option(
    "--ontologies",
    multiple=True,
    help="Explicit OLS4 ontology IDs to index (repeatable). Default: all.",
)
@click.option(
    "--from-schemas",
    "from_schemas_dir",
    default=None,
    help="Directory with *_schema.jsonld files to restrict indexing.",
)
@click.option(
    "--from-sources",
    "from_sources_yaml",
    default=None,
    help="Sources YAML whose URI prefixes restrict indexing.",
)
@click.option(
    "--cache-dir",
    default=None,
    help="Directory for the OLS4 HTTP-response diskcache.",
)
@click.option(
    "--data-dir",
    default=None,
    help="Directory to write ontology_index.pkl.gz / ontology_graph.graphml.",
)
@click.option(
    "--db",
    default=None,
    help="Path to rdfsolve SQLite database for DB persistence.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Build the index but do not write any files or DB rows.",
)
def ontology_index_cmd(
    ontologies: tuple[str, ...],
    from_schemas_dir: str | None,
    from_sources_yaml: str | None,
    cache_dir: str | None,
    data_dir: str | None,
    db: str | None,
    dry_run: bool,
) -> None:
    """Build and persist an OntologyIndex from OLS4 metadata.

    Indexes ontology class hierarchies from the EBI Ontology Lookup
    Service (OLS4) and persists the result to disk and/or database.
    """
    from rdfsolve.api import build_ontology_index, save_ontology_index

    # Collect class URIs from schemas or sources if requested
    schema_class_uris: set[str] | None = None
    explicit_ids: list[str] | None = list(ontologies) if ontologies else None

    if from_schemas_dir:
        from rdfsolve.graphs import collect_schemas  # noqa: F811

        # Use a lightweight helper to extract IRIs
        import json as _json

        from rdfsolve._uri import expand_curie

        root = Path(from_schemas_dir)
        uris: set[str] = set()
        context: dict[str, str] = {}
        for sf in sorted(root.rglob("*_schema.jsonld")):
            try:
                doc = _json.loads(sf.read_text(encoding="utf-8"))
            except Exception:
                continue
            raw_ctx = doc.get("@context", {})
            ctx: dict[str, str] = {}
            if isinstance(raw_ctx, list):
                for item in raw_ctx:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            if isinstance(v, str):
                                ctx[k] = v
            elif isinstance(raw_ctx, dict):
                for k, v in raw_ctx.items():
                    if isinstance(v, str):
                        ctx[k] = v
            context.update(ctx)
            for node in doc.get("@graph", []):
                raw_id = node.get("@id") if isinstance(node, dict) else None
                if isinstance(raw_id, str) and raw_id:
                    expanded = expand_curie(raw_id, ctx)
                    if expanded.startswith(("http://", "https://")):
                        uris.add(expanded)
        schema_class_uris = uris or None

    elif from_sources_yaml:
        from rdfsolve.sources import load_sources as _load

        entries = _load(Path(from_sources_yaml))
        uris = set()
        for entry in entries:
            for key in ("uri_prefix", "bioregistry_uri_prefix"):
                val = entry.get(key)
                if isinstance(val, str) and val:
                    uris.add(val)
            for val in entry.get("bioregistry_uri_prefixes", []) or []:
                if isinstance(val, str) and val:
                    uris.add(val)
        schema_class_uris = uris or None

    try:
        idx = build_ontology_index(
            schema_class_uris=schema_class_uris,
            cache_dir=cache_dir,
            ontology_ids=explicit_ids,
        )
    except Exception as exc:
        click.echo(f"Error building index: {exc}", err=True)
        raise click.Abort()

    stats = idx.stats()
    click.echo("\n── OntologyIndex statistics ──")
    for key, val in stats.items():
        click.echo(f"  {key:<20}: {val:,}")

    if dry_run:
        click.echo("(dry-run: nothing written)")
        return

    wrote = False
    if db:
        try:
            from rdfsolve.api import save_ontology_index_to_db
            from rdfsolve.backend.database import Database

            _db = Database(db)
            save_ontology_index_to_db(idx, _db)
            click.echo(f"  Persisted to database: {db}")
            wrote = True
        except Exception as exc:
            click.echo(f"Error saving to DB: {exc}", err=True)
            raise click.Abort()

    if data_dir:
        try:
            save_ontology_index(idx, data_dir)
            click.echo(f"  Written to: {data_dir}/")
            wrote = True
        except Exception as exc:
            click.echo(f"Error saving to disk: {exc}", err=True)
            raise click.Abort()

    if not wrote:
        click.echo(
            "⚠ Index built but not persisted. "
            "Pass --db and/or --data-dir to save it.",
            err=True,
        )


# ---------------------------------------------------------------------------
# qlever-boot command
# ---------------------------------------------------------------------------


@main.command("qlever-boot")
@click.option(
    "--source",
    "sources",
    multiple=True,
    help="Source name (repeatable). Must match 'name' in sources.yaml.",
)
@click.option(
    "--filter",
    "name_filter",
    default=None,
    help="Regex filter applied to source names.",
)
@click.option(
    "--sources-yaml",
    default="data/sources.yaml",
    show_default=True,
    help="Path to sources.yaml.",
)
@click.option(
    "--list-sources",
    is_flag=True,
    default=False,
    help="List downloadable sources and exit.",
)
@click.option(
    "--step",
    default="all",
    show_default=True,
    type=click.Choice(
        ["all", "setup", "get-data", "index", "start", "stop", "index-start", "setup-data"],
        case_sensitive=False,
    ),
    help="Which pipeline step(s) to run.",
)
@click.option("--data-dir", default="data", show_default=True, help="Root data directory.")
@click.option("--port", type=int, default=7019, show_default=True, help="Base SPARQL server port.")
@click.option(
    "--runtime",
    default="native",
    show_default=True,
    type=click.Choice(["native", "docker"]),
    help="QLever runtime for the Qleverfile.",
)
@click.option("--singularity-image", default="./data/qlever.sif", show_default=True)
@click.option("--docker-ref", default="docker://adfreiburg/qlever:latest", show_default=True)
@click.option("--memory-for-queries", default="500G", show_default=True)
@click.option("--timeout", "qlever_timeout", default="9999999999s", show_default=True)
@click.option("--parser-buffer-size", default="8GB", show_default=True)
@click.option("--parallel-parsing", is_flag=True, default=False)
@click.option("--num-triples-per-batch", type=int, default=1_000_000, show_default=True)
@click.option("--qlever-image", default="docker.io/adfreiburg/qlever:latest", show_default=True)
@click.option("--num-threads", type=int, default=8, show_default=True)
@click.option("--cache-size", default="8G", show_default=True)
@click.option("--server-memory", default="40G", show_default=True)
@click.option("--wait-timeout", type=int, default=120, show_default=True)
def qlever_boot_cmd(
    sources: tuple[str, ...],
    name_filter: str | None,
    sources_yaml: str,
    list_sources: bool,
    step: str,
    data_dir: str,
    port: int,
    runtime: str,
    singularity_image: str,
    docker_ref: str,
    memory_for_queries: str,
    qlever_timeout: str,
    parser_buffer_size: str,
    parallel_parsing: bool,
    num_triples_per_batch: int,
    qlever_image: str,
    num_threads: int,
    cache_size: str,
    server_memory: str,
    wait_timeout: int,
) -> None:
    """Boot QLever SPARQL endpoint(s) via Singularity.

    Generates a Qleverfile, downloads data, builds the index, and starts
    the SPARQL server for one or more sources from sources.yaml.
    """
    if list_sources:
        from rdfsolve.api import list_qlever_sources

        rows = list_qlever_sources(sources_yaml)
        click.echo(f"{'Name':<40} {'Format':<15} {'Provider'}")
        click.echo("─" * 70)
        for r in rows:
            click.echo(f"{r['name']:<40} {r['format']:<15} {r['provider']}")
        click.echo(f"\nTotal: {len(rows)} downloadable sources.")
        return

    from rdfsolve.api import boot_qlever_sources

    try:
        results = boot_qlever_sources(
            sources_yaml,
            source_names=list(sources) if sources else None,
            name_filter=name_filter,
            step=step,
            data_dir=data_dir,
            base_port=port,
            runtime=runtime,
            singularity_image=singularity_image,
            docker_ref=docker_ref,
            memory_for_queries=memory_for_queries,
            timeout=qlever_timeout,
            parser_buffer_size=parser_buffer_size,
            parallel_parsing=parallel_parsing,
            num_triples_per_batch=num_triples_per_batch,
            qlever_image=qlever_image,
            num_threads=num_threads,
            cache_size=cache_size,
            server_memory=server_memory,
            wait_timeout=wait_timeout,
        )
    except (FileNotFoundError, ValueError) as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    ok = [r for r in results if r["status"] == "ok"]
    failed = [r for r in results if r["status"] == "failed"]
    click.echo(f"\n  Done: {len(ok)} succeeded, {len(failed)} failed")
    for r in ok:
        click.echo(f"  ✓ {r['name']:<35} {r.get('endpoint', '—')}")
    for r in failed:
        click.echo(f"  ✗ {r['name']:<35} {r.get('error', '?')[:40]}")

    if failed:
        raise SystemExit(1)


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


@instance_match_group.command("discover-prefixes")
@click.option(
    "--mapping-dir",
    "-d",
    "mapping_dirs",
    required=True,
    multiple=True,
    help=(
        "Directory to scan for JSON-LD mapping files (repeatable). "
        "Pass each of sssom/, semra/, instance_matching/ etc."
    ),
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Write prefix list to this file (one per line). Default: stdout.",
)
def discover_prefixes_cmd(
    mapping_dirs: tuple[str, ...],
    output: str | None,
) -> None:
    """Discover entity prefixes from mapping JSON-LD files.

    Scans the given directories for *.jsonld files, extracts all unique
    entity CURIE prefixes (e.g. 'mesh', 'chebi', 'ensembl'), validates
    them against bioregistry, and prints the sorted list.  Use this to
    dynamically determine the --prefixes list for ``instance-match seed``.

    Example::

        rdfsolve instance-match discover-prefixes \\
            -d output/mappings/sssom \\
            -d output/mappings/semra
    """
    from rdfsolve.instance_matcher import discover_mapping_prefixes

    prefixes = discover_mapping_prefixes(*mapping_dirs)
    text = "\n".join(prefixes)
    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(text + "\n")
        click.echo(
            f"Wrote {len(prefixes)} prefixes to {output}",
            err=True,
        )
    else:
        click.echo(text)
    # Always print a human-readable summary to stderr so it shows up in logs
    # regardless of whether --output was used.
    click.echo(
        f"discover-prefixes: {len(prefixes)} valid prefixes from {len(mapping_dirs)} dir(s)",
        err=True,
    )
    if prefixes:
        click.echo(f"  prefixes: {', '.join(prefixes)}", err=True)
    else:
        click.echo("  (none found — check that mapping dirs contain *.jsonld files)", err=True)


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
    "--delay",
    "inter_request_delay",
    default=0.0,
    show_default=True,
    type=float,
    help=(
        "Seconds to sleep between successive SPARQL requests to the same endpoint. "
        "Set >0 for public remote endpoints; leave 0 for local QLever."
    ),
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
    inter_request_delay: float,
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
            inter_request_delay=inter_request_delay,
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
@click.option(
    "--ports-json",
    default=None,
    help=(
        "Path to QLever ports.json ({name: port}).  When supplied, "
        "queries go to local QLever endpoints instead of remote SPARQL."
    ),
)
@click.option(
    "--delay",
    "inter_request_delay",
    default=0.0,
    show_default=True,
    type=float,
    help=(
        "Seconds to sleep between successive SPARQL requests to the same endpoint. "
        "Set >0 (e.g. 2.0) for public remote endpoints; leave 0 for local QLever."
    ),
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
    ports_json: str | None,
    inter_request_delay: float,
) -> None:
    """Seed mapping files for multiple bioregistry resources.

    Writes {PREFIX}_instance_mapping.jsonld to OUTPUT_DIR for each
    supplied PREFIX.  Existing files are skipped unless --no-skip-existing
    is passed.

    Use --delay for public remote endpoints to avoid overwhelming them.
    When --ports-json is given (local QLever), --delay defaults to 0.
    """
    from rdfsolve.api import seed_instance_mappings

    n_total = len(prefix_list)
    click.echo(
        f"seed: {n_total} prefix(es) to probe"
        + (f" against dataset(s): {', '.join(datasets)}" if datasets else "")
        + (f"  [ports-json={ports_json}]" if ports_json else ""),
        err=True,
    )

    try:
        result = seed_instance_mappings(
            prefixes=list(prefix_list),
            sources=sources or sources_csv,
            output_dir=output_dir,
            predicate=predicate,
            dataset_names=list(datasets) if datasets else None,
            timeout=timeout,
            skip_existing=not no_skip_existing,
            ports_json=ports_json,
            inter_request_delay=inter_request_delay,
        )
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for i, p in enumerate(result["succeeded"], start=1):
        click.echo(f"  [{i}/{n_total}] OK   {p}")
    for f in result["failed"]:
        click.echo(f"  FAIL {f['prefix']}: {f['error']}", err=True)

    n_ok = len(result["succeeded"])
    n_fail = len(result["failed"])
    click.echo(
        f"seed complete: {n_ok}/{n_total} succeeded, {n_fail} failed",
        err=True,
    )

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
@click.option(
    "--mapping-type",
    default="instance",
    show_default=True,
    type=click.Choice(["instance", "class"], case_sensitive=False),
    help=(
        "Mapping type to store in @about.mapping_type. "
        "Use 'instance' for entity-level mappings (default) "
        "or 'class' for concept/class mappings."
    ),
)
def semra_import_cmd(
    source: str,
    prefixes: tuple[str, ...],
    output_dir: str,
    mapping_type: str,
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
            mapping_type=mapping_type,
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
    help=(
        "SeMRA source key (repeatable). Pass 'all' to import every "
        "registered source."
    ),
)
@click.option(
    "--exclude",
    "-x",
    "exclude_list",
    multiple=True,
    help=(
        "SeMRA source key to exclude (repeatable). "
        "Useful with --sources all to skip sources that are known to "
        "fail (e.g. 'clo' requires Java, 'wikidata' hits 502 errors)."
    ),
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
@click.option(
    "--mapping-type",
    default="instance",
    show_default=True,
    type=click.Choice(["instance", "class"], case_sensitive=False),
    help=(
        "Mapping type to store in @about.mapping_type. "
        "Use 'instance' for entity-level mappings (default) "
        "or 'class' for concept/class mappings."
    ),
)
def semra_seed_cmd(
    source_list: tuple[str, ...],
    exclude_list: tuple[str, ...],
    prefixes: tuple[str, ...],
    output_dir: str,
    mapping_type: str,
) -> None:
    """Seed mapping files from multiple SeMRA sources.

    Pass ``--sources all`` to import every registered SeMRA source.
    Use ``--exclude clo --exclude wikidata`` to skip problematic sources.
    """
    from rdfsolve.api import seed_semra_mappings

    # Expand "all" to the full registered-source list.
    # NOTE: 'clo' (requires Java) and 'wikidata' (unreliable SPARQL
    # endpoint) are excluded from 'all' by default.  Pass them
    # explicitly with ``--sources clo`` if you really need them.
    _ALL_SOURCES = [
        "fplx", "pubchemmesh", "ncitchebi", "ncithgnc", "ncitgo",
        "ncituniprot", "biomappingspositive", "gilda",
        "omimgene", "cbms2019", "compath", "rdfsolve_instance",
    ]
    sources: list[str] = []
    for s in source_list:
        if s.lower() == "all":
            sources.extend(_ALL_SOURCES)
        else:
            sources.append(s)
    # Deduplicate while preserving order
    seen: set[str] = set()
    sources = [s for s in sources if not (s in seen or seen.add(s))]  # type: ignore[func-returns-value]

    # Apply exclusions
    if exclude_list:
        excl = {e.lower() for e in exclude_list}
        excluded = [s for s in sources if s.lower() in excl]
        sources = [s for s in sources if s.lower() not in excl]
        for e in excluded:
            click.echo(f"  SKIP {e} (excluded via --exclude)")

    try:
        result = seed_semra_mappings(
            sources=sources,
            keep_prefixes=list(prefixes) if prefixes else None,
            output_dir=output_dir,
            mapping_type=mapping_type,
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


# ── sssom command group ──────────────────────────────────────────


@main.group("sssom")
def sssom_group() -> None:
    r"""SSSOM integration: import SSSOM mapping bundles.

    Downloads SSSOM TSV bundles from the URLs listed in
    ``data/sssom_sources.yaml`` and writes one JSON-LD file per
    ``.sssom.tsv`` file extracted.

    Typical workflow::

        rdfsolve sssom import --name ols_mappings
        rdfsolve sssom seed
    """


@sssom_group.command("import")
@click.option(
    "--name",
    "-n",
    required=True,
    help="Source name as defined in sssom_sources.yaml.",
)
@click.option(
    "--url",
    default=None,
    help=("Override the URL for this source (default: read from YAML)."),
)
@click.option(
    "--sources-yaml",
    default="data/sssom_sources.yaml",
    show_default=True,
    help="Path to the SSSOM sources YAML file.",
)
@click.option(
    "--output-dir",
    default="docker/mappings/sssom",
    show_default=True,
    help="Directory to write JSON-LD files.",
)
@click.option(
    "--mapping-type",
    default="instance",
    show_default=True,
    type=click.Choice(["instance", "class"], case_sensitive=False),
    help=(
        "Mapping type to store in @about.mapping_type. "
        "Use 'instance' for entity-level mappings (default) "
        "or 'class' for concept/class mappings."
    ),
)
def sssom_import_cmd(
    name: str,
    url: str | None,
    sources_yaml: str,
    output_dir: str,
    mapping_type: str,
) -> None:
    """Import one SSSOM source by name."""
    import yaml as _yaml

    from rdfsolve.api import import_sssom_source

    yaml_path = Path(sources_yaml)
    entries: list[dict[str, str]] = []
    if yaml_path.exists():
        entries = _yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or []

    entry = next((e for e in entries if e.get("name") == name), None)
    if entry is None:
        if url is None:
            click.echo(
                f"Error: source '{name}' not found in {sources_yaml} and no --url given.",
                err=True,
            )
            raise click.Abort()
        entry = {"name": name, "url": url}
    elif url is not None:
        entry = dict(entry)
        entry["url"] = url

    try:
        result = import_sssom_source(
            entry=entry,
            output_dir=output_dir,
            mapping_type=mapping_type,
        )
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in result["succeeded"]:
        click.echo(f"  OK {s}")
    for f in result["failed"]:
        click.echo(
            f"  FAIL {f.get('file', f.get('source', '?'))}: {f.get('error')}",
            err=True,
        )
    if result["failed"]:
        raise SystemExit(1)


@sssom_group.command("seed")
@click.option(
    "--sources-yaml",
    default="data/sssom_sources.yaml",
    show_default=True,
    help="Path to the SSSOM sources YAML file.",
)
@click.option(
    "--output-dir",
    default="docker/mappings/sssom",
    show_default=True,
    help="Directory to write JSON-LD files.",
)
@click.option(
    "--property-mappings-dir",
    default=None,
    help=(
        "Output directory for entries with type: property_mappings. "
        "Defaults to <output-dir>/../property_mappings/."
    ),
)
@click.option(
    "--name",
    "-n",
    "names",
    multiple=True,
    help=("Process only sources with these names (repeatable). Default: process all."),
)
@click.option(
    "--mapping-type",
    default="instance",
    show_default=True,
    type=click.Choice(["instance", "class"], case_sensitive=False),
    help=(
        "Mapping type to store in @about.mapping_type. "
        "Use 'instance' for entity-level mappings (default) "
        "or 'class' for concept/class mappings."
    ),
)
def sssom_seed_cmd(
    sources_yaml: str,
    output_dir: str,
    property_mappings_dir: str | None,
    names: tuple[str, ...],
    mapping_type: str,
) -> None:
    """Seed SSSOM mapping files for all (or selected) sources.

    Entries whose ``type`` is ``property_mappings`` are routed to
    ``--property-mappings-dir`` (default: ``<output-dir>/../property_mappings/``).
    All other entries are written to ``--output-dir``.
    """
    import yaml as _yaml

    from rdfsolve.api import seed_sssom_mappings

    prop_dir = property_mappings_dir or str(Path(output_dir).parent / "property_mappings")

    # Load the YAML to split entries by type
    yaml_path = Path(sources_yaml)
    if not yaml_path.exists():
        click.echo(f"Error: file not found: {yaml_path}", err=True)
        raise click.Abort()

    all_entries: list[dict] = _yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or []
    name_set = set(names) if names else None
    if name_set:
        all_entries = [e for e in all_entries if e.get("name") in name_set]

    class_entries = [e for e in all_entries if e.get("type") != "property_mappings"]
    prop_entries = [e for e in all_entries if e.get("type") == "property_mappings"]

    all_succeeded: list[str] = []
    all_failed: list[dict] = []

    try:
        if class_entries:
            result = seed_sssom_mappings(
                sssom_sources_yaml=sources_yaml,
                output_dir=output_dir,
                names=[e["name"] for e in class_entries],
                mapping_type=mapping_type,
            )
            all_succeeded.extend(result.get("succeeded", []))
            all_failed.extend(result.get("failed", []))

        if prop_entries:
            result = seed_sssom_mappings(
                sssom_sources_yaml=sources_yaml,
                output_dir=prop_dir,
                names=[e["name"] for e in prop_entries],
                mapping_type=mapping_type,
            )
            all_succeeded.extend(result.get("succeeded", []))
            all_failed.extend(result.get("failed", []))
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    for s in all_succeeded:
        click.echo(f"  OK {s}")
    for f in all_failed:
        click.echo(
            f"  FAIL {f.get('file', f.get('source', '?'))}: {f.get('error')}",
            err=True,
        )
    if all_failed:
        raise SystemExit(1)


# ── instance-match derive ────────────────────────────────────────


@instance_match_group.command("derive")
@click.option(
    "--input",
    "-i",
    "input_paths",
    required=True,
    multiple=True,
    help="Input instance-mapping JSON-LD file (repeatable).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    required=True,
    help="Output JSON-LD file for class-derived mappings.",
)
@click.option(
    "--endpoint",
    "-e",
    "endpoint_url",
    default="",
    help="SPARQL / QLever endpoint URL for class lookup (single-endpoint mode).",
)
@click.option(
    "--ports-json",
    "ports_json_path",
    default=None,
    type=click.Path(exists=True),
    help="Path to ports.json for multi-endpoint mode (one QLever per dataset).",
)
@click.option(
    "--batch-size",
    default=50,
    show_default=True,
    type=int,
    help="Number of IRIs per VALUES query.",
)
@click.option(
    "--timeout",
    default=60.0,
    show_default=True,
    type=float,
    help="Per-request SPARQL timeout in seconds.",
)
@click.option(
    "--min-count",
    "min_instance_count",
    default=1,
    show_default=True,
    type=int,
    help="Minimum instance pairs to retain a class pair.",
)
@click.option(
    "--min-confidence",
    default=0.0,
    show_default=True,
    type=float,
    help="Minimum confidence score to retain a class pair.",
)
@click.option(
    "--cache-index",
    is_flag=True,
    default=False,
    help=("Cache the class index to disk and reuse on subsequent runs."),
)
@click.option(
    "--index-cache-path",
    default=None,
    help=("Explicit path for the cached index JSON. Defaults to {output}.class_index_cache.json."),
)
@click.option(
    "--enrich",
    "enrich_in_place",
    is_flag=True,
    default=False,
    help=("Write enriched copies of input files alongside the originals ({stem}.enriched.jsonld)."),
)
@click.option(
    "--name",
    "source_name",
    default=None,
    help=("Human-readable name for the session report. Defaults to the output file stem."),
)
def instance_match_derive_cmd(
    input_paths: tuple[str, ...],
    output_path: str,
    endpoint_url: str,
    ports_json_path: str | None,
    batch_size: int,
    timeout: float,
    min_instance_count: int,
    min_confidence: float,
    cache_index: bool,
    index_cache_path: str | None,
    enrich_in_place: bool,
    source_name: str | None,
) -> None:
    """Derive class-level mappings from instance-mapping files."""
    from rdfsolve.api import derive_class_mappings_from_instances

    if not endpoint_url and not ports_json_path:
        raise click.UsageError("Provide either --endpoint or --ports-json.")

    try:
        report = derive_class_mappings_from_instances(
            input_paths=list(input_paths),
            output_path=output_path,
            endpoint_url=endpoint_url,
            ports_json_path=ports_json_path,
            timeout=timeout,
            batch_size=batch_size,
            min_instance_count=min_instance_count,
            min_confidence=min_confidence,
            cache_index=cache_index,
            index_cache_path=index_cache_path,
            enrich_in_place=enrich_in_place,
            source_name=source_name,
        )
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise click.Abort()

    derivation = report.get("derivation", {})
    click.echo(f"  OK {derivation.get('output_class_pairs', '?')} class pairs -> {output_path}")
    click.echo(f"  elapsed: {report.get('elapsed_s', 0):.1f}s  cost: {report.get('cost', {})}")


if __name__ == "__main__":
    main()
