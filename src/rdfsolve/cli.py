"""Command line interface for :mod:`rdfsolve`."""

import json
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


@click.group()
@click.version_option()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    r"""RDFSolve - RDF Schema Extraction and Analysis Toolkit.

    Extract, analyze, and export RDF schemas from SPARQL endpoints.


    Typical workflow: discover > extract > export

    Additionally, you can use rdfsolve count to count instances per class
    """
    import logging

    # Ensure context object exists
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            force=True,
        )
        # Also set the rdfsolve logger specifically
        logging.getLogger("rdfsolve").setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s", force=True)


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option(
    "--graph-uri",
    multiple=True,
    help="Specific graph URI(s) to search (optional)",
)
@click.option("--output-dir", default=".", help="Output directory")
def discover(endpoint: str, graph_uri: tuple[str, ...], output_dir: str) -> None:
    """Discover existing VoID descriptions in a SPARQL endpoint.

    Searches the endpoint for graphs containing VoID (Vocabulary of
    Interlinked Datasets) partitions for class, property, and datatype.

    Use this command first to check if the endpoint already has VoID
    descriptions before extracting a new schema. Saves discovered
    partitions to a Turtle file.

    Discovery always searches well-known and VoID graphs, as these
    commonly contain partition descriptions.


    Example:
      >>> rdfsolve discover --endpoint https://sparql.uniprot.org/sparql
    """
    from pathlib import Path

    click.echo(f"Discovering VoID graphs at: {endpoint}")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        # Discovery always includes all graphs (exclude_graphs=False)
        result = discover_void_graphs(
            endpoint,
            graph_uris=graph_uris,
            exclude_graphs=False,
        )

        click.echo("\nDiscovery Results:")
        click.echo("=" * 60)
        click.echo(f"Total candidate graphs: {result.get('total_graphs', 0)}")
        void_graphs = len(result.get("void_graphs", []))
        click.echo(f"Graphs with VoID content: {void_graphs}")

        void_content = result.get("void_content", {})
        if void_content:
            click.echo("\nGraphs with VoID partitions:")
            for graph_uri_item, info in void_content.items():
                if info.get("has_any_partitions"):
                    click.echo(f"\n  {graph_uri_item}")
                    part_count = info.get("partition_count", 0)
                    click.echo(f"     Partition count: {part_count}")

        # Build and save VoID graph from discovered partitions
        partitions = result.get("partitions", [])
        if partitions:
            from rdfsolve.parser import VoidParser

            found_graphs = result.get("found_graphs", [])
            base_uri = found_graphs[0] if found_graphs else None
            parser = VoidParser(graph_uris=graph_uris)
            void_graph = parser.build_void_graph_from_partitions(partitions, base_uri=base_uri)

            # Save to file
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            void_file = output_path / "discovered_void.ttl"
            void_graph.serialize(destination=str(void_file), format="turtle")
            click.echo(f"\nOK Discovered VoID saved: {void_file}")
            click.echo(f"   Total triples: {len(void_graph)}")
        else:
            click.echo("\nNo VoID partitions found to save")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option(
    "--graph-uri",
    multiple=True,
    help="Specific graph URI(s) to analyze (optional)",
)
@click.option("--output-dir", default=".", help="Output directory")
@click.option(
    "--no-counts",
    is_flag=True,
    help="Skip instance counting (faster)",
)
@click.option("--sample-limit", type=int, help="Limit results when sampling")
@click.option(
    "--offset-limit-steps",
    type=int,
    help="Use pagination with this step size",
)
@click.option(
    "--exclude-service-graphs/--include-service-graphs",
    default=True,
    help="Exclude service/system graphs (default: excluded)",
)
@click.option(
    "--force-generate",
    is_flag=True,
    help="Force fresh generation even if VoID exists (no prompt)",
)
@click.option(
    "--dataset-name",
    help="Custom name for the dataset (used in output filenames)",
)
@click.option(
    "--void-base-uri",
    help="Custom base URI for VoID partition IRIs",
)
def extract(
    endpoint: str,
    graph_uri: tuple[str, ...],
    output_dir: str,
    no_counts: bool,
    sample_limit: Optional[int],
    offset_limit_steps: Optional[int],
    exclude_service_graphs: bool,
    force_generate: bool,
    dataset_name: Optional[str],
    void_base_uri: Optional[str],
) -> None:
    r"""Extract RDF schema from a SPARQL endpoint.

    Queries the endpoint to discover schema patterns and generates
    a VoID description with class and property partitions. This
    creates a complete schema representation of the RDF data.

    By default, service and system graphs (Virtuoso, well-known URIs,
    OWL ontology) are excluded from extraction. Use
    --include-service-graphs to include them.


    Outputs:
      - void_description.ttl - VoID metadata in Turtle format
      - schema.csv           - Schema patterns in CSV


    Example:
      >>> rdfsolve extract --endpoint https://sparql.uniprot.org/sparql \
        --output-dir ./uniprot_schema
    """
    click.echo(f"Extracting schema from endpoint: {endpoint}")
    if graph_uri:
        click.echo(f"Graph URIs: {', '.join(graph_uri)}")
    if not exclude_service_graphs:
        click.echo("  Including service/system graphs in extraction")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # First, try to discover existing VoID descriptions
        click.echo("Checking for existing VoID descriptions...")
        discovery = discover_void_graphs(endpoint, graph_uris=graph_uris, exclude_graphs=False)

        void_graph = None
        partitions = discovery.get("partitions", [])

        if partitions and not force_generate:
            # Found existing VoID - use it or prompt
            found_graphs = discovery.get("found_graphs", [])
            click.echo(
                f"\nOK: Found existing VoID: {len(partitions)} partitions "
                f"in {len(found_graphs)} graph(s)"
            )
            for graph_uri_item in found_graphs:
                click.echo(f"  • {graph_uri_item}")

            click.echo("Using discovered VoID (use --force-generate to extract fresh schema)")
            click.echo("Building VoID from discovered partitions...")

            from rdfsolve.parser import VoidParser

            base_uri = found_graphs[0] if found_graphs else None
            parser = VoidParser(graph_uris=graph_uris)
            void_graph = parser.build_void_graph_from_partitions(partitions, base_uri=base_uri)
        else:
            # No existing VoID or force generate requested
            if partitions and force_generate:
                click.echo(
                    f"\nOK: Found existing VoID: {len(partitions)} partitions "
                    f"but --force-generate specified"
                )
                click.echo("Generating fresh schema from endpoint (may take a while)...")
            else:
                click.echo("No existing VoID found.")
                if not force_generate:
                    # Prompt user for confirmation
                    click.echo(
                        "\nThis will extract schema from the endpoint, "
                        "which may take several minutes."
                    )
                    if not click.confirm("Continue with schema extraction?", default=True):
                        click.echo("Extraction cancelled.")
                        return
                click.echo("Generating schema from endpoint (may take a while)...")

            void_graph = generate_void_from_endpoint(
                endpoint_url=endpoint,
                graph_uris=graph_uris,
                output_file=None,
                counts=not no_counts,
                offset_limit_steps=offset_limit_steps,
                exclude_graphs=exclude_service_graphs,
                void_base_uri=void_base_uri,
            )

        # Save VoID description with custom or default name
        if dataset_name:
            void_filename = f"{dataset_name}_void.ttl"
            schema_filename = f"{dataset_name}_schema.csv"
        else:
            void_filename = "void_description.ttl"
            schema_filename = "schema.csv"

        void_file = output_path / void_filename
        void_graph.serialize(destination=str(void_file), format="turtle")
        click.echo(f"OK VoID description saved: {void_file}")

        schema_df = graph_to_schema(void_graph, graph_uris=graph_uris)
        schema_csv = output_path / schema_filename
        schema_df.to_csv(schema_csv, index=False)
        click.echo(f"OK Schema CSV saved: {schema_csv}")
        click.echo(f"  Total schema patterns: {len(schema_df)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


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


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option(
    "--graph-uri",
    multiple=True,
    help="Specific graph URI(s) to query (optional)",
)
@click.option("--output-dir", default=".", help="Output directory")
@click.option(
    "--dataset-name",
    help="Custom dataset name (used in output filenames)",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["jsonld", "void", "linkml", "shacl", "all"]),
    default="all",
    help="Export format (default: all)",
)
@click.option(
    "--chunk-size",
    type=int,
    default=10_000,
    help="Pagination page size for pattern queries (default: 10000)",
)
@click.option(
    "--class-chunk-size",
    type=int,
    default=None,
    help=(
        "Page size for Phase-1 class discovery in --two-phase mode. "
        "Default (None) = no pagination (single query). "
        "Set to e.g. 50000 for endpoints with very many classes."
    ),
)
@click.option(
    "--class-batch-size",
    type=int,
    default=15,
    help=(
        "Number of classes per VALUES query in Phase-2 of "
        "--two-phase mode (default: 15). Higher = fewer queries "
        "but each query is heavier."
    ),
)
@click.option(
    "--no-counts",
    is_flag=True,
    help="Skip triple-count queries (faster)",
)
@click.option(
    "--timeout",
    type=float,
    default=120.0,
    help="HTTP timeout per request in seconds (default: 120)",
)
@click.option(
    "--schema-name",
    default=None,
    help="Custom name for LinkML/SHACL schema (default: dataset name)",
)
@click.option(
    "--schema-description",
    default=None,
    help="Description for LinkML/SHACL schema",
)
@click.option(
    "--schema-uri",
    default=None,
    help="Base URI for LinkML/SHACL schema",
)
@click.option(
    "--shacl-closed/--shacl-open",
    default=True,
    help="Generate closed SHACL shapes (default: closed)",
)
@click.option(
    "--shacl-suffix",
    default=None,
    help="Suffix for SHACL shape names (e.g. 'Shape' → PersonShape)",
)
@click.option(
    "--two-phase",
    is_flag=True,
    help="Use two-phase mining (discover classes first, then per-class queries). Gentler on large endpoints.",
)
@click.option(
    "--report-path",
    type=click.Path(),
    default=None,
    help="Write analytics JSON report to this path (updated incrementally).",
)
def mine(
    endpoint: str,
    graph_uri: tuple[str, ...],
    output_dir: str,
    dataset_name: Optional[str],
    fmt: str,
    chunk_size: int,
    class_chunk_size: Optional[int],
    class_batch_size: int,
    no_counts: bool,
    timeout: float,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
    shacl_closed: bool,
    shacl_suffix: Optional[str],
    two_phase: bool,
    report_path: Optional[str],
) -> None:
    r"""Mine RDF schema directly from a SPARQL endpoint.

    Uses lightweight SELECT queries to discover schema patterns:
    subject_class -> property -> object_class / Literal / Resource.

    This is an alternative to 'extract' that avoids heavy CONSTRUCT
    queries and VoID-on-the-endpoint overhead.  The primary export
    is JSON-LD; a VoID Turtle file can also be generated for
    downstream conversion (LinkML, SHACL, RDF-config) via 'export'.

    Formats:
      - jsonld:  Mined-schema JSON-LD file (always generated with 'all')
      - void:    VoID Turtle for use with 'rdfsolve export'
      - linkml:  LinkML YAML schema
      - shacl:   SHACL shapes Turtle file
      - all:     All of the above (default)

    Example:
      >>> rdfsolve mine \
            --endpoint https://sparql.wikipathways.org/sparql/ \
            --dataset-name wikipathways \
            --output-dir ./wp_schema
    """
    from .miner import SchemaMiner

    click.echo(f"Mining schema from: {endpoint}")
    if graph_uri:
        click.echo(f"  Graph URIs: {', '.join(graph_uri)}")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        name = dataset_name or "schema"

        miner = SchemaMiner(
            endpoint_url=endpoint,
            graph_uris=graph_uris,
            chunk_size=chunk_size,
            class_chunk_size=class_chunk_size,
            class_batch_size=class_batch_size,
            timeout=timeout,
            counts=not no_counts,
            two_phase=two_phase,
            report_path=report_path,
        )
        schema = miner.mine(dataset_name=name)

        click.echo(
            f"OK {len(schema.patterns)} patterns "
            f"({len(schema.get_classes())} classes, "
            f"{len(schema.get_properties())} properties)"
        )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # JSON-LD export
        if fmt in ("jsonld", "all"):
            jsonld_file = output_path / f"{name}_schema.jsonld"
            with open(jsonld_file, "w") as f:
                json.dump(schema.to_jsonld(), f, indent=2)
            click.echo(f"OK JSON-LD:  {jsonld_file}")

        # VoID Turtle export
        if fmt in ("void", "all"):
            void_graph = schema.to_void_graph()
            void_file = output_path / f"{name}_void.ttl"
            void_graph.serialize(
                destination=str(void_file), format="turtle",
            )
            click.echo(
                f"OK VoID:     {void_file} "
                f"({len(void_graph)} triples)"
            )

        # LinkML / SHACL — build a VoidParser from the mined graph once
        if fmt in ("linkml", "shacl", "all"):
            from rdfsolve.parser import VoidParser as _VP
            _lp = _VP(void_source=schema.to_void_graph())

            if fmt in ("linkml", "all"):
                _export_linkml_from_parser(
                    _lp,
                    output_path,
                    name,
                    schema_name,
                    schema_description,
                    schema_uri,
                )

            if fmt in ("shacl", "all"):
                _export_shacl_from_parser(
                    _lp,
                    output_path,
                    name,
                    schema_name,
                    schema_description,
                    schema_uri,
                    shacl_closed,
                    shacl_suffix,
                )

        # Report summary
        if miner.last_report:
            rpt = miner.last_report
            click.echo(
                f"OK Report:   {rpt.total_queries_sent} queries "
                f"({rpt.total_queries_failed} failed), "
                f"{rpt.total_duration_s:.1f}s total"
            )
            if report_path:
                click.echo(f"   Written:  {report_path}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command("mine-all")
@click.option(
    "--sources",
    required=True,
    type=click.Path(exists=True),
    help="Path to sources CSV file (columns: dataset_name, endpoint_url, graph_uri, use_graph)",
)
@click.option("--output-dir", default=".", help="Output directory")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["jsonld", "void", "all"]),
    default="all",
    help="Export format (default: all)",
)
@click.option(
    "--chunk-size",
    type=int,
    default=10_000,
    help="Pagination page size (default: 10000)",
)
@click.option(
    "--no-counts",
    is_flag=True,
    help="Skip triple-count queries (faster)",
)
@click.option(
    "--timeout",
    type=float,
    default=120.0,
    help="HTTP timeout per request in seconds (default: 120)",
)
@click.option(
    "--class-chunk-size",
    type=int,
    default=None,
    help=(
        "Page size for Phase-1 class discovery in two-phase "
        "rows. Default: None (single query, no pagination). "
        "Ignored for rows that are not two-phase."
    ),
)
@click.option(
    "--class-batch-size",
    type=int,
    default=15,
    help=(
        "Number of classes per VALUES query in Phase-2 of "
        "two-phase mode (default: 15). Higher = fewer queries "
        "but each query is heavier."
    ),
)
@click.option(
    "--no-reports",
    is_flag=True,
    help="Skip writing per-source analytics JSON reports.",
)
def mine_all(
    sources: str,
    output_dir: str,
    fmt: str,
    chunk_size: int,
    no_counts: bool,
    timeout: float,
    class_chunk_size: int | None,
    class_batch_size: int,
    no_reports: bool,
) -> None:
    r"""Mine schemas for all sources in a CSV file.

    Reads the sources CSV and runs the miner for each endpoint.
    Results are written to the output directory as
    ``{dataset_name}_schema.jsonld`` and/or ``{dataset_name}_void.ttl``.

    The CSV must have columns: dataset_name, endpoint_url, graph_uri,
    use_graph. Rows without an endpoint_url are skipped.


    Example:
      >>> rdfsolve mine-all \
            --sources data/sources.csv \
            --output-dir ./mined_schemas
    """
    from .api import mine_all_sources

    click.echo(f"Mining all sources from: {sources}")
    click.echo(f"Output directory: {output_dir}")

    def _on_progress(
        name: str, idx: int, total: int,
        error: str | None,
    ) -> None:
        if error == "skipped":
            click.echo(
                f"  [{idx}/{total}] SKIP {name} "
                f"(no endpoint)"
            )
        elif error:
            click.echo(
                f"  [{idx}/{total}] FAIL {name}: {error}",
                err=True,
            )
        else:
            click.echo(
                f"  [{idx}/{total}] OK   {name}"
            )

    try:
        result = mine_all_sources(
            sources_csv=sources,
            output_dir=output_dir,
            fmt=fmt,
            chunk_size=chunk_size,
            class_chunk_size=class_chunk_size,
            class_batch_size=class_batch_size,
            timeout=timeout,
            counts=not no_counts,
            reports=not no_reports,
            on_progress=_on_progress,
        )

        click.echo("\n" + "=" * 50)
        click.echo(f"Succeeded: {len(result['succeeded'])}")
        click.echo(f"Failed:    {len(result['failed'])}")
        click.echo(f"Skipped:   {len(result['skipped'])}")

        if result["failed"]:
            click.echo("\nFailed datasets:")
            for entry in result["failed"]:
                click.echo(
                    f"  • {entry['dataset']}: "
                    f"{entry['error'][:80]}"
                )

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


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
    "--sources-csv", default="data/sources.csv", show_default=True,
    help="Path to data sources CSV.",
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
    sources_csv: str,
    predicate: str,
    datasets: tuple[str, ...],
    timeout: float,
    output: Optional[str],
) -> None:
    """Probe endpoints for a single bioregistry resource.

    Queries every endpoint in SOURCES_CSV for RDF classes whose instances
    match the URI patterns registered in bioregistry for PREFIX and emits
    a JSON-LD mapping document.
    """
    import json

    from rdfsolve.api import probe_instance_mapping

    try:
        result = probe_instance_mapping(
            prefix=prefix,
            sources_csv=sources_csv,
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
    "--sources-csv", default="data/sources.csv", show_default=True,
    help="Path to data sources CSV.",
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
    sources_csv: str,
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
            sources_csv=sources_csv,
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
