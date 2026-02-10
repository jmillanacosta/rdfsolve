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


@main.command()
@click.option(
    "--void-file",
    required=True,
    help="Path to VoID file (Turtle format)",
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
    void_file: str,
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

    Takes a VoID description file and exports the schema in multiple
    formats for different use cases: analysis (CSV), semantic web
    (JSON-LD), data modeling (LinkML), validation (SHACL), RDF-config
    schema standard, and coverage analysis.


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

    Example:
        >>> rdfsolve export --void-file void_description.ttl \\
            --format rdfconfig \\
            --endpoint-url https://example.org/sparql \\
            --graph-uri http://example.org/graph \\
            --output-dir ./exports

        # Creates: ./exports/{dataset}_config/model.yaml
        #          ./exports/{dataset}_config/prefix.yaml
        #          ./exports/{dataset}_config/endpoint.yaml
    """
    click.echo(f"Exporting schema from: {void_file}")

    try:
        parser = load_parser_from_file(void_file)
        dataset_name = Path(void_file).stem.replace("_void", "")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # CSV export
        if format in ["csv", "all"]:
            _export_csv(parser, output_path, dataset_name)

        # JSON-LD export
        if format in ["jsonld", "all"]:
            _export_jsonld(
                void_file,
                output_path,
                dataset_name,
                endpoint_url=endpoint_url,
                graph_uri=graph_uri,
            )

        # LinkML export
        if format in ["linkml", "all"]:
            _export_linkml(
                void_file,
                output_path,
                dataset_name,
                schema_name,
                schema_description,
                schema_uri,
            )

        # SHACL export
        if format in ["shacl", "all"]:
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

        # RDF-config export
        if format in ["rdfconfig", "all"]:
            _export_rdfconfig(
                void_file,
                output_path,
                dataset_name,
                endpoint_url,
                endpoint_name,
                graph_uri,
            )

        # Pattern coverage export
        if format in ["coverage", "all"]:
            click.echo("  Coverage: Skipped (requires instance data, not available from VoID file)")

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
def mine(
    endpoint: str,
    graph_uri: tuple[str, ...],
    output_dir: str,
    dataset_name: Optional[str],
    fmt: str,
    chunk_size: int,
    no_counts: bool,
) -> None:
    r"""Mine RDF schema directly from a SPARQL endpoint.

    Uses lightweight SELECT queries to discover schema patterns:
    subject_class -> property -> object_class / Literal / Resource.

    This is an alternative to 'extract' that avoids heavy CONSTRUCT
    queries and VoID-on-the-endpoint overhead.  The primary export
    is JSON-LD; a VoID Turtle file can also be generated for
    downstream conversion (LinkML, SHACL, RDF-config) via 'export'.


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
            counts=not no_counts,
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
            void_file = output_path / f"{name}_void.ttl"
            void_graph = schema.to_void_graph()
            void_graph.serialize(
                destination=str(void_file), format="turtle",
            )
            click.echo(
                f"OK VoID:     {void_file} "
                f"({len(void_graph)} triples)"
            )

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
def mine_all(
    sources: str,
    output_dir: str,
    fmt: str,
    chunk_size: int,
    no_counts: bool,
    timeout: float,
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
            timeout=timeout,
            counts=not no_counts,
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


if __name__ == "__main__":
    main()
