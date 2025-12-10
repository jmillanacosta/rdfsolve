"""Command line interface for :mod:`rdfsolve`."""

import json
from pathlib import Path
from typing import Optional

import click

from .api import (
    count_instances_per_class,
    discover_void_graphs,
    generate_void_from_endpoint,
    graph_to_schema,
    load_parser_from_file,
    to_jsonld_from_file,
    to_linkml_from_file,
)

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
      rdfsolve discover --endpoint https://sparql.uniprot.org/sparql
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
      rdfsolve extract --endpoint https://sparql.uniprot.org/sparql \
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


@main.command()
@click.option(
    "--void-file",
    required=True,
    help="Path to VoID file (Turtle format)",
)
@click.option("--output-dir", default=".", help="Output directory for exports")
@click.option(
    "--format",
    type=click.Choice(["csv", "jsonld", "linkml", "coverage", "all"]),
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
def export(
    void_file: str,
    output_dir: str,
    format: str,
    schema_name: Optional[str],
    schema_description: Optional[str],
    schema_uri: Optional[str],
) -> None:
    r"""Export RDF schema to various formats.

    Takes a VoID description file and exports the schema in multiple
    formats for different use cases: analysis (CSV), semantic web
    (JSON-LD), data modeling (LinkML), and coverage analysis.

    Example:
      rdfsolve export --void-file void_description.ttl --format jsonld --output-dir ./exports
    """
    click.echo(f"Exporting schema from: {void_file}")

    try:
        parser = load_parser_from_file(void_file)
        dataset_name = Path(void_file).stem.replace("_void", "")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # CSV export
        if format in ["csv", "all"]:
            schema_df = parser.to_schema(filter_void_admin_nodes=True)
            schema_csv = output_path / f"{dataset_name}_schema.csv"
            schema_df.to_csv(schema_csv, index=False)
            click.echo(f"OK CSV:      {schema_csv} ({len(schema_df)} triples)")

        # JSON-LD export
        if format in ["jsonld", "all"]:
            schema_jsonld = to_jsonld_from_file(void_file, filter_void_admin_nodes=True)
            jsonld_file = output_path / f"{dataset_name}_schema.jsonld"
            with open(jsonld_file, "w") as f:
                json.dump(schema_jsonld, f, indent=2)
            click.echo(f"OK JSON-LD:  {jsonld_file}")

        # LinkML export
        if format in ["linkml", "all"]:
            # Use provided schema_name or derive from filename
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

        # Pattern coverage export - requires instance data (not available from VoID alone)
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
      rdfsolve count --endpoint https://sparql.uniprot.org/sparql \
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
