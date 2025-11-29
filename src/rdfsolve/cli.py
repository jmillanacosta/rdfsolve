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
def main() -> None:
    """RDFSolve: A tool for RDF schema analysis and VoID generation."""
    pass


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--graph-uri", multiple=True, help="Graph URI(s) to analyze (can specify multiple)")
@click.option("--output-dir", default=".", help="Output directory")
@click.option("--no-counts", is_flag=True, help="Skip COUNT aggregations for faster discovery")
@click.option("--sample-limit", type=int, help="Limit results for faster sampling")
@click.option(
    "--offset-limit-steps",
    type=int,
    help="Use chunked (paginated) queries with this LIMIT/OFFSET step size",
)
def generate(
    endpoint: str,
    graph_uri: tuple[str, ...],
    output_dir: str,
    no_counts: bool,
    sample_limit: Optional[int],
    offset_limit_steps: Optional[int],
) -> None:
    """Generate VoID description from a SPARQL endpoint.

    This command queries a SPARQL endpoint, discovers schema patterns,
    and generates VoID descriptions with class/property partitions.
    """
    click.echo(f"Generating VoID from endpoint: {endpoint}")
    if graph_uri:
        click.echo(f"Graph URIs: {', '.join(graph_uri)}")

    try:
        # Normalize graph_uris
        graph_uris = list(graph_uri) if graph_uri else None

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Execute and save results
        click.echo("Executing CONSTRUCT query (this may take a while)...")
        void_file = output_path / "void_description.ttl"

        # Generate VoID from endpoint
        void_graph = generate_void_from_endpoint(
            endpoint_url=endpoint,
            graph_uris=graph_uris,
            output_file=str(void_file),
            counts=not no_counts,
            offset_limit_steps=offset_limit_steps,
        )
        click.echo(f"VoID description saved: {void_file}")

        # Parse the generated VoID to extract schema
        schema_df = graph_to_schema(void_graph, graph_uris=graph_uris)
        schema_csv = output_path / "schema.csv"
        schema_df.to_csv(schema_csv, index=False)
        click.echo(f"Schema CSV saved: {schema_csv}")
        click.echo(f"Total schema patterns: {len(schema_df)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--void-file", required=True, help="Path to VoID file")
@click.option("--output-dir", default=".", help="Output directory")
@click.option("--format", type=click.Choice(["csv", "json", "linkml", "all"]), default="all")
def parse(void_file: str, output_dir: str, format: str) -> None:
    """Parse an existing VoID file and extract schema.

    Reads a VoID Turtle file and exports the schema in various formats
    (CSV, JSON-LD, LinkML YAML).
    """
    click.echo(f"Parsing VoID file: {void_file}")

    try:
        parser = load_parser_from_file(void_file)

        # Extract dataset name from file
        dataset_name = Path(void_file).stem.replace("_void", "")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate schema CSV
        if format in ["csv", "all"]:
            schema_df = parser.to_schema(filter_void_admin_nodes=True)
            schema_csv = output_path / f"{dataset_name}_schema.csv"
            schema_df.to_csv(schema_csv, index=False)
            click.echo(f"Schema CSV: {schema_csv}")
            click.echo(f"Total schema triples: {len(schema_df)}")

        # Generate schema JSON-LD
        if format in ["json", "all"]:
            schema_json = to_jsonld_from_file(void_file, filter_void_admin_nodes=True)
            schema_json_file = output_path / f"{dataset_name}_schema.json"
            with open(schema_json_file, "w") as f:
                json.dump(schema_json, f, indent=2)
            click.echo(f"Schema JSON-LD: {schema_json_file}")

        # Generate LinkML schema
        if format in ["linkml", "all"]:
            linkml_yaml = to_linkml_from_file(
                void_file, filter_void_nodes=True, schema_name=dataset_name
            )
            linkml_file = output_path / f"{dataset_name}_schema.yaml"
            with open(linkml_file, "w") as f:
                f.write(linkml_yaml)
            click.echo(f"LinkML schema: {linkml_file}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--graph-uri", multiple=True, help="Graph URI(s) to discover (optional)")
def discover(endpoint: str, graph_uri: tuple[str, ...]) -> None:
    """Discover VoID graphs available in a SPARQL endpoint.

    Queries the endpoint to find graphs containing VoID partition data
    (class partitions, property partitions, datatype partitions).
    """
    click.echo(f"Discovering VoID graphs at: {endpoint}")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        result = discover_void_graphs(endpoint, graph_uris=graph_uris)

        click.echo("\nDiscovery Results:")
        click.echo("=" * 60)
        click.echo(f"Total candidate graphs: {result.get('total_graphs', 0)}")
        click.echo(f"Graphs with VoID content: {len(result.get('void_graphs', []))}")

        void_content = result.get("void_content", {})
        if void_content:
            click.echo("\nGraphs with VoID partitions:")
            for graph_uri, info in void_content.items():
                if info.get("has_any_partitions"):
                    click.echo(f"\n  📊 {graph_uri}")
                    click.echo(f"     Class partitions: {info.get('class_partition_count', 0)}")
                    click.echo(
                        f"     Property partitions: {info.get('property_partition_count', 0)}"
                    )
                    click.echo(
                        f"     Datatype partitions: {info.get('datatype_partition_count', 0)}"
                    )

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--graph-uri", multiple=True, help="Graph URI(s) to query (optional)")
@click.option("--sample-limit", type=int, help="Limit results for sampling")
@click.option("--output", help="Output CSV file (optional)")
def count(
    endpoint: str, graph_uri: tuple[str, ...], sample_limit: Optional[int], output: Optional[str]
) -> None:
    """Count instances per class in a SPARQL endpoint.

    Queries the endpoint to count how many instances exist for each
    rdf:type class in the dataset.
    """
    click.echo(f"Counting instances at: {endpoint}")

    try:
        graph_uris = list(graph_uri) if graph_uri else None
        counts = count_instances_per_class(
            endpoint, graph_uris=graph_uris, sample_limit=sample_limit
        )

        if isinstance(counts, dict):
            click.echo(f"\nFound {len(counts)} classes:")
            click.echo("=" * 60)

            # Sort by count descending
            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)

            for class_uri, count in sorted_counts[:20]:  # Show top 20
                click.echo(f"{count:>8,}  {class_uri}")

            if len(sorted_counts) > 20:
                click.echo(f"\n... and {len(sorted_counts) - 20} more classes")

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
