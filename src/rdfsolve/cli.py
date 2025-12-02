"""Command line interface for :mod:`rdfsolve`."""

import json
from pathlib import Path
from typing import Optional

import click

from .api import (
    count_instances_per_class,
    discover_void_graphs,
    extract_partitions_from_void,
    generate_void_alternative_method,
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


@main.command(name="generate-alt")
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--dataset-prefix", required=True, help="Dataset prefix for IRIs")
@click.option("--graph-uri", help="Graph URI to analyze (optional)")
@click.option("--output-dir", default=".", help="Output directory")
def generate_alternative(
    endpoint: str,
    dataset_prefix: str,
    graph_uri: Optional[str],
    output_dir: str,
) -> None:
    """Generate VoID using alternative single-query method.

    Uses a unified non-paginated CONSTRUCT query that extracts all VoID
    partition data in one request. Read-only approach from void-generator.

    Source: https://github.com/sib-swiss/void-generator/issues/30
    """
    click.echo(f"Generating VoID (alternative method) from: {endpoint}")
    click.echo(f"Dataset prefix: {dataset_prefix}")

    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        void_file = output_path / "void_alternative.ttl"

        import time

        t0 = time.time()

        void_graph = generate_void_alternative_method(
            endpoint_url=endpoint,
            dataset_prefix=dataset_prefix,
            graph_uri=graph_uri,
            output_file=str(void_file),
        )

        elapsed = time.time() - t0

        click.echo(f"\nVoID description saved: {void_file}")
        click.echo(f"Generation time: {elapsed:.2f}s")
        click.echo(f"Total triples: {len(void_graph)}")

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
            for graph_uri_item, info in void_content.items():
                if info.get("has_any_partitions"):
                    click.echo(f"\n  {graph_uri_item}")
                    click.echo(f"     Partition count: {info.get('partition_count', 0)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--void-graph", multiple=True, required=True, help="VoID graph URI(s)")
@click.option("--output", help="Output JSON file (optional)")
def extract(endpoint: str, void_graph: tuple[str, ...], output: Optional[str]) -> None:
    """Extract partition data from VoID graphs.

    Retrieves class-property-object partition information from
    discovered VoID graphs using a lightweight query.
    """
    click.echo(f"Extracting partitions from {len(void_graph)} VoID graph(s)...")

    try:
        void_graph_uris = list(void_graph)
        partitions = extract_partitions_from_void(endpoint, void_graph_uris)

        click.echo(f"\nExtracted {len(partitions)} partition records")
        click.echo("=" * 60)

        # Show sample
        for partition in partitions[:5]:
            subj = partition.get("subject_class", "").split("/")[-1].split("#")[-1]
            prop = partition.get("property", "").split("/")[-1].split("#")[-1]
            obj_class = partition.get("object_class", "")
            obj_dtype = partition.get("object_datatype", "")
            obj = (obj_class or obj_dtype).split("/")[-1].split("#")[-1]
            click.echo(f"  {subj} -> {prop} -> {obj}")

        if len(partitions) > 5:
            click.echo(f"  ... and {len(partitions) - 5} more")

        if output:
            import json

            with open(output, "w") as f:
                json.dump(partitions, f, indent=2)
            click.echo(f"\nFull results saved to: {output}")

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
