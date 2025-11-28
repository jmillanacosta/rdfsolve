"""Command line interface for :mod:`rdfsolve`."""

from pathlib import Path

import click

__all__ = [
    "main",
]


@click.group()
def main():
    """RDFSolve: A tool for RDF schema analysis and VoID generation."""
    pass


@main.command()
@click.option("--endpoint", required=True, help="SPARQL endpoint URL")
@click.option("--graph-uri", required=True, help="Graph URI to analyze")
@click.option("--dataset-name", required=True, help="Dataset name")
@click.option("--void-iri", required=True, help="VoID IRI")
@click.option("--output-dir", default=".", help="Output directory")
@click.option("--no-counts", is_flag=True, help="Skip COUNT aggregations for faster discovery")
@click.option("--sample-limit", type=int, help="Limit results for faster sampling")
def analyze(endpoint, graph_uri, dataset_name, void_iri, output_dir, no_counts, sample_limit):
    """Analyze an RDF dataset and generate VoID description and schema."""
    from .rdfsolve import RDFSolver

    click.echo(f"Analyzing dataset: {dataset_name}")
    click.echo(f"Endpoint: {endpoint}")
    click.echo(f"Graph URI: {graph_uri}")

    try:
        # Initialize RDFSolver
        solver = RDFSolver(
            endpoint=endpoint, path=output_dir, void_iri=void_iri, dataset_name=dataset_name
        )

        # Generate VoID description
        output_file = Path(output_dir) / f"{dataset_name}_void.ttl"
        solver.void_generator(
            graph_uri=graph_uri,
            output_file=str(output_file),
            counts=not no_counts,
            sample_limit=sample_limit,
        )

        click.echo(f"VoID description generated: {output_file}")

        # Extract schema
        parser = solver.extract_schema()

        # Generate schema DataFrame
        schema_df = parser.to_schema(filter_void_admin_nodes=True)
        schema_csv = Path(output_dir) / f"{dataset_name}_schema.csv"
        schema_df.to_csv(schema_csv, index=False)

        # Generate schema JSON
        schema_json = parser.to_json(filter_void_nodes=True)
        schema_json_file = Path(output_dir) / f"{dataset_name}_schema.json"

        import json

        with open(schema_json_file, "w") as f:
            json.dump(schema_json, f, indent=2)

        click.echo(f"Schema CSV: {schema_csv}")
        click.echo(f"Schema JSON: {schema_json_file}")
        click.echo(f"Total schema triples: {len(schema_df)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option("--void-file", required=True, help="Path to VoID file")
@click.option("--output-dir", default=".", help="Output directory")
def parse(void_file, output_dir):
    """Parse an existing VoID file and extract schema."""
    from .void_parser import VoidParser

    click.echo(f"Parsing VoID file: {void_file}")

    try:
        parser = VoidParser(void_file)

        # Generate schema DataFrame
        schema_df = parser.to_schema(filter_void_admin_nodes=True)

        # Extract dataset name from file
        dataset_name = Path(void_file).stem.replace("_void", "")

        schema_csv = Path(output_dir) / f"{dataset_name}_schema.csv"
        schema_df.to_csv(schema_csv, index=False)

        # Generate schema JSON
        schema_json = parser.to_json(filter_void_nodes=True)
        schema_json_file = Path(output_dir) / f"{dataset_name}_schema.json"

        import json

        with open(schema_json_file, "w") as f:
            json.dump(schema_json, f, indent=2)

        click.echo(f"Schema CSV: {schema_csv}")
        click.echo(f"Schema JSON: {schema_json_file}")
        click.echo(f"Total schema triples: {len(schema_df)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@main.command()
def config_info():
    """Show information about the RDF config module."""
    click.echo("RDF Config Module Information:")
    click.echo("=" * 40)
    click.echo("The rdfsolve.config module provides advanced RDF config")
    click.echo("parsing capabilities for YAML-based model definitions.")
    click.echo("")
    click.echo("Usage in Python:")
    click.echo("  from rdfsolve.config import RDFConfigParser")
    click.echo("  parser = RDFConfigParser('/path/to/config/dir')")
    click.echo("  schema = parser.parse_to_schema()")
    click.echo("")
    click.echo("This module is separate from the main VoID-based workflow")
    click.echo("and is useful for Ruby RDFConfig-style model processing.")


@main.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=5000, help="Port to bind to")
@click.option("--debug/--no-debug", default=True, help="Enable debug mode")
def web(host, port, debug):
    """Start the RDFSolve web interface."""
    try:
        from .web import create_app

        click.echo("Starting RDFSolve web interface...")
        click.echo(f"Server will be available at: http://localhost:{port}")
        click.echo(f"Debug mode: {debug}")

        app = create_app()
        app.run(debug=debug, host=host, port=port)

    except ImportError:
        click.echo("Flask is not installed. Please install with: pip install flask", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"Error starting web interface: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    main()
