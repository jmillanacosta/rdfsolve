#!/usr/bin/env python3
"""Discover all graphs at endpoints and save results.

This script can be run before mining to discover and catalog all named graphs
at SPARQL endpoints. The results are saved as JSON and can be integrated into
the mining workflow.

Usage:
    python scripts/discover_graphs_standalone.py --endpoint https://example.org/sparql --name mydata
    python scripts/discover_graphs_standalone.py --sources data/sources.yaml --output output/
"""

import json
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

import click
from rdfsolve import discover_all_graphs, load_sources


@click.command()
@click.option("--endpoint", help="Single SPARQL endpoint URL")
@click.option("--name", help="Dataset name (required with --endpoint)")
@click.option("--sources", help="Path to sources.yaml file (batch mode)")
@click.option("--output", default="output", help="Output directory")
@click.option("--limit", type=int, help="Limit to first N sources (for testing)")
def main(endpoint, name, sources, output, limit):
    """Discover named graphs at SPARQL endpoints."""

    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    if endpoint:
        # Single endpoint mode
        if not name:
            click.echo("Error: --name required with --endpoint", err=True)
            sys.exit(1)

        discover_single(endpoint, name, output_path)

    elif sources:
        # Batch mode from sources.yaml
        discover_batch(sources, output_path, limit)

    else:
        click.echo("Error: Either --endpoint or --sources required", err=True)
        click.echo("See --help for usage", err=True)
        sys.exit(1)


def discover_single(endpoint: str, name: str, output_path: Path):
    """Discover graphs for a single endpoint."""
    click.echo(f"Discovering graphs: {name}")
    click.echo(f"  Endpoint: {endpoint}")

    try:
        result = discover_all_graphs(endpoint)

        out_dir = output_path / name
        out_dir.mkdir(parents=True, exist_ok=True)

        # Save full results
        graphs_file = out_dir / f"{name}_graphs.json"
        with open(graphs_file, "w") as f:
            json.dump(result, f, indent=2)

        # Print summary
        click.echo(f"  ✓ Found {result['total_graphs']} graphs")
        click.echo(f"    - Ontology graphs (.owl): {len(result['ontology_graphs'])}")
        click.echo(f"    - VoID metadata graphs: {len(result['void_graphs'])}")
        click.echo(f"  Saved to: {graphs_file}")

        if result.get("error"):
            click.echo(f"  ⚠ Warning: {result['error']}", err=True)

    except Exception as e:
        click.echo(f"  ✗ Error: {e}", err=True)
        import traceback

        traceback.print_exc()


def discover_batch(sources_file: str, output_path: Path, limit: int | None):
    """Discover graphs for all sources in sources.yaml."""
    click.echo(f"Loading sources from: {sources_file}")

    try:
        sources_list = load_sources(sources_file)
    except Exception as e:
        click.echo(f"Error loading sources: {e}", err=True)
        sys.exit(1)

    # Filter to remote endpoints only
    remote_sources = [s for s in sources_list if s.get("endpoint")]

    if limit:
        remote_sources = remote_sources[:limit]
        click.echo(f"Limiting to first {limit} sources")

    click.echo(f"Processing {len(remote_sources)} remote endpoints...")
    click.echo()

    success_count = 0
    error_count = 0

    for i, source in enumerate(remote_sources, 1):
        name = source["name"]
        endpoint = source["endpoint"]

        click.echo(f"[{i}/{len(remote_sources)}] {name}")
        try:
            result = discover_all_graphs(endpoint)

            out_dir = output_path / name
            out_dir.mkdir(parents=True, exist_ok=True)

            graphs_file = out_dir / f"{name}_graphs.json"
            with open(graphs_file, "w") as f:
                json.dump(result, f, indent=2)

            click.echo(
                f"  ✓ {result['total_graphs']} graphs "
                f"({len(result['ontology_graphs'])} .owl, "
                f"{len(result['void_graphs'])} void)"
            )

            success_count += 1

        except Exception as e:
            click.echo(f"  ✗ Error: {e}", err=True)
            error_count += 1

    click.echo()
    click.echo("=" * 70)
    click.echo(f"Summary: {success_count} succeeded, {error_count} failed")


if __name__ == "__main__":
    main()
