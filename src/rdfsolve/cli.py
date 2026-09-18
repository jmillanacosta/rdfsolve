"""Command line entry points."""

from __future__ import annotations

from pathlib import Path

import click


@click.group()
def main() -> None:
    """Inspect rdfsolve registries."""


@main.group()
def registry() -> None:
    """Inspect the source registry."""


@registry.command("identity")
@click.option(
    "--sources",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Registry YAML. Defaults to data/sources.yaml.",
)
@click.option(
    "--overrides",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Curated relations. Defaults to identity_overrides.yaml next to the registry.",
)
@click.option(
    "--output",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Directory for identity.json and identity_review.tsv.",
)
def identity(sources: Path | None, overrides: Path | None, output: Path) -> None:
    """Resolve which registry entries describe the same dataset."""
    from rdfsolve.dataset_identity import read_overrides, read_registry, resolve_identity
    from rdfsolve.sources import DEFAULT_SOURCES_YAML

    registry_path = sources or DEFAULT_SOURCES_YAML
    overrides_path = overrides or registry_path.with_name("identity_overrides.yaml")
    resolution = resolve_identity(read_registry(registry_path), read_overrides(overrides_path))
    resolution.write(output)
    click.echo(
        f"{len(resolution.entries)} registry entries, {len(resolution.datasets)} datasets, "
        f"{len(resolution.candidates)} candidate relations to review"
    )
