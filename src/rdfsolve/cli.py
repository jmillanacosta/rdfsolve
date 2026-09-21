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
    if resolution.review_complete:
        click.echo(
            f"{len(resolution.entries)} registry entries, "
            f"{resolution.canonical_dataset_count} canonical datasets, identity review complete"
        )
    else:
        click.echo(
            f"{len(resolution.entries)} registry entries, {len(resolution.datasets)} provisional groups, "
            f"{len(resolution.candidates)} candidate relations to review; "
            "canonical dataset count unresolved"
        )


@main.group()
def release() -> None:
    """Build and inspect frozen evidence-corpus releases."""


@release.command("build")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--release-id", default=None, help="Stable release identifier; generated when omitted."
)
def release_build(run_dir: Path, release_id: str | None) -> None:
    """Build release.json and release.ttl from one completed run directory."""
    import json

    from rdfsolve.release import (
        build_release_manifest,
        release_to_rdf,
        summarize_release,
        write_release_manifest,
    )
    from rdfsolve.version import VERSION

    manifest = build_release_manifest(run_dir, release_id=release_id, rdfsolve_version=VERSION)
    json_path = write_release_manifest(manifest, run_dir)
    ttl_path = run_dir / "release.ttl"
    ttl_path.write_text(release_to_rdf(manifest).serialize(format="turtle"), encoding="utf-8")
    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summarize_release(manifest), indent=2), encoding="utf-8")
    click.echo(f"Wrote {json_path}")
    click.echo(f"Wrote {ttl_path}")
    click.echo(f"Wrote {summary_path}")


@release.command("validate")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
def release_validate(run_dir: Path) -> None:
    """Verify file inventory, hashes, references, and supported RDF serializations."""
    from rdfsolve.release.model import ReleaseManifest
    from rdfsolve.release.validate import validate_release

    manifest_path = run_dir / "release.json"
    if not manifest_path.exists():
        raise click.ClickException("release.json is missing; run `rdfsolve release build` first")
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    result = validate_release(manifest, run_dir)
    report = run_dir / "validation_release.json"
    report.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    click.echo(f"Checked {result.checked_artifacts} artifacts; {len(result.issues)} issues")
    if not result.valid:
        raise click.ClickException(f"Release validation failed; see {report}")


@release.command("summarize")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
def release_summarize(run_dir: Path) -> None:
    """Print release-level counts derived from the canonical manifest."""
    import json

    from rdfsolve.release.model import ReleaseManifest
    from rdfsolve.release.summary import summarize_release

    path = run_dir / "release.json"
    if not path.exists():
        raise click.ClickException("release.json is missing; run `rdfsolve release build` first")
    manifest = ReleaseManifest.model_validate_json(path.read_text(encoding="utf-8"))
    click.echo(json.dumps(summarize_release(manifest), indent=2, sort_keys=True))


@release.command("compare-declared")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--output",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Output directory. Defaults to RUN_DIR/analysis/observed_declared.",
)
def release_compare_declared(run_dir: Path, output: Path | None) -> None:
    """Compare frozen empirical patterns with simple provider SHACL declarations."""
    from rdfsolve.analysis.release_declared import (
        build_release_declared_comparison,
        write_release_declared_comparison,
    )
    from rdfsolve.release.model import ReleaseManifest

    manifest_path = run_dir / "release.json"
    if not manifest_path.exists():
        raise click.ClickException("release.json is missing; run `rdfsolve release build` first")
    manifest = ReleaseManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    result = build_release_declared_comparison(manifest, run_dir)
    out_dir = output or run_dir / "analysis" / "observed_declared"
    json_path, tsv_path = write_release_declared_comparison(result, out_dir)
    click.echo(
        f"Compared {len(result.compared_datasets)} datasets; "
        f"wrote {len(result.comparisons)} rows to {json_path} and {tsv_path}"
    )
    if result.skipped:
        click.echo(f"Skipped {len(result.skipped)} ambiguous/unreadable datasets")


@release.command("acquire-ontologies")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--max-artifacts", type=int, default=None, help="Bound ontology downloads for pilot runs."
)
def release_acquire_ontologies(run_dir: Path, max_artifacts: int | None) -> None:
    """Download unique configured/reference ontologies and write usage assessments."""
    from rdfsolve.evidence.ontology_reference import acquire_reference_ontologies

    result = acquire_reference_ontologies(run_dir, max_artifacts=max_artifacts)
    click.echo(result.model_dump_json(indent=2))
