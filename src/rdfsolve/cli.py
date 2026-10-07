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


@registry.command("rdf")
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
    "outputs",
    type=click.Path(dir_okay=False, path_type=Path),
    multiple=True,
    required=True,
    help="File to write; .ttl, .jsonld or .nt. Repeat for several formats.",
)
def registry_rdf(sources: Path | None, overrides: Path | None, outputs: tuple[Path, ...]) -> None:
    """Describe the registry and its identity decisions as RDF (DCAT, VoID, SD)."""
    from rdfsolve.registry_rdf import write_registry_rdf
    from rdfsolve.sources import DEFAULT_SOURCES_YAML

    try:
        graph = write_registry_rdf(sources or DEFAULT_SOURCES_YAML, outputs, overrides)
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    for path in outputs:
        click.echo(f"Wrote {path} ({len(graph)} triples)")


@registry.command("enrich-bioregistry")
@click.option(
    "--sources",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Registry YAML. Defaults to data/sources.yaml.",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Separate YAML file for the refresh proposal.",
)
@click.option("--name", "names", multiple=True, help="Refresh only selected source names.")
def enrich_bioregistry_registry(sources: Path | None, output: Path, names: tuple[str, ...]) -> None:
    """Refresh Bioregistry metadata without changing local download classifications."""
    import json

    from rdfsolve.sources import DEFAULT_SOURCES_YAML, enrich_registry_with_bioregistry

    path = sources or DEFAULT_SOURCES_YAML
    report = enrich_registry_with_bioregistry(path, output=output, names=set(names) or None)
    click.echo(json.dumps(report, indent=2, sort_keys=True))


@main.group()
def release() -> None:
    """Build and inspect frozen evidence-corpus releases."""


@release.command("build")
@click.argument("run_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--release-id", default=None, help="Stable release identifier; generated when omitted."
)
def release_build(run_dir: Path, release_id: str | None) -> None:
    """Build registry.ttl, release.json and release.ttl from one completed run directory."""
    from rdfsolve.registry_rdf import write_registry_rdf
    from rdfsolve.release import (
        build_release_manifest,
        release_to_rdf,
        summarize_release,
        write_release_manifest,
        write_release_summary,
    )
    from rdfsolve.version import VERSION

    # The registry's RDF is written first, so the manifest lists it as an artifact.
    registry_ttl = run_dir / "registry.ttl"
    if (run_dir / "sources.yaml").exists():
        try:
            write_registry_rdf(run_dir / "sources.yaml", [registry_ttl])
            click.echo(f"Wrote {registry_ttl}")
        except ValueError as error:
            # As for the identity review, a broken registry is reported, not fatal.
            registry_ttl.unlink(missing_ok=True)
            click.echo(f"registry.ttl not written: {error}", err=True)
    manifest = build_release_manifest(run_dir, release_id=release_id, rdfsolve_version=VERSION)
    json_path = write_release_manifest(manifest, run_dir)
    ttl_path = run_dir / "release.ttl"
    ttl_path.write_text(release_to_rdf(manifest).serialize(format="turtle"), encoding="utf-8")
    summary_json, summary_tsv = write_release_summary(summarize_release(manifest, run_dir), run_dir)
    click.echo(f"Wrote {json_path}")
    click.echo(f"Wrote {ttl_path}")
    click.echo(f"Wrote {summary_json}")
    click.echo(f"Wrote {summary_tsv}")


@release.command("assemble")
@click.argument(
    "run_dirs",
    nargs=-1,
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--output", required=True, type=click.Path(path_type=Path), help="New run directory to write."
)
def release_assemble(run_dirs: tuple[Path, ...], output: Path) -> None:
    """Join the chunked runs of one frozen corpus into one run directory for release build."""
    from rdfsolve.release.assemble import assemble_runs

    out = assemble_runs(list(run_dirs), output)
    click.echo(f"Wrote {out} from {len(run_dirs)} runs; see {out / 'assembly.json'}")


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
    click.echo(json.dumps(summarize_release(manifest, run_dir), indent=2, sort_keys=True))


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
    from rdfsolve.ontology.reference import acquire_reference_ontologies

    result = acquire_reference_ontologies(run_dir, max_artifacts=max_artifacts)
    click.echo(result.model_dump_json(indent=2))


@main.command("export-graphs")
@click.option(
    "--sources",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Registry YAML with the entries to export.",
)
@click.option("--name", "names", multiple=True, help="Entry to export; repeatable.")
@click.option("--host", "hosts", multiple=True, help="Export every entry on this endpoint host.")
@click.option(
    "--output-root",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Exports go to OUTPUT_ROOT/<name>/<stamp>/.",
)
@click.option("--stamp", required=True, help="Folder name of this export, such as 20261006.")
@click.option("--cap", type=int, default=50_000_000, show_default=True, help="LIMIT per CONSTRUCT.")
@click.option(
    "--max-gb", type=float, default=300.0, show_default=True, help="Stop a source past this size."
)
@click.option(
    "--max-hours", type=float, default=72.0, show_default=True, help="Stop a source past this."
)
@click.option(
    "--max-requests", type=int, default=5000, show_default=True, help="CONSTRUCTs per source."
)
@click.option(
    "--count-seconds", type=float, default=900.0, show_default=True, help="Budget of a COUNT."
)
@click.option(
    "--read-timeout", type=float, default=1800.0, show_default=True, help="Longest silence."
)
def export_graphs(
    sources: Path,
    names: tuple[str, ...],
    hosts: tuple[str, ...],
    output_root: Path,
    stamp: str,
    cap: int,
    max_gb: float,
    max_hours: float,
    max_requests: int,
    count_seconds: float,
    read_timeout: float,
) -> None:
    """Export every graph of the selected entries' endpoints to verified gzip files."""
    import json
    import logging

    import yaml

    from rdfsolve.graph_export import ExportBudgetError, entries_on_host, export_source

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    entries = yaml.safe_load(sources.read_text(encoding="utf-8"))
    # In the order given: a caller lists small sources first.
    by_name = {e["name"]: e for e in entries}
    chosen = [by_name[n] for n in dict.fromkeys(names) if n in by_name]
    for host in hosts:
        chosen += [e for e in entries_on_host(entries, host) if e not in chosen]
    missing = set(names) - {e["name"] for e in chosen}
    if missing or not chosen:
        raise click.ClickException(f"No such entries: {sorted(missing) or 'none selected'}")
    for entry in chosen:
        try:
            manifest = export_source(
                entry,
                output_root,
                stamp,
                cap=cap,
                max_bytes=int(max_gb * 10**9),
                max_seconds=max_hours * 3600,
                max_requests=max_requests,
                count_seconds=count_seconds,
                read_timeout=read_timeout,
            )
        except ExportBudgetError as error:
            click.echo(
                json.dumps(
                    {"source": entry["name"], "outcome": "not started", "reason": str(error)}
                )
            )
            continue
        graphs = manifest.get("graphs", [])
        click.echo(
            json.dumps(
                {
                    "source": entry["name"],
                    "outcome": manifest.get("outcome"),
                    "graphs": len(graphs),
                    "retrieved": sum(str(g["outcome"]).startswith("retrieved") for g in graphs),
                    "bytes": manifest.get("bytes"),
                    "seconds": manifest.get("seconds"),
                    "reason": manifest.get("reason"),
                }
            )
        )


@main.command("export-local")
@click.argument(
    "export_dirs",
    nargs=-1,
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--workdir-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="QLever work folders go to WORKDIR_ROOT/<name> (a data directory's qlever_workdirs); "
    "by default to qlever/ inside each export folder.",
)
def export_local(export_dirs: tuple[Path, ...], workdir_root: Path | None) -> None:
    """Make complete exports local inputs: a prepared QLever folder and the registry fields."""
    import json

    import yaml

    from rdfsolve.graph_export import prepare_workdir

    for export_dir in export_dirs:
        manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
        name = manifest["source"]
        workdir = workdir_root / name if workdir_root else export_dir / "qlever"
        try:
            fields = prepare_workdir(export_dir, workdir)
        except ValueError as error:
            click.echo(f"# {name}: not exported-local: {error}")
            continue
        click.echo(f"# {name}: QLever folder {workdir}")
        click.echo(yaml.safe_dump({name: fields}, sort_keys=False))
