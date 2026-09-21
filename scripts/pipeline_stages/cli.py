"""Cli operations for the pipeline command."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from rdfsolve.qlever.inputs import unusable_inputs

from .analysis import AnalysisStage, SSSOMSeedingStage
from .base import Stage
from .cloud import LsLodCloudStage
from .config import PipelineConfig
from .grouped import GroupedMiningStage
from .local import LocalMiningStage
from .remote import RemoteMiningStage

log = logging.getLogger(__name__)


class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.stages: list[Stage] = []
        self.results: dict[str, Any] = {}

    def add_stage(self, stage_cls: type[Stage]) -> Pipeline:
        self.stages.append(stage_cls(self.config))
        return self

    def run(self) -> dict[str, Any]:
        log.info("=" * 70)
        log.info("RDFSOLVE LOD CLOUD ANALYSIS PIPELINE")
        log.info("=" * 70)
        log.info(f"Sources: {len(self.config.sources)}")
        log.info(f"  Remote: {len(self.config.get_remote_sources())}")
        log.info(f"  Local: {len(self.config.get_local_sources())}")
        log.info(f"Output: {self.config.output_dir}")
        log.info("")

        start = time.time()

        for stage in self.stages:
            self.results[stage.name] = stage.run()

            if not self.results[stage.name].get("success"):
                log.error(f"Pipeline aborted at stage: {stage.name}")
                break

        elapsed = time.time() - start
        self.results["total_elapsed_seconds"] = elapsed

        log.info("")
        log.info("=" * 70)
        log.info(f"PIPELINE FINISHED in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log.info("=" * 70)

        suffix = self.config.output_suffix or ""
        results_filename = f"pipeline_results{suffix}.json"
        results_path = self.config.output_dir / results_filename
        results_path.write_text(json.dumps(self.results, indent=2, default=str))
        log.info(f"Results saved to: {results_path}")

        return self.results


def preflight(config: PipelineConfig, *, grouped: bool, remote: bool) -> None:
    """Check configuration and cached inputs. Do not query or start servers."""
    log.info(
        "Query budgets: %d rows/page, %d classes/batch, %d MiB/response, %d remote hosts",
        config.chunk_size,
        config.class_batch_size,
        config.max_response_bytes // (1024 * 1024),
        config.parallelism,
    )
    if remote:
        selected = config.get_remote_sources()
        if not selected:
            raise ValueError("No remote sources selected")
        log.info(
            "Preflight: %d remote sources; endpoint availability is checked during mining",
            len(selected),
        )
        return
    stage = GroupedMiningStage(config) if grouped else LocalMiningStage(config)
    stage._ensure_qlever_image()
    import shutil

    if shutil.which("singularity") is None:
        raise FileNotFoundError("singularity is not on PATH")
    sources = config.get_local_sources()
    if not sources:
        raise ValueError("No local sources selected")
    covered: set[str] = set()
    rejected: list[str] = []
    if grouped:
        for name, members in stage._identify_groups(sources).items():
            workdir = config.data_dir / "qlever_groups" / name
            try:
                cached = stage._has_qlever_index(workdir, name)
            except ValueError as error:
                rejected.append(f"{name}: {error}")
                continue
            if cached:
                covered.update(source.name for source in members)
                log.info("Cached group %s: %s", name, workdir)
    missing: list[str] = []
    unusable: list[str] = []
    for source in sources:
        if source.name in covered:
            continue
        workdir = config.data_dir / "qlever_workdirs" / source.name
        try:
            cached = stage._has_qlever_index(workdir, source.name)
        except ValueError as error:
            rejected.append(f"{source.name}: {error}")
            cached = False
        else:
            if cached:
                log.info("Cached source %s: %s", source.name, workdir)
            else:
                missing.append(source.name)
        if not cached:
            unusable.extend(
                f"{source.name}: {path} ({reason})" for path, reason in unusable_inputs(workdir)
            )
    for entry in rejected:
        log.error("Unusable index: %s", entry)
    for entry in unusable:
        log.error("Unusable input: %s", entry)
    if missing:
        log.warning("No cached index for %d sources: %s", len(missing), missing)
    problems = []
    if rejected:
        problems.append(f"{len(rejected)} unusable indices")
    if unusable:
        problems.append(f"{len(unusable)} unusable inputs")
    if missing and config.no_index:
        problems.append(f"missing indices: {missing}")
    if problems:
        raise FileNotFoundError(
            "Prepare inputs or narrow --sources. " + "; ".join(problems) + ". See the logged paths."
        )
    log.info(
        "Preflight: %d local sources are ready to mine or index; "
        "loadability is checked at startup",
        len(sources),
    )


def main():
    parser = argparse.ArgumentParser(
        description="RDFSolve LOD Cloud Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/pipeline.py --sources wikipathways aopwikirdf
    python scripts/pipeline.py --remote-only
    python scripts/pipeline.py --local-only
    python scripts/pipeline.py --skip-mining --skip-mappings
        """,
    )

    parser.add_argument("--remote-only", action="store_true", help="Remote mining only")
    parser.add_argument("--local-only", action="store_true", help="Local mining only")
    parser.add_argument(
        "--grouped-only", action="store_true", help="Grouped mining only (related sources together)"
    )
    parser.add_argument(
        "--lslod-cloud-only",
        action="store_true",
        help="LSLOD Cloud mining only (all local sources)",
    )
    parser.add_argument("--sources", nargs="+", help="Specific source names")
    parser.add_argument("--sources-file", type=Path, help="Source registry YAML")
    parser.add_argument(
        "--preflight", action="store_true", help="Check selected inputs without mining"
    )
    parser.add_argument("--no-index", action="store_true", help="Use prepared indices only")
    parser.add_argument(
        "--skip-providers", nargs="+", help="Skip sources from these providers (e.g., idsm)"
    )
    parser.add_argument(
        "--exclude-graph",
        action="append",
        default=[],
        metavar="PREFIX",
        help="Graph IRI prefix to skip when discovering graphs; repeat to replace the default list",
    )
    parser.add_argument("--skip-mining", action="store_true", help="Skip mining stages")
    parser.add_argument("--skip-mappings", action="store_true", help="Skip mapping stages")
    parser.add_argument("--skip-inference", action="store_true", help="No effect; the pipeline has no mapping inference stage")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip analysis stage")
    parser.add_argument(
        "--skip-completed",
        action="store_true",
        help="Skip sources with existing schema output files",
    )
    parser.add_argument(
        "--ontology-as-data",
        action="store_true",
        help="Opt in to bounded superclass aggregation (not observed typing)",
    )
    parser.add_argument(
        "--get-graphs-from-store",
        action="store_true",
        help="Mine explicitly configured small Graph Store downloads locally; fail on retrieval errors",
    )
    parser.add_argument(
        "--graph-store-url",
        action="append",
        default=[],
        metavar="SOURCE=URL",
        help="Explicit Graph Store service for one source; repeat as needed",
    )
    parser.add_argument(
        "--extract-ontology",
        action="store_true",
        help="Extract ontology structure (TBox: rdfs:subClassOf, domain/range)",
    )
    parser.add_argument(
        "--extract-metadata",
        action="store_true",
        help="Extract infrastructure metadata (VoID/DCAT)",
    )
    parser.add_argument(
        "--discover-ontology-graphs",
        action="store_true",
        help=(
            "After schema mining, scan named graphs for ontology material and save "
            "version/header evidence plus empirical term overlap"
        ),
    )
    parser.add_argument(
        "--ontology-discovery-max-graphs",
        type=int,
        default=500,
        help="Maximum named graphs inspected by ontology discovery (default: 500)",
    )
    parser.add_argument(
        "--property-usage-evidence",
        action="store_true",
        help=(
            "Collect class/property subject support over the selected graph scope; "
            "writes a separate empirical-evidence artifact"
        ),
    )
    parser.add_argument(
        "--property-value-profiles",
        action="store_true",
        help=(
            "Also collect node-kind, datatype, and language-tag profiles for "
            "class/property evidence; adds aggregate queries"
        ),
    )
    parser.add_argument(
        "--property-value-histograms",
        action="store_true",
        help=(
            "Also collect bounded per-subject value-count histograms; intended "
            "primarily for local indexes because it requires nested aggregation"
        ),
    )
    parser.add_argument(
        "--declared-artifacts",
        action="store_true",
        help=(
            "Archive explicitly configured provider SHACL/SPARQL-example RDF "
            "separately from empirical mining and project a documented subset "
            "as declared evidence"
        ),
    )
    parser.add_argument(
        "--no-enrichment", action="store_true", help="Skip definitions and observed examples"
    )
    parser.add_argument(
        "--navigation-hops",
        type=int,
        choices=[0, 2, 3, 4, 5, 6],
        default=5,
        help="Compose schema routes locally; 0 disables (no endpoint queries)",
    )
    parser.add_argument(
        "--navigation-min-hops",
        type=int,
        choices=[2, 3, 4, 5, 6],
        default=3,
        help="Lowest hop bound accepted when longer routes cannot be composed",
    )
    parser.add_argument(
        "--navigation-probes",
        type=int,
        default=0,
        help="Maximum joined-path support queries; zero keeps discovery local",
    )
    parser.add_argument(
        "--navigation-limit",
        type=int,
        default=100,
        help="Maximum saved candidate routes per hop length",
    )
    parser.add_argument(
        "--examples-per-pattern",
        type=int,
        default=1,
        choices=range(0, 21),
        help="Examples per class and pattern (0: definitions only; default: 1)",
    )
    parser.add_argument(
        "--trim-descriptions",
        type=int,
        default=None,
        help="Maximum description characters in exports; omit to keep full text (lossy)",
    )
    parser.add_argument(
        "--ontology-scope",
        choices=["schema", "full"],
        default="schema",
        help="Export schema-relevant ontology or all queried axioms",
    )
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    parser.add_argument(
        "--output-suffix",
        type=str,
        default="",
        help="Suffix for output files (e.g., _local, _remote)",
    )
    parser.add_argument(
        "--output-formats",
        nargs="+",
        choices=["void", "json-ld", "shacl", "pydantic", "json"],
        default=["json-ld", "void"],
        help="Output format(s) to generate (default: json-ld void)",
    )
    parser.add_argument("--base-port", type=int, default=7019, help="First local QLever port")
    parser.add_argument(
        "--qlever-startup-timeout",
        type=int,
        default=600,
        help="Seconds to wait for an index to load",
    )
    parser.add_argument("--data-dir", type=Path, help="Data-directory")
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Use existing RDF and indices; do not fetch source data",
    )
    parser.add_argument(
        "--parallelism", type=int, default=4, help="Maximum concurrent remote hosts"
    )
    parser.add_argument("--chunk-size", type=int, default=10000, help="Rows per query page")
    parser.add_argument(
        "--class-chunk-size",
        type=int,
        help="Page the class listing at this size; unset runs it as one query",
    )
    parser.add_argument("--class-batch-size", type=int, default=15, help="Classes per query batch")
    parser.add_argument(
        "--max-response-mb", type=int, default=64, help="Decompressed response limit in MiB"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Override query timeout in seconds (default: source setting)",
    )
    parser.add_argument("--endpoint-status-file", type=Path, help="Endpoint health check JSON")
    parser.add_argument("--download-status-file", type=Path, help="Download health check JSON")

    args = parser.parse_args()

    import signal

    def stop_on_signal(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop_on_signal)
    repo_dir = Path(__file__).resolve().parents[2]
    config = PipelineConfig(base_dir=repo_dir.parent, repo_dir=repo_dir)
    if args.sources_file:
        config.sources_file = args.sources_file.resolve()

    if args.output_dir:
        config.output_dir = args.output_dir
    if args.data_dir:
        config.data_dir = args.data_dir
    if not 1024 <= args.base_port <= 60000:
        parser.error("--base-port must be between 1024 and 60000")
    if args.qlever_startup_timeout <= 0:
        parser.error("--qlever-startup-timeout must be positive")
    config.base_port = args.base_port
    config.qlever_startup_timeout = args.qlever_startup_timeout
    if min(args.parallelism, args.chunk_size, args.class_batch_size, args.max_response_mb) < 1:
        parser.error("Request and concurrency limits must be positive")
    if args.timeout is not None and args.timeout <= 0:
        parser.error("--timeout must be positive")
    config.parallelism = args.parallelism
    if args.exclude_graph:
        config.exclude_graph_prefixes = tuple(args.exclude_graph)
    config.chunk_size = args.chunk_size
    config.class_batch_size = args.class_batch_size
    config.class_chunk_size = args.class_chunk_size
    config.max_response_bytes = args.max_response_mb * 1024 * 1024
    config.timeout = args.timeout
    config.get_graphs_from_store = args.get_graphs_from_store
    for value in args.graph_store_url:
        source_name, separator, url = value.partition("=")
        if not source_name or not separator or not url:
            parser.error("--graph-store-url requires SOURCE=URL")
        config.graph_store_urls[source_name] = url
    if config.get_graphs_from_store and not config.graph_store_urls:
        parser.error("--get-graphs-from-store requires --graph-store-url SOURCE=URL")
    config.no_download = args.no_download
    config.no_index = args.no_index
    config.output_suffix = args.output_suffix
    config.output_formats = args.output_formats
    config.endpoint_status_file = args.endpoint_status_file
    config.download_status_file = args.download_status_file
    config.skip_mining = args.skip_mining
    config.skip_mappings = args.skip_mappings
    config.skip_inference = args.skip_inference
    config.skip_completed = args.skip_completed
    config.skip_remote = args.local_only or args.grouped_only or args.lslod_cloud_only
    config.skip_local = args.remote_only or args.grouped_only or args.lslod_cloud_only
    config.extract_ontology = args.extract_ontology
    config.ontology_scope = args.ontology_scope
    config.ontology_as_data = args.ontology_as_data
    config.discover_ontology_graphs = args.discover_ontology_graphs
    config.ontology_discovery_max_graphs = args.ontology_discovery_max_graphs
    if config.ontology_discovery_max_graphs < 1:
        parser.error("--ontology-discovery-max-graphs must be positive")
    config.extract_metadata = args.extract_metadata
    config.collect_property_usage_evidence = args.property_usage_evidence
    config.collect_property_value_profiles = args.property_value_profiles
    config.collect_property_value_histograms = args.property_value_histograms
    config.collect_declared_artifacts = args.declared_artifacts
    config.enrich = not args.no_enrichment
    config.examples_per_pattern = args.examples_per_pattern
    config.trim_descriptions = args.trim_descriptions
    if config.trim_descriptions is not None and config.trim_descriptions < 0:
        parser.error("--trim-descriptions must be nonnegative")
    config.navigation_probes = args.navigation_probes
    if config.navigation_probes < 0 or (config.navigation_probes and not args.navigation_hops):
        parser.error("--navigation-probes requires navigation hops and a nonnegative budget")
    config.navigation_hops = args.navigation_hops
    config.navigation_min_hops = args.navigation_min_hops
    config.navigation_limit = args.navigation_limit
    if config.navigation_limit < 0:
        parser.error("--navigation-limit must be nonnegative")

    config.load_sources(args.sources, skip_providers=args.skip_providers)

    if not config.sources:
        log.error("No sources loaded. Check sources.yaml or --sources argument.")
        sys.exit(1)

    if args.preflight:
        preflight(config, grouped=args.grouped_only, remote=args.remote_only)
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline(config)

    if args.grouped_only:
        pipeline.add_stage(GroupedMiningStage)
    elif args.lslod_cloud_only:
        pipeline.add_stage(LsLodCloudStage)
    else:
        if not config.skip_mining:
            if not config.skip_remote:
                pipeline.add_stage(RemoteMiningStage)
            if not config.skip_local:
                pipeline.add_stage(LocalMiningStage)

        if not config.skip_mappings:
            pipeline.add_stage(SSSOMSeedingStage)

        if not args.skip_analysis:
            pipeline.add_stage(AnalysisStage)

    results = pipeline.run()

    for stage_results in results.values():
        if isinstance(stage_results, dict) and not stage_results.get("success", True):
            sys.exit(1)

    sys.exit(0)
