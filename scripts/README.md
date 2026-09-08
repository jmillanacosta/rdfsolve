# RDFSolve Scripts

Workflow scripts for mining, mapping, and analyzing RDF schemas.

## Mining Scripts

### `pipeline.py`

Main pipeline for mining schemas from SPARQL endpoints.

To reuse local data when upstream sources are unavailable:

```bash
python scripts/pipeline.py --grouped-only --no-download \
    --data-dir ../data --output-dir ../new-run
```

Existing Qleverfiles are kept unchanged.

```bash
# Mine all remote endpoints
python scripts/pipeline.py --remote-only

# Mine specific sources
python scripts/pipeline.py --sources wikipathways aopwikirdf

# Full pipeline
python scripts/pipeline.py
```

### `test_metadata_endpoints.py`

Test metadata capture across all endpoints.

```bash
python scripts/test_metadata_endpoints.py
```

## Mapping Scripts

### `convert_semra.py`

Convert SeMRA/SSSOM files to rdfsolve format. Preserves `mapping_justification`
field.

```bash
python scripts/convert_semra.py mappings.sssom.tsv -o output.jsonld
```

### `infer_mappings.py`

Run inference on mapping files (inversion, transitivity). Preserves
`mapping_justification`.

```bash
python scripts/infer_mappings.py \
    mappings/*.jsonld \
    -o inferenced.jsonld \
    --inversion \
    --transitivity \
    --chain-cutoff 3
```

## Graph Scripts

### `build_graphs.py`

Build connectivity graphs from mined schemas.

```bash
python scripts/build_graphs.py \
    output/schemas/ \
    --output results/graphs/ \
    --mappings output/mappings/
```

## SLURM Jobs

Set SLURM parameters (cpus, mem, time) from the slurm `.sh` files.

### `slurm_remote.sh`

Mine remote SPARQL endpoints.

```bash
sbatch scripts/slurm_remote.sh
```

### `slurm_local.sh`

Download and index local RDF dumps with QLever.

```bash
sbatch scripts/slurm_local.sh
```

### `slurm_inference.sh`

Run mapping inference pipeline.

```bash
sbatch scripts/slurm_inference.sh
```

### `slurm_graphs.sh`

Build connectivity graphs.

```bash
sbatch scripts/slurm_graphs.sh
```

### `slurm_full.sh`

Run complete pipeline.

```bash
sbatch scripts/slurm_full.sh
```

### `slurm_void_discovery.sh`

Discover VoID descriptions.

```bash
sbatch scripts/slurm_void_discovery.sh
```

## Environment Variables

Override defaults via environment:

```bash
export RDFSOLVE_BASE=/path/to/rdfsolve
export OUTPUT_DIR=/path/to/output
export TIMEOUT=600
sbatch scripts/slurm_remote.sh
```

## Read VoID and test Graph Store access

Discovery returns a `VoidSchema`, not an export receipt. It does not write files
unless `output_dir` is set.

```python
from rdfsolve import MinedSchema, discover_void_source

void = discover_void_source(
    endpoint="https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
    name="aopwikirdf",
)
print(void.has_void, void.has_partitions, void.has_patterns)
datasets = void.datasets
schema = void.to_mined_schema()
# Equivalent retrieval and conversion:
schema = MinedSchema.from_void_source(
    endpoint="https://aopwiki.rdf.bigcat-bioinformatics.org/sparql",
    name="aopwikirdf",
)
```

`void.graph` retains retrieved RDF; typed views cover supported fields.
Metadata-only VoID is useful even when `schema.patterns` is empty.
`void.class_partitions` also exposes standalone class-count partitions.

Graph Store retrieval is opt-in through `get_graphs_from_store=True`,
`graph_store_url`, and explicit `graph_uris`. Both discovery and `SchemaMiner`
accept these options. Downloads have byte and time limits. Their parsed triple
counts must match the SPARQL graph counts before use. Count agreement is a
check, not proof that both services expose identical RDF.

The local Graph Store mining path uses RDFLib memory and the existing mining
queries. The default download cap is 64 MiB, not a RAM limit. Keep large
datasets in the disk-backed QLever workflow. Retrieval failures stop the run;
they do not switch silently to another data source.

```bash
uv run scripts/test_aopwiki_graph_store.py --output-dir ../graph-store-aopwiki
# Add --mine to run local instance mining after the count check.
```

At the September 8 check, AOPWiki returned 10,001 Graph Store triples for
`http://aopwiki.org/`, while SPARQL counted 338,061. The script rejects this
response. Do not treat it as a complete local dataset.

For configured pipeline sources:

```bash
uv run scripts/pipeline.py --remote-only --sources aopwikirdf \
  --get-graphs-from-store \
  --graph-store-url aopwikirdf=https://aopwiki.rdf.bigcat-bioinformatics.org/sparql-graph-crud/
```

The source must declare its graph scope. A Graph Store failure does not mark the
SPARQL endpoint as down.

Remote SPARQL and Graph Store requests share one in-flight slot per host in each
process. POSIX applications can set `RDFSOLVE_HTTP_LOCK_DIR` to coordinate
processes through shared storage. The mining launcher sets this explicitly. Use
that same directory in interactive sessions to coordinate with jobs. Restart
existing Python sessions to load changes; old processes do not gain these limits
automatically.
