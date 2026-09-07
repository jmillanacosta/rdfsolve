# RDFSolve Scripts

Workflow scripts for mining, mapping, and analyzing RDF schemas.

## Mining Scripts

### `pipeline.py`
Main pipeline for mining schemas from SPARQL endpoints.

All mining modes retrieve source definitions and up to two examples per class
and observed pattern before export. Set `--examples-per-pattern N` (0–20), or
`--no-enrichment` to skip these queries. Zero retrieves definitions only. Queries
use the mining graph scope and timeout. Ten sample queries share each request.
Samples follow endpoint order; they are not random or representative.

Canonical `*_schema.json` keeps RDF node kinds, literal datatypes, languages,
definitions, examples, and query failures. Pydantic uses class labels as names,
definitions as docstrings, and observed values as field examples. Examples are
not defaults. Missing source definitions remain missing.

The JSON envelope's `version` identifies the storage format. `schema_version`
identifies the source release: version IRI, release label, source date, then a
dated mining snapshot. A snapshot is not an upstream release. Metadata from an
unrelated imported ontology is not used as the dataset's metadata.

To reuse local data when upstream sources are unavailable:

```bash
python scripts/pipeline.py --grouped-only --no-download --data-dir ../data --output-dir ../new-run
```

Existing Qleverfiles are kept unchanged. Existing local and grouped indices are
used before the pipeline tries to fetch source data. `--no-download` blocks source
downloads; it does not block mining queries or a missing QLever image pull. Keep
`qlever.sif` in the data directory for runs without image access. A partial index
must be inspected before rebuilding. Cached RDF files are not rewritten.

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
Convert SeMRA/SSSOM files to rdfsolve format. Preserves `mapping_justification` field.

```bash
python scripts/convert_semra.py mappings.sssom.tsv -o output.jsonld
```

### `infer_mappings.py`
Run inference on mapping files (inversion, transitivity). Preserves `mapping_justification`.

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
