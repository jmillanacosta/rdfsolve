# RDFSolve Scripts

Workflow scripts for mining, mapping, and analyzing RDF schemas.

## Mining Scripts

### `pipeline.py`
Main pipeline for mining schemas from SPARQL endpoints.

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
