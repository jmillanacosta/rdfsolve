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

# Mine local RDF dumps
python scripts/pipeline.py --local-only

# Full pipeline
python scripts/pipeline.py
```

**Outputs per source:**
- `{source}_schema.jsonld` - JSON-LD with descriptions, cardinality, examples
- `{source}_void.ttl` - VoID metadata
- `{source}_schema.json` - JSON Schema with `$ref` (domain classes only)
- `{source}_models.py` - Pydantic models (domain classes only)
- `{source}_shapes.ttl` - SHACL shapes with constraints
- `{source}_report.json` - Mining statistics

### `check_metadata_endpoints.py`
Test metadata capture across all endpoints.

```bash
python scripts/check_metadata_endpoints.py
```

### `check_endpoints.py`
Health check for SPARQL endpoints.

```bash
python scripts/check_endpoints.py --output endpoint_status.json
```

### `check_downloads.py`
Health check for downloadable RDF dumps.

```bash
python scripts/check_downloads.py --output download_status.json
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

Numbered pipeline for large-scale HPC mining. Run from `scripts/` directory:

### `01_mine_remote.sh`
Mine remote SPARQL endpoints.

```bash
sbatch 01_mine_remote.sh
```

Queries remote SPARQL endpoints and generates all output formats.

### `02_mine_local.sh`
Download and index local RDF dumps with QLever.

```bash
sbatch 02_mine_local.sh
```

Downloads RDF dumps, indexes with QLever, mines schemas.

### `03_mine_grouped.sh`
Grouped mining + LSLOD cloud.

```bash
sbatch 03_mine_grouped.sh
```

Mines multi-file provider groups and creates mega-QLever instance for cross-dataset SSSOM mapping generation.

### `04_mappings.sh`
Generate cross-dataset mappings.

```bash
sbatch 04_mappings.sh
```

Runs the mapping pipeline: external mappings, cross-references with classes, class mapping inference, and consolidation with semra.

### `05_analysis.sh`
Analysis and visualization.

```bash
sbatch 05_analysis.sh
```

Generates cross-dataset analysis, graphs, and reports.

## Environment Variables

Override defaults via environment:

```bash
export RDFSOLVE_BASE=/path/to/rdfsolve
export OUTPUT_DIR=/path/to/output
export DATA_DIR=/path/to/data
export TIMEOUT=600
export SKIP_COMPLETED=true  # Skip sources with existing output
sbatch 01_mine_remote.sh
```

Default values:
- `RDFSOLVE_BASE`: `$(pwd)/..` (parent of scripts directory)
- `OUTPUT_DIR`: `$RDFSOLVE_BASE/output_YYYY-MM-DD`
- `DATA_DIR`: `$RDFSOLVE_BASE/data`
- `SKIP_COMPLETED`: `false` (re-mine all sources)
- `TIMEOUT`: `300` (remote), `600` (local)
- `SKIP_PROVIDERS`: Space-separated list to exclude (e.g., `"idsm bio2rdf"`)
- SLURM logs: `logs/%x_%j.out` (relative to execution directory)
