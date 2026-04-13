# Pipeline scripts

These are the scripts used to produce and analyze the materials for the rdfsolve paper.

The catalog used to identify resources, their endpoints and RDF dump URLs is [sources.yaml](../data/sources.yaml).

All pipeline logic lives in the `rdfsolve` Python package; the remaining
shell scripts in this directory orchestrate HPC (SLURM) and utility workflows.

---

## Mining (via `rdfsolve` CLI)

The mining commands are built into the `rdfsolve` package.

### Discover VoID descriptions from remote endpoints
```bash
rdfsolve discover
```

### Mine schemas from remote SPARQL endpoints
```bash
rdfsolve mine
```
Filter to specific sources:
```bash
rdfsolve mine --filter "chembl|drugbank"
```

### Mine schemas from a local QLever endpoint
```bash
rdfsolve local-mine \
    --endpoint http://localhost:7001 \
    --name drugbank
```
With VoID discovery first:
```bash
rdfsolve local-mine \
    --endpoint http://localhost:7001 \
    --name drugbank \
    --discover-first
```

### Generate Qleverfiles
```bash
rdfsolve qleverfile --data-dir /data/rdf
```

---

## Seeding (via `rdfsolve` CLI)

### Enrich sources.yaml with Bioregistry metadata
```bash
rdfsolve bioregistry-enrich
```
Dry-run (preview only):
```bash
rdfsolve bioregistry-enrich --dry-run
```
Specific sources:
```bash
rdfsolve bioregistry-enrich --names drugbank chebi
```

### Seed schemas from endpoints
```bash
rdfsolve mine
```
Specific sources:
```bash
rdfsolve mine --filter "aopwikirdf|wikipathways"
```

### Seed instance mappings
```bash
rdfsolve instance-match seed --prefixes ensembl
```
Multiple prefixes, restrict to specific datasets:
```bash
rdfsolve instance-match seed \
    --prefixes ensembl uniprot chebi \
    --datasets aopwikirdf wikipathways
```

### Seed SeMRA mappings
```bash
rdfsolve semra seed --sources fplx
```
Import all registered sources:
```bash
rdfsolve semra seed --sources all
```

### Seed SSSOM mappings
```bash
rdfsolve sssom seed
```
List available sources (dry-run):
```bash
rdfsolve sssom seed --list
```

### Run inference over mappings
```bash
rdfsolve inference seed
```
Skip transitivity, enable generalisation:
```bash
rdfsolve inference seed --no-transitivity --generalisation
```

---

## Graph building (via `rdfsolve` CLI)

### Build connectivity graphs and export to Parquet
```bash
rdfsolve build-graphs
```
Filter to specific datasets:
```bash
rdfsolve build-graphs --datasets aopwikirdf wikipathways chembl
```
Schema selection only (no graph build):
```bash
rdfsolve build-graphs --schema-only
```

### Build ontology index from OLS4
```bash
rdfsolve ontology-index
```
Specific ontologies:
```bash
rdfsolve ontology-index --ontologies chebi go
```
From schema files:
```bash
rdfsolve ontology-index --from-schemas docker/schemas
```

---

## QLever boot (via `rdfsolve` CLI)

### Boot QLever endpoints from sources.yaml
```bash
rdfsolve qlever-boot --source drugbank
```
List downloadable sources:
```bash
rdfsolve qlever-boot --list-sources
```
Filter by pattern:
```bash
rdfsolve qlever-boot --filter "chebi|drugbank"
```

---

## Full pipeline (HPC / Singularity)

```bash
bash scripts/run_pipeline_hpc.sh
```
Single dataset:
```bash
bash scripts/run_pipeline_hpc.sh --dataset aopwikirdf --skip-remote
```

---

## SLURM jobs

### Full pipeline
```bash
sbatch scripts/slurm_full_pipeline_hpc.sh
```

### Mining only (reuse existing indexes)
```bash
sbatch scripts/slurm_mine_only_hpc.sh
```

### Single source (or multiple)
```bash
sbatch scripts/slurm_single_source.sh cellosaurus
sbatch scripts/slurm_single_source.sh chebi rdfportal.chebi
sbatch scripts/slurm_single_source.sh "aopwikirdf|drugbank"
```

### Test job (aopwikirdf)
AOPWiki RDF is used as a test job since it is a relatively small dataset.
```bash
sbatch scripts/slurm_aopwikirdf_test.sh
```

---

## Utilities

### Estimate download sizes (no actual download)
```bash
bash scripts/estimate_download_sizes.sh
```
For a specific source:
```bash
bash scripts/estimate_download_sizes.sh --source glycosmos
```

### Fix broken IRIs in N-Quads streams
Used to fix large integers in some graphs that made the Qlever indexer choke.
```bash
cat broken.nq | python scripts/nq_iri_fix.py > fixed.nq
```
