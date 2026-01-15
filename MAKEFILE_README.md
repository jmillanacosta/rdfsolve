# Notebook Generation Makefile

This Makefile provides a convenient way to generate and manage RDFSolve notebooks both locally and in CI/CD environments.

## Quick Start

```bash
# Show all available commands
make help

# Install dependencies
make install-deps

# Generate a single schema notebook
make schema DATASET=aopwikirdf

# Generate all notebook types for all datasets
make all

# Test with one dataset
make test-one
```

## Available Targets

### Setup and Installation

- `make install-deps` - Install Python dependencies
- `make install-system-deps` - Install system dependencies (requires sudo, Ubuntu/Debian)
- `make setup` - Create necessary output directories
- `make list-datasets` - List all available datasets from sources.csv

### Single Dataset Operations

Generate notebooks for a specific dataset by setting `DATASET=<name>`:

```bash
# Schema notebook
make schema DATASET=aopwikirdf

# Pydantic model notebook
make pydantic DATASET=aopwikirdf

# Namespace discovery notebook
make namespace DATASET=aopwikirdf
```

### Bulk Operations

Process all datasets:

```bash
make schema-all      # Generate all schema notebooks
make pydantic-all    # Generate all pydantic notebooks
make namespace-all   # Generate all namespace notebooks
make all            # Run complete pipeline
```

### Results Management

- `make collect` - Collect and organize all generated results
- `make generate-results-json` - Generate results.json for web interface
- `make count-results` - Count generated files

### Cleanup

- `make clean` - Remove generated notebooks and HTML files
- `make clean-all` - Remove all generated files including data

## Directory Structure

The Makefile expects and creates the following structure:

```
.
├── data/
│   └── sources.csv              # Dataset definitions
├── notebooks/
│   ├── make_notebooks.py        # Notebook generator script
│   ├── 01_schema_extraction/    # Generated schema notebooks
│   ├── 02_pydantic_models/      # Generated pydantic notebooks
│   └── 03_bioregistry_namespaces/  # Generated namespace notebooks
├── docs/
│   ├── notebooks/               # HTML output from notebooks
│   │   ├── 01_schema_extraction/
│   │   ├── 02_pydantic_models/
│   │   └── 03_bioregistry_namespaces/
│   ├── data/
│   │   └── schema_extraction/   # Generated data files
│   └── results.json            # Summary of all results
└── scripts/
    └── generate_results_json.sh  # JSON generation script
```

## Environment Variables

The Makefile supports several environment variables that can be set:

- `DATASET` - Specific dataset to process
- `NOTEBOOK_TYPE` - Type of notebook (schema, pydantic, namespace, or all)
- `PYTHON` - Python executable (default: python)
- `PIP` - Pip executable (default: pip)
- `GITHUB_OUTPUT` - Path for GitHub Actions output (default: /dev/null)
- `GITHUB_STEP_SUMMARY` - Path for GitHub Actions summary (default: /dev/null)

## Local Development

### Process a Single Dataset

```bash
# Generate and execute schema notebook
make schema DATASET=aopwikirdf

# This will:
# 1. Generate the notebook from template
# 2. Execute it with jupyter nbconvert
# 3. Convert to HTML
# 4. Place results in docs/
```

### Test Quickly

```bash
# Test with the first dataset in sources.csv
make test-one

# Or specify a dataset
make test-one DATASET=aopwikirdf
```

### Full Pipeline

```bash
# Run everything (this will take a while!)
make all
```

## GitHub Actions Integration

The Makefile is designed to work seamlessly with GitHub Actions. The workflow can use the same targets:

```yaml
- name: Generate schema notebook
  run: make schema DATASET=${{ matrix.dataset }}
  env:
    GITHUB_OUTPUT: ${{ github.output }}
    GITHUB_STEP_SUMMARY: ${{ github.step_summary }}
```

The Makefile automatically detects if it's running in CI (via `GITHUB_OUTPUT` environment variable) and adjusts behavior accordingly.

## Parallel Processing

For parallel processing locally, you can use GNU Parallel or xargs:

```bash
# Using xargs (available on most systems)
tail -n +2 data/sources.csv | cut -d',' -f1 | \
  xargs -P 4 -I {} make schema DATASET={}

# Using GNU Parallel (more advanced)
tail -n +2 data/sources.csv | cut -d',' -f1 | \
  parallel -j 4 make schema DATASET={}
```

## Troubleshooting

### Notebook Execution Fails

If a notebook fails to execute:

1. Check the error output in the terminal
2. Look for the generated `.ipynb` file in `notebooks/01_schema_extraction/`
3. Try executing it manually:
   ```bash
   cd notebooks/01_schema_extraction
   jupyter nbconvert --execute --to html <dataset>_schema.ipynb
   ```

### Dependencies Missing

```bash
# Reinstall dependencies
make install-deps

# On Ubuntu/Debian, install system deps
make install-system-deps
```

### Clean Start

```bash
# Remove all generated files
make clean-all

# Reinstall and run
make install-deps
make all
```

## Advanced Usage

### Custom Python Environment

```bash
# Use a specific Python interpreter
make all PYTHON=/path/to/python3

# Or with a virtual environment activated
source venv/bin/activate
make all
```

### Generate Only Specific Types

```bash
# Schema notebooks only
NOTEBOOK_TYPE=schema make schema-all

# Pydantic notebooks only
NOTEBOOK_TYPE=pydantic make pydantic-all

# Namespace notebooks only
NOTEBOOK_TYPE=namespace make namespace-all
```

## Performance Tips

1. **Use parallel processing** for bulk operations (see above)
2. **Process incrementally** - generate notebooks for new datasets only
3. **Use `test-one`** to verify setup before running `all`
4. **Monitor resources** - some notebooks are memory-intensive

## Contributing

When adding new features to the notebook generation pipeline:

1. Update the Makefile targets as needed
2. Test both local execution and CI integration
3. Update this README with new usage patterns
4. Ensure backward compatibility with existing workflows
