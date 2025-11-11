# Schema Notebooks Workflows

This directory contains two GitHub Actions workflows for generating schema analysis notebooks and HTML reports:

## Workflows Overview

### 1. Sequential Workflow (`generate-schema-notebooks.yml`)
- **Purpose**: Process datasets sequentially in a single job
- **Best for**: Small numbers of datasets or when you need to process a specific dataset
- **Features**:
  - Manual trigger with dataset selection
  - Single-job processing
  - Good for debugging individual datasets
  - Less resource intensive

### 2. Matrix Workflow (`generate-schema-notebooks-matrix.yml`)  
- **Purpose**: Process all datasets in parallel using GitHub's matrix strategy
- **Best for**: Processing all 48+ datasets efficiently
- **Features**:
  - Parallel processing (up to 20 concurrent jobs)
  - Automatic dataset detection from sources.csv
  - Faster completion for large batch processing
  - Individual job retry logic

## Usage

### Running All Datasets (Recommended)
Use the matrix workflow for processing all datasets:

```bash
# Trigger via GitHub UI (Actions tab) or git push to main branch
# Matrix workflow will automatically process all datasets in sources.csv
```

### Running Specific Dataset
Use the sequential workflow for single dataset processing:

1. Go to GitHub Actions tab
2. Select "Generate Schema Notebooks" workflow  
3. Click "Run workflow"
4. Enter specific dataset name (e.g., "chebi", "drugbank.drugs")

### Automatic Triggers
Both workflows automatically trigger on:
- Push to main branch (when relevant files change)
- Pull requests to main branch
- Changes to `sources.csv`, template, or workflow files

## Output Structure

Generated files are organized as:

```
docs/notebooks/schema_extraction/
├── index.html                    # Main index of all analyses
├── chebi_schema.html             # Individual dataset reports
├── drugbank.drugs_schema.html    
├── pubchem.compound_schema.html
└── ...                          # One HTML file per dataset
```

## Workflow Details

### Matrix Strategy
- Reads `sources.csv` to generate dynamic job matrix
- Each dataset runs in parallel (up to 20 concurrent jobs)
- Individual failure handling (one dataset failure won't stop others)
- Consolidated HTML index creation

### Safety Features
- **Concurrency Control**: Prevents conflicting workflow runs
- **Retry Logic**: Automatic retry on git push conflicts
- **Timeout Handling**: 20-minute timeout per notebook execution
- **Error Handling**: Failed notebooks still generate informative HTML
- **Artifact Backup**: HTML files uploaded as GitHub artifacts

### Git Operations
- Automatic rebase on conflicts
- Smart commit detection (only commit if changes exist)
- Proper user attribution for automated commits
- Branch protection friendly (works with required checks)

## Troubleshooting

### Common Issues

1. **JSON Format Error**: Matrix generation failure
   - **Solution**: Check `sources.csv` format and jq command
   
2. **Git Push Conflicts**: Multiple workflows running
   - **Solution**: Workflows have retry logic and concurrency controls
   
3. **Notebook Execution Timeout**: Large datasets
   - **Solution**: Increase timeout or exclude problematic datasets

4. **Missing Dependencies**: Package installation failures
   - **Solution**: Check Python dependencies in workflow setup

### Monitoring Progress

- Check Actions tab for real-time progress
- Download HTML artifacts if git push fails
- Review workflow summary for success/failure counts
- Individual job logs available for debugging

## Configuration

### Environment Variables
- No special environment variables required
- Uses `GITHUB_TOKEN` for repository operations

### Customization
- Modify timeout values in workflow files
- Adjust matrix strategy (batch size, concurrency)
- Update HTML styling in index generation
- Add/remove datasets in `sources.csv`

### Performance Tuning
- Matrix workflow: Faster for many datasets
- Sequential workflow: More reliable for single datasets
- Consider splitting very large datasets across multiple workflows