# Schema Analysis HTML Reports

This directory contains the generated HTML reports from the automated schema analysis workflows.

Files in this directory:
- `index.html` - Navigation page listing all schema analysis reports
- `*_schema.html` - Individual dataset schema analysis reports  
- `error_report.html` - Error report (if all workflows fail)

## Viewing Reports

The reports can be viewed directly in GitHub or served via GitHub Pages if enabled for this repository.

## Automated Generation

These files are automatically generated and updated by GitHub Actions workflows:
- Sequential workflow: `generate-schema-notebooks.yml`
- Matrix workflow: `generate-schema-notebooks-matrix.yml`

Do not edit these files manually as they will be overwritten by the next workflow run.