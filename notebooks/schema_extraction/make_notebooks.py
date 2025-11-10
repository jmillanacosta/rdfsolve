#!/usr/bin/env python3
"""
Generate RDFSolve notebooks from sources.csv using _schema_template.ipynb template.
Creates {dataset_name}_schema.ipynb for each dataset.
"""

import csv
import sys
import argparse
from pathlib import Path
import os


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Generate RDFSolve notebooks from sources.csv template'
    )
    parser.add_argument(
        '--dataset', '-d', 
        help='Generate notebook for specific dataset name only'
    )
    parser.add_argument(
        '--list', '-l', 
        action='store_true',
        help='List available dataset names from sources.csv'
    )
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    template_file = script_dir / "_schema_template.ipynb"
    sources_file = Path(os.path.abspath(
        os.path.join(script_dir, "..", "sources.csv")
    ))
    
    # Check if template exists
    if not template_file.exists():
        print(f"Error: Template file {template_file} not found")
        sys.exit(1)
    
    # Check if sources file exists
    if not sources_file.exists():
        print(f"Error: Sources file {sources_file} not found")
        sys.exit(1)
    
    # Read CSV file to get dataset names
    datasets = []
    with open(sources_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dataset_name = row['dataset_name'].strip().rstrip('/')
            if dataset_name:
                datasets.append({
                    'dataset_name': dataset_name,
                    'void_iri': row['void_iri'].strip(),
                    'graph_uri': row['graph_uri'].strip(),
                    'endpoint_url': row['endpoint_url'].strip()
                })
    
    # Handle --list option
    if args.list:
        print("Available datasets in sources.csv:")
        for i, dataset in enumerate(datasets, 1):
            print(f"{i:2d}. {dataset['dataset_name']}")
        return
    
    # Filter datasets if specific dataset requested
    if args.dataset:
        filtered_datasets = [d for d in datasets 
                           if d['dataset_name'] == args.dataset]
        if not filtered_datasets:
            print(f"Error: Dataset '{args.dataset}' not found in sources.csv")
            print("Available datasets:")
            for dataset in datasets:
                print(f"  - {dataset['dataset_name']}")
            sys.exit(1)
        datasets = filtered_datasets
        print(f"Generating notebook for dataset: {args.dataset}")
    else:
        print("Generating RDFSolve notebooks from template...")
    
    print(f"Template: {template_file}")
    print(f"Sources: {sources_file}")
    print()
    
    # Read template
    with open(template_file, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    count = 0
    
    # Process datasets
    for dataset_info in datasets:
        # Extract dataset information
        dataset_name = dataset_info['dataset_name']
        void_iri = dataset_info['void_iri']
        graph_uri = dataset_info['graph_uri']
        endpoint_url = dataset_info['endpoint_url']
        
        # Output filename
        output_file = script_dir / f"{dataset_name}_schema.ipynb"
        
        print(f"Generating: {dataset_name}_schema.ipynb")
        print(f"   Dataset: {dataset_name}")
        print(f"   VoID IRI: {void_iri}")
        print(f"   Graph URI: {graph_uri}")
        print(f"   Endpoint: {endpoint_url}")
        
        # Create notebook content by replacing placeholders
        content = template_content
        
        # Replace template placeholders
        content = content.replace('{{endpoint_url}}', endpoint_url)
        content = content.replace('{{dataset_name}}', dataset_name)
        content = content.replace('{{void_iri}}', void_iri)
        content = content.replace('{{graph_uri}}', graph_uri)
        
        # Replace text descriptions
        content = content.replace(
            "{{dataset_title}}",
            f"{dataset_name} Dataset Configuration"
        )
        # Dataset-specific analysis will be generic for all datasets
        
        # Write the new notebook
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"\tCreated: {output_file}")
        print()
        
        count += 1
    
    print(f"Generated {count} notebooks")
    print(f"All notebooks saved in: {script_dir}/")
    print()
    print("To run a specific notebook:")
    print("   jupyter notebook {dataset_name}_schema.ipynb")
    print()
    print("To run all notebooks programmatically:")
    print("   for nb in *_schema.ipynb; do jupyter nbconvert --execute "
          "--to notebook $nb; done")


if __name__ == "__main__":
    main()
