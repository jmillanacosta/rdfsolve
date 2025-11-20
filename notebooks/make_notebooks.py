#!/usr/bin/env python3
"""
Generate RDFSolve notebooks from sources.csv using templates.
Creates {dataset_name}_schema.ipynb and {dataset_name}_pydantic.ipynb for each dataset.
"""

import csv
import sys
import argparse
from pathlib import Path
import os


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Generate RDFSolve notebooks from sources.csv templates'
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
    parser.add_argument(
        '--type', '-t', 
        choices=['schema', 'pydantic', 'all'],
        default='all',
        help='Type of notebook to generate (default: all)'
    )
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    schema_template_file = script_dir / "01_schema_extraction" / "_schema_template.ipynb"
    pydantic_template_file = script_dir / "02_pydantic_models" / "_pydantic_template.ipynb"
    sources_file = Path(os.path.abspath(
        os.path.join(script_dir, "..", "data", "sources.csv")
    ))
    
    # Check if templates exist
    templates_to_check = []
    if args.type in ['schema', 'all']:
        templates_to_check.append(('schema', schema_template_file))
    if args.type in ['pydantic', 'all']:
        templates_to_check.append(('pydantic', pydantic_template_file))
    
    for template_type, template_path in templates_to_check:
        if not template_path.exists():
            print(f"Error: {template_type.title()} template file {template_path} not found")
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
                # Parse use_graph flag (handle string boolean values)
                use_graph_val = str(row.get('use_graph', 'False')).strip()
                use_graph = use_graph_val.lower() in ['true', '1', 'yes']
                
                datasets.append({
                    'dataset_name': dataset_name,
                    'void_iri': (row['void_iri'] or '').strip(),
                    'graph_uri': (row['graph_uri'] or '').strip(),
                    'endpoint_url': (row['endpoint_url'] or '').strip(),
                    'use_graph': use_graph
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
        print(f"Generating RDFSolve notebooks ({args.type}) from templates...")
    
    for template_type, template_path in templates_to_check:
        print(f"{template_type.title()} template: {template_path}")
    print(f"Sources: {sources_file}")
    print()
    
    count = 0
    
    # Process datasets
    for dataset_info in datasets:
        # Extract dataset information
        dataset_name = dataset_info['dataset_name']
        void_iri = dataset_info['void_iri']
        graph_uri = dataset_info['graph_uri']
        endpoint_url = dataset_info['endpoint_url']
        use_graph = dataset_info['use_graph']
        
        print(f"Generating notebooks for: {dataset_name}")
        print(f"   Dataset: {dataset_name}")
        print(f"   VoID IRI: {void_iri}")
        print(f"   Graph URI: {graph_uri}")
        print(f"   Endpoint: {endpoint_url}")
        print(f"   Use Graph: {use_graph}")
        
        # Process each template type
        for template_type, template_path in templates_to_check:
            # Read template content
            with open(template_path, 'r', encoding='utf-8') as f:
                template_content = f.read()
            
            # Determine output file and directory
            if template_type == 'schema':
                output_dir = script_dir / "01_schema_extraction"
                output_file = output_dir / f"{dataset_name}_schema.ipynb"
            else:  # pydantic
                output_dir = script_dir / "02_pydantic_models"
                output_file = output_dir / f"{dataset_name}_pydantic.ipynb"
            
            # Create directory if it doesn't exist
            output_dir.mkdir(exist_ok=True)
            
            # Create notebook content by replacing placeholders
            content = template_content
            
            # Replace template placeholders
            content = content.replace('{{endpoint_url}}', endpoint_url)
            content = content.replace('{{dataset_name}}', dataset_name)
            content = content.replace('{{void_iri}}', void_iri)
            content = content.replace('{{graph_uri}}', graph_uri)
            
            # Conditionally add graph_uri parameter based on use_graph flag
            if use_graph:
                graph_param = 'graph_uri=graph_uri,'
            else:
                graph_param = ''
            content = content.replace('{{graph_uri_param}}', graph_param)
            
            # Replace text descriptions
            content = content.replace(
                "{{dataset_title}}",
                f"{dataset_name} Dataset Configuration"
            )
            
            # Write the new notebook
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"   Created: {output_file}")
        
        print()
        count += 1
    
    print(f"Generated {count} datasets with {args.type} notebooks")
    print(f"Schema notebooks saved in: {script_dir}/01_schema_extraction/")
    print(f"Pydantic notebooks saved in: {script_dir}/02_pydantic_models/")
    print()
    print("To run notebooks:")
    print("   cd 01_schema_extraction && jupyter notebook {dataset_name}_schema.ipynb")
    print("   cd 02_pydantic_models && jupyter notebook {dataset_name}_pydantic.ipynb")
    print()
    print("To run all schema notebooks programmatically:")
    print("   cd 01_schema_extraction && for nb in *_schema.ipynb; do jupyter nbconvert --execute --to notebook $nb; done")
    print("To run all pydantic notebooks programmatically:")
    print("   cd 02_pydantic_models && for nb in *_pydantic.ipynb; do jupyter nbconvert --execute --to notebook $nb; done")


if __name__ == "__main__":
    main()
