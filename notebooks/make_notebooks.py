#!/usr/bin/env python3
"""
Generate RDFSolve notebooks from sources.csv using templates.
Creates {dataset_name}_schema.ipynb and {dataset_name}_pydantic.ipynb for each dataset.
"""

import argparse
import csv
import os
import sys
from pathlib import Path


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate RDFSolve notebooks from sources.csv templates"
    )
    parser.add_argument("--dataset", "-d", help="Generate notebook for specific dataset name only")
    parser.add_argument(
        "--list", "-l", action="store_true", help="List available dataset names from sources.csv"
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["schema", "pydantic", "namespace", "all"],
        default="all",
        help="Type of notebook to generate (default: all)",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    schema_template_file = script_dir / "01_schema_extraction" / "_schema_template.ipynb"
    pydantic_template_file = script_dir / "02_pydantic_models" / "_pydantic_template.ipynb"
    namespace_template_file = script_dir / "03_bioregistry_namespaces" / "_namespace_template.ipynb"
    sources_file = Path(os.path.abspath(os.path.join(script_dir, "..", "data", "sources.csv")))

    # Check if templates exist
    templates_to_check = []
    if args.type in ["schema", "all"]:
        templates_to_check.append(("schema", schema_template_file))
    if args.type in ["pydantic", "all"]:
        templates_to_check.append(("pydantic", pydantic_template_file))
    if args.type in ["namespace", "all"]:
        templates_to_check.append(("namespace", namespace_template_file))

    for template_type, template_path in templates_to_check:
        if not template_path.exists():
            sys.exit(1)

    # Check if sources file exists
    if not sources_file.exists():
        sys.exit(1)

    # Read CSV file to get dataset names
    datasets = []
    with open(sources_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dataset_name = row["dataset_name"].strip().rstrip("/")
            if dataset_name:
                # Parse use_graph flag (handle string boolean values)
                use_graph_val = str(row.get("use_graph", "False")).strip()
                use_graph = use_graph_val.lower() in ["true", "1", "yes"]

                datasets.append(
                    {
                        "dataset_name": dataset_name,
                        "void_iri": (row["void_iri"] or "").strip(),
                        "graph_uri": (row["graph_uri"] or "").strip(),
                        "endpoint_url": (row["endpoint_url"] or "").strip(),
                        "use_graph": use_graph,
                    }
                )

    # Handle --list option
    if args.list:
        for _i, _dataset in enumerate(datasets, 1):
            pass
        return

    # Filter datasets if specific dataset requested
    if args.dataset:
        filtered_datasets = [d for d in datasets if d["dataset_name"] == args.dataset]
        if not filtered_datasets:
            for _dataset in datasets:
                pass
            sys.exit(1)
        datasets = filtered_datasets
    else:
        pass

    for template_type, template_path in templates_to_check:
        pass

    count = 0

    # Process datasets
    for dataset_info in datasets:
        # Extract dataset information
        dataset_name = dataset_info["dataset_name"]
        void_iri = dataset_info["void_iri"]
        graph_uri = dataset_info["graph_uri"]
        endpoint_url = dataset_info["endpoint_url"]
        use_graph = dataset_info["use_graph"]

        # Process each template type
        for template_type, template_path in templates_to_check:
            # Read template content
            with open(template_path, encoding="utf-8") as f:
                template_content = f.read()

            # Determine output file and directory
            if template_type == "schema":
                output_dir = script_dir / "01_schema_extraction"
                output_file = output_dir / f"{dataset_name}_schema.ipynb"
            elif template_type == "pydantic":
                output_dir = script_dir / "02_pydantic_models"
                output_file = output_dir / f"{dataset_name}_pydantic.ipynb"
            else:  # namespace
                output_dir = script_dir / "03_bioregistry_namespaces"
                output_file = output_dir / f"{dataset_name}_namespaces.ipynb"

            # Create directory if it doesn't exist
            output_dir.mkdir(exist_ok=True)

            # Create notebook content by replacing placeholders
            content = template_content

            # Replace template placeholders
            content = content.replace("{{endpoint_url}}", endpoint_url)
            content = content.replace("{{dataset_name}}", dataset_name)
            content = content.replace("{{void_iri}}", void_iri)
            content = content.replace("{{graph_uri}}", graph_uri)
            
            # Add ttl_db_path for namespace templates
            ttl_db_path = f"../../docs/data/schema_extraction/{dataset_name}/{dataset_name}_generated_void.ttl"
            content = content.replace("{{ttl_db_path}}", ttl_db_path)

            # Conditionally add graph_uri parameter based on use_graph flag
            if use_graph:
                graph_param = "graph_uri=graph_uri,"
            else:
                graph_param = ""
            content = content.replace("{{graph_uri_param}}", graph_param)

            # Replace text descriptions
            content = content.replace("{{dataset_title}}", f"{dataset_name} Dataset Configuration")

            # Write the new notebook
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(content)

        count += 1


if __name__ == "__main__":
    main()
