#!/bin/bash
# Script to generate results.json from notebook outputs

set -e

DOCS_DIR="docs"
SCHEMA_DOCS="$DOCS_DIR/notebooks/01_schema_extraction"
PYDANTIC_DOCS="$DOCS_DIR/notebooks/02_pydantic_models"
NAMESPACE_DOCS="$DOCS_DIR/notebooks/03_bioregistry_namespaces"
DATA_DIR="$DOCS_DIR/data/schema_extraction"

# Count results
total_schema_files=$(find "$SCHEMA_DOCS" -name "*_schema.html" 2>/dev/null | wc -l)
total_pydantic_files=$(find "$PYDANTIC_DOCS" -name "*_pydantic.html" 2>/dev/null | wc -l)
total_namespace_files=$(find "$NAMESPACE_DOCS" -name "*_namespaces.html" 2>/dev/null | wc -l)

schema_success_count=$(find "$SCHEMA_DOCS" -name "*_schema.html" -exec grep -L "Schema Analysis Failed" {} \; 2>/dev/null | wc -l)
pydantic_success_count=$(find "$PYDANTIC_DOCS" -name "*_pydantic.html" -exec grep -L "Pydantic Generation Failed" {} \; 2>/dev/null | wc -l)
namespace_success_count=$(find "$NAMESPACE_DOCS" -name "*_namespaces.html" -exec grep -L "Namespace Discovery Failed" {} \; 2>/dev/null | wc -l)

schema_failed_count=$((total_schema_files - schema_success_count))
pydantic_failed_count=$((total_pydantic_files - pydantic_success_count))
namespace_failed_count=$((total_namespace_files - namespace_success_count))

data_files_count=$(find "$DATA_DIR" -type f \( -name "*.jsonld" -o -name "*.yaml" -o -name "*.csv" -o -name "*.ttl" -o -name "*.nq" -o -name "*.parquet" -o -name "*.json" -o -name "*.jsonl" \) 2>/dev/null | wc -l)

# Start JSON
cat > "$DOCS_DIR/results.json" << EOF
{
  "lastUpdated": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "stats": {
    "totalDatasets": $total_schema_files,
    "schemaSuccessful": $schema_success_count,
    "schemaFailed": $schema_failed_count,
    "pydanticSuccessful": $pydantic_success_count,
    "pydanticFailed": $pydantic_failed_count,
    "namespaceSuccessful": $namespace_success_count,
    "namespaceFailed": $namespace_failed_count,
    "totalDataFiles": $data_files_count
  },
  "datasets": [
EOF

# Process all datasets
first=true
for html_file in "$SCHEMA_DOCS"/*_schema.html; do
  if [ -f "$html_file" ]; then
    dataset_name=$(basename "$html_file" .html | sed 's/_schema$//')
    
    # Check schema status
    if grep -q "Schema Analysis Failed" "$html_file" 2>/dev/null; then
      schema_status="error"
    else
      schema_status="success"
    fi
    
    schema_file_size=$(ls -lh "$html_file" 2>/dev/null | awk '{print $5}' || echo "N/A")
    schema_report_url="notebooks/01_schema_extraction/$(basename "$html_file")"
    
    # Check pydantic status
    pydantic_html="$PYDANTIC_DOCS/${dataset_name}_pydantic.html"
    if [ -f "$pydantic_html" ]; then
      if grep -q "Pydantic Generation Failed" "$pydantic_html" 2>/dev/null; then
        pydantic_status="error"
      else
        pydantic_status="success"
      fi
      pydantic_file_size=$(ls -lh "$pydantic_html" 2>/dev/null | awk '{print $5}' || echo "N/A")
      pydantic_report_url="notebooks/02_pydantic_models/${dataset_name}_pydantic.html"
    else
      pydantic_status="missing"
      pydantic_file_size="N/A"
      pydantic_report_url=""
    fi
    
    # Check namespace status
    namespace_html="$NAMESPACE_DOCS/${dataset_name}_namespaces.html"
    if [ -f "$namespace_html" ]; then
      if grep -q "Namespace Discovery Failed" "$namespace_html" 2>/dev/null; then
        namespace_status="error"
      else
        namespace_status="success"
      fi
      namespace_file_size=$(ls -lh "$namespace_html" 2>/dev/null | awk '{print $5}' || echo "N/A")
      namespace_report_url="notebooks/03_bioregistry_namespaces/${dataset_name}_namespaces.html"
    else
      namespace_status="missing"
      namespace_file_size="N/A"
      namespace_report_url=""
    fi
    
    # Add comma for all but first entry
    if [ "$first" = false ]; then
      echo "    ," >> "$DOCS_DIR/results.json"
    fi
    first=false
    
    # Start dataset object
    cat >> "$DOCS_DIR/results.json" << EOF
    {
      "name": "$dataset_name",
      "generated": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
      "notebooks": {
        "schema": {
          "status": "$schema_status",
          "reportSize": "$schema_file_size",
          "reportUrl": "$schema_report_url"
        },
        "pydantic": {
          "status": "$pydantic_status",
          "reportSize": "$pydantic_file_size",
          "reportUrl": "$pydantic_report_url"
        },
        "namespace": {
          "status": "$namespace_status",
          "reportSize": "$namespace_file_size",
          "reportUrl": "$namespace_report_url"
        }
      },
      "dataFiles": {
EOF
    
    # Check for data files
    data_first=true
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_schema.jsonld" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"jsonld\": \"../data/schema_extraction/$dataset_name/${dataset_name}_schema.jsonld\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_linkml_schema.yaml" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"linkml\": \"../data/schema_extraction/$dataset_name/${dataset_name}_linkml_schema.yaml\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_pattern_coverage.csv" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"coverage\": \"../data/schema_extraction/$dataset_name/${dataset_name}_pattern_coverage.csv\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_generated_void.ttl" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"void\": \"../data/schema_extraction/$dataset_name/${dataset_name}_generated_void.ttl\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_instances.parquet" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"instances\": \"../data/schema_extraction/$dataset_name/${dataset_name}_instances.parquet\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_schema.nq" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"nquads\": \"../data/schema_extraction/$dataset_name/${dataset_name}_schema.nq\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_sparql_queries.ttl" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"queries\": \"../data/schema_extraction/$dataset_name/${dataset_name}_sparql_queries.ttl\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_instances_minimal.jsonl" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"instancesJsonl\": \"../data/schema_extraction/$dataset_name/${dataset_name}_instances_minimal.jsonl\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_instances_subject_index.json" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"subjectIndex\": \"../data/schema_extraction/$dataset_name/${dataset_name}_instances_subject_index.json\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    if [ -f "$DATA_DIR/$dataset_name/${dataset_name}_instances_object_index.json" ]; then
      if [ "$data_first" = false ]; then echo "        ," >> "$DOCS_DIR/results.json"; fi
      echo "        \"objectIndex\": \"../data/schema_extraction/$dataset_name/${dataset_name}_instances_object_index.json\"" >> "$DOCS_DIR/results.json"
      data_first=false
    fi
    
    # Close dataFiles and dataset object
    echo "      }" >> "$DOCS_DIR/results.json"
    echo "    }" >> "$DOCS_DIR/results.json"
  fi
done

# Close JSON
cat >> "$DOCS_DIR/results.json" << EOF
  ]
}
EOF

echo "Generated $DOCS_DIR/results.json"
