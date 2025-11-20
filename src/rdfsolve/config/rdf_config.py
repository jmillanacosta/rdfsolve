"""
RDF Configuration Parser

This module provides functionality to parse RDF-config YAML models and generate
schema graphs. Handling blank nodes, URI resolution, and model processing.

"""

import os
import re
import yaml
import logging
from typing import Dict, List, Any, Optional
from ..utils import resolve_curie, normalize_uri, clean_predicate, is_blank_node


# Custom YAML constructor to handle blank node keys ([] as keys)
class BlankNodeSafeLoader(yaml.SafeLoader):
    pass


def blank_node_constructor(loader, node):
    """Handle blank node represented as [] key"""
    return "[]"


# Register the constructor for list keys
BlankNodeSafeLoader.add_constructor(
    "tag:yaml.org,2002:python/tuple", blank_node_constructor
)


# Also handle flow sequences as blank nodes
def flow_sequence_constructor(loader, node):
    """Handle [] in flow style as blank node key"""
    if isinstance(node, yaml.SequenceNode):
        if not node.value:  # Empty list
            return "[]"
        return loader.construct_sequence(node)
    return blank_node_constructor(loader, node)


# Default configuration directories
DEFAULT_CONFIG_DIRS = [
    "pdb",
    "pubchem",
    "uniprot",
    "bgee",
]
DEFAULT_CONFIG_BASE_DIR = "config"


class RDFConfigParser:
    """Parser for RDF-config YAML models following Ruby RDFConfig logic"""

    def __init__(self, config_dir: str):
        self.config_dir = config_dir
        self.prefixes = {}
        self.blank_node_counter = 0
        self.blank_node_map = {}  # Track blank nodes by their position

    def load_prefixes(self) -> Dict[str, str]:
        """Load prefix mappings from prefix.yaml"""
        prefix_path = os.path.join(self.config_dir, "prefix.yaml")
        if not os.path.exists(prefix_path):
            return {}

        with open(prefix_path, "r") as f:
            prefixes = yaml.safe_load(f) or {}

        self.prefixes = prefixes
        return prefixes

    def load_model(self) -> List[Dict]:
        """Load model from model.yaml, handling blank node keys"""
        model_path = os.path.join(self.config_dir, "model.yaml")
        if not os.path.exists(model_path):
            return []

        try:
            with open(model_path, "r") as f:
                raw_content = f.read()

            # Pre-process: Replace [] keys with special marker before YAML parsing
            # This handles the Ruby RDFConfig convention of using [] as dictionary keys
            processed_content = self._preprocess_blank_node_keys(raw_content)

            model_data = yaml.safe_load(processed_content) or []

            # Post-process: Convert marker back to blank node identifier
            model_data = self._postprocess_blank_nodes(model_data)

        except yaml.YAMLError as e:
            logger = logging.getLogger(__name__)
            logger.error("YAML Error loading %s: %s", model_path, e)
            return []

        return model_data if isinstance(model_data, list) else [model_data]

    def _preprocess_blank_node_keys(self, content: str) -> str:
        """Replace [] keys with __BLANK_NODE__ marker for YAML parsing"""
        import re

        # Match patterns like "- []:" or "  []:" (blank node as key)
        # Replace with a parseable string key
        content = re.sub(
            r"^(\s+)- \[\]:$", r"\1- __BLANK_NODE__:", content, flags=re.MULTILINE
        )
        content = re.sub(
            r"^(\s+)\[\]:$", r"\1__BLANK_NODE__:", content, flags=re.MULTILINE
        )
        return content

    def _postprocess_blank_nodes(self, data: Any) -> Any:
        """Recursively convert __BLANK_NODE__ markers back to '[]' identifier"""
        if isinstance(data, dict):
            new_dict = {}
            for key, value in data.items():
                # Convert marker back to blank node identifier
                new_key = "[]" if key == "__BLANK_NODE__" else key
                new_dict[new_key] = self._postprocess_blank_nodes(value)
            return new_dict
        elif isinstance(data, list):
            return [self._postprocess_blank_nodes(item) for item in data]
        else:
            return data

    def get_blank_node_id(self, context: str = "") -> str:
        """Generate unique blank node ID"""
        self.blank_node_counter += 1
        return f"_:bnode_{self.blank_node_counter}_{context}"

    def resolve_curie(self, curie: str) -> Optional[str]:
        """Convert CURIE to full IRI"""
        return resolve_curie(curie, self.prefixes)

    def clean_predicate(self, predicate: str) -> str:
        """Remove cardinality markers from predicate"""
        return clean_predicate(predicate)

    def is_blank_node(self, value: Any) -> bool:
        """Check if value represents a blank node"""
        return is_blank_node(value)

    def extract_rdf_types(self, properties_list: List[Dict]) -> List[str]:
        """Extract rdf:type URIs from properties"""
        rdf_types = []

        for prop_dict in properties_list:
            if not isinstance(prop_dict, dict):
                continue

            for predicate, object_list in prop_dict.items():
                if predicate.strip() != "a":
                    continue

                if not isinstance(object_list, list):
                    object_list = [object_list]

                for obj in object_list:
                    obj_value = None
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if isinstance(v, str) and ":" in v:
                                obj_value = v
                                break
                    elif isinstance(obj, str):
                        obj_value = obj

                    if obj_value:
                        type_uri = self.resolve_curie(obj_value)
                        if type_uri:
                            rdf_types.append(type_uri)

        return rdf_types

    def process_object(
        self,
        obj: Any,
        subject_name: str,
        predicate_uri: str,
        schema: Dict,
        subject_name_map: Dict,
    ) -> Optional[str]:
        """Process an object and return its URI"""
        # Handle blank nodes
        if self.is_blank_node(obj):
            bnode_id = self.get_blank_node_id(f"{subject_name}_{predicate_uri}")

            # If blank node has nested properties, process them
            if isinstance(obj, dict):
                if "[]" in obj and isinstance(obj["[]"], list):
                    # Process blank node properties
                    self.process_properties(
                        bnode_id, obj["[]"], schema, subject_name_map
                    )
                elif len(obj) == 1:
                    key = list(obj.keys())[0]
                    if isinstance(key, list) or (
                        isinstance(obj[key], list) and obj[key]
                    ):
                        # Nested structure
                        nested_props = (
                            obj[key] if isinstance(obj[key], list) else [obj[key]]
                        )
                        self.process_properties(
                            bnode_id, nested_props, schema, subject_name_map
                        )
            elif isinstance(obj, list) and len(obj) == 1 and isinstance(obj[0], dict):
                if "[]" in obj[0]:
                    self.process_properties(
                        bnode_id, obj[0]["[]"], schema, subject_name_map
                    )

            return bnode_id

        # Handle dict with name => value
        if isinstance(obj, dict):
            for obj_name, obj_value in obj.items():
                if isinstance(obj_value, str):
                    # Check if it's a subject reference
                    if obj_value in subject_name_map:
                        return subject_name_map[obj_value]
                    # Try to resolve as URI
                    if ":" in obj_value or obj_value.startswith("http"):
                        obj_uri = self.resolve_curie(obj_value)
                        if obj_uri:
                            return obj_uri

        # Handle direct string value
        elif isinstance(obj, str):
            # Check if it's a subject reference
            if obj in subject_name_map:
                return subject_name_map[obj]
            # Try to resolve as URI
            if ":" in obj or obj.startswith("http"):
                obj_uri = self.resolve_curie(obj)
                if obj_uri:
                    return obj_uri

        return None

    def process_properties(
        self,
        subject_uri: str,
        properties_list: List[Dict],
        schema: Dict,
        subject_name_map: Dict,
    ):
        """Process properties for a subject"""
        if subject_uri not in schema:
            schema[subject_uri] = {}

        for prop_dict in properties_list:
            if not isinstance(prop_dict, dict):
                continue

            for predicate, object_list in prop_dict.items():
                # Clean and resolve predicate
                clean_pred = self.clean_predicate(predicate)
                predicate_uri = self.resolve_curie(clean_pred)

                if not predicate_uri:
                    continue

                if predicate_uri not in schema[subject_uri]:
                    schema[subject_uri][predicate_uri] = []

                # Process objects
                if not isinstance(object_list, list):
                    object_list = [object_list]

                for obj in object_list:
                    obj_uri = self.process_object(
                        obj, subject_uri, predicate_uri, schema, subject_name_map
                    )
                    if obj_uri and obj_uri not in schema[subject_uri][predicate_uri]:
                        schema[subject_uri][predicate_uri].append(obj_uri)

    def parse_to_schema(self) -> Dict[str, Dict[str, List[str]]]:
        """Parse model.yaml to schema graph structure"""
        self.load_prefixes()
        model_data = self.load_model()

        if not model_data:
            return {}

        schema = {}
        subject_name_map = {}  # Map subject names to their rdf:type URIs

        # First pass: collect all subjects and their types
        for subject_hash in model_data:
            if not isinstance(subject_hash, dict):
                continue

            subject_spec = list(subject_hash.keys())[0]
            properties_list = subject_hash[subject_spec]

            if not isinstance(properties_list, list):
                continue

            # Parse subject name
            if " " in str(subject_spec):
                subject_name, _ = str(subject_spec).split(None, 1)
            else:
                subject_name = str(subject_spec)

            # Get rdf:types
            rdf_types = self.extract_rdf_types(properties_list)

            # Map subject name to its rdf:type URIs
            for rdf_type_uri in rdf_types:
                subject_name_map[subject_name] = rdf_type_uri

        # Second pass: process all properties
        for subject_hash in model_data:
            if not isinstance(subject_hash, dict):
                continue

            subject_spec = list(subject_hash.keys())[0]
            properties_list = subject_hash[subject_spec]

            if not isinstance(properties_list, list):
                continue

            # Parse subject name
            if " " in str(subject_spec):
                subject_name, _ = str(subject_spec).split(None, 1)
            else:
                subject_name = str(subject_spec)

            # Get rdf:types
            rdf_types = self.extract_rdf_types(properties_list)

            # Process properties for each rdf:type
            for subject_uri in rdf_types:
                self.process_properties(
                    subject_uri, properties_list, schema, subject_name_map
                )

        return schema


def normalize_uri(uri_string, prefixes, source, remove_qualifiers=True):
    """Normalize URI strings to full URIs"""
    if not uri_string or uri_string in ["BN", "null", ""]:
        return None

    uri_string = str(uri_string).strip()

    # Remove qualifiers (* and ?) if flag is set
    if remove_qualifiers:
        uri_string = uri_string.rstrip("*?")

    # Already a full URI
    if uri_string.startswith("<") and uri_string.endswith(">"):
        return uri_string
    if uri_string.startswith("http"):
        return f"<{uri_string}>"

    # Handle prefix:suffix format
    if ":" in uri_string and not uri_string.startswith("http"):
        prefix, suffix = uri_string.split(":", 1)
        if prefix in prefixes:
            base_uri = prefixes[prefix].rstrip(">")
            if base_uri.startswith("<"):
                base_uri = base_uri[1:]
            return f"<{base_uri}{suffix}>"

    # Handle relative URIs - assume they belong to the source namespace
    if source in prefixes:
        base_uri = prefixes[source].rstrip(">")
        if base_uri.startswith("<"):
            base_uri = base_uri[1:]
        return f"<{base_uri}{uri_string}>"

    return f"<{uri_string}>"


def is_example_or_metadata(key, value):
    """Identify and filter out examples, comments, and metadata"""
    if key and isinstance(key, str) and "http" in key:
        return False
    if value and isinstance(value, str) and "http" in value:
        return False

    return False


def clean_and_normalize_schema(source, remove_qualifiers=True):
    """Master data cleaning function for RDF schemas"""
    if not (
        os.path.exists(f"config/{source}/model.yaml")
        and os.path.exists(f"config/{source}/prefix.yaml")
    ):
        return {}

    # Load prefixes
    with open(f"config/{source}/prefix.yaml", "r") as f:
        prefixes = yaml.safe_load(f) or {}

    # Load model
    with open(f"config/{source}/model.yaml", "r") as f:
        raw_model = f.read()

    # Clean up common inconsistencies
    raw_model = raw_model.replace("- []", "- BN")
    raw_model = raw_model.replace("- a:", "- rdf:type:")

    # Fix malformed lists [item1, item2] -> proper YAML
    list_pattern = r"\[([^\[\]]*)\]"

    def fix_yaml_list(match):
        items = [
            item.strip().strip("\"'")
            for item in match.group(1).split(",")
            if item.strip()
        ]
        if not items:
            return "[]"
        return "\n" + "\n".join(f"    - {item}" for item in items)

    raw_model = re.sub(list_pattern, fix_yaml_list, raw_model, flags=re.MULTILINE)

    # Parse as YAML
    try:
        model_data = yaml.safe_load(raw_model)
    except yaml.YAMLError:
        return {}

    if not model_data:
        return {}

    # Normalize to consistent structure
    normalized_schema = {}

    def process_node(node_data):
        """Process a single node and its properties"""
        if not isinstance(node_data, dict):
            return {}

        node_properties = {}

        for key, value in node_data.items():
            # Skip examples and metadata
            if is_example_or_metadata(key, value):
                continue

            # Normalize the property URI
            property_uri = normalize_uri(key, prefixes, source, remove_qualifiers)
            if not property_uri:
                continue

            # Process the value(s)
            processed_values = process_values(
                value, prefixes, source, remove_qualifiers
            )
            if processed_values:
                node_properties[property_uri] = processed_values

        return node_properties

    def process_values(value, prefixes, source, remove_qualifiers=True):
        """Process property values (objects)"""
        if value is None:
            return None

        if isinstance(value, list):
            processed_list = []
            for item in value:
                if item is None:
                    continue

                if isinstance(item, dict):
                    # Nested property-value pairs
                    for sub_prop, sub_val in item.items():
                        if is_example_or_metadata(sub_prop, sub_val):
                            continue

                        sub_prop_uri = normalize_uri(
                            sub_prop, prefixes, source, remove_qualifiers
                        )
                        sub_val_processed = process_values(
                            sub_val, prefixes, source, remove_qualifiers
                        )

                        if sub_prop_uri and sub_val_processed:
                            processed_list.append({sub_prop_uri: sub_val_processed})
                else:
                    # Direct value
                    normalized_val = normalize_uri(
                        item, prefixes, source, remove_qualifiers
                    )
                    if normalized_val and not is_example_or_metadata("", item):
                        processed_list.append(normalized_val)

            return processed_list if processed_list else None

        elif isinstance(value, dict):
            # Single nested object
            processed_dict = {}
            for sub_prop, sub_val in value.items():
                if is_example_or_metadata(sub_prop, sub_val):
                    continue

                sub_prop_uri = normalize_uri(
                    sub_prop, prefixes, source, remove_qualifiers
                )
                sub_val_processed = process_values(
                    sub_val, prefixes, source, remove_qualifiers
                )

                if sub_prop_uri and sub_val_processed:
                    processed_dict[sub_prop_uri] = sub_val_processed

            return processed_dict if processed_dict else None

        else:
            # Simple value
            if is_example_or_metadata("", value):
                return None
            return normalize_uri(value, prefixes, source, remove_qualifiers)

    # Process the top-level model structure
    if isinstance(model_data, list):
        for item in model_data:
            if isinstance(item, dict):
                for subject, properties in item.items():
                    subject_uri = normalize_uri(
                        subject, prefixes, source, remove_qualifiers
                    )
                    if subject_uri:
                        processed_props = process_node({subject: properties})
                        if processed_props:
                            if subject_uri in normalized_schema:
                                # Merge properties
                                for prop, val in processed_props.items():
                                    if prop in normalized_schema[subject_uri]:
                                        # Combine values
                                        existing = normalized_schema[subject_uri][prop]
                                        if isinstance(existing, list) and isinstance(
                                            val, list
                                        ):
                                            normalized_schema[subject_uri][prop] = (
                                                existing + val
                                            )
                                        elif not isinstance(
                                            existing, list
                                        ) and isinstance(val, list):
                                            normalized_schema[subject_uri][prop] = [
                                                existing
                                            ] + val
                                        elif isinstance(
                                            existing, list
                                        ) and not isinstance(val, list):
                                            normalized_schema[subject_uri][prop] = (
                                                existing + [val]
                                            )
                                        else:
                                            normalized_schema[subject_uri][prop] = [
                                                existing,
                                                val,
                                            ]
                                    else:
                                        normalized_schema[subject_uri][prop] = val
                            else:
                                normalized_schema[subject_uri] = {
                                    subject_uri: processed_props.get(subject_uri, {})
                                }

    elif isinstance(model_data, dict):
        for subject, properties in model_data.items():
            subject_uri = normalize_uri(subject, prefixes, source)
            if subject_uri:
                processed_props = process_node(properties)
                if processed_props:
                    normalized_schema[subject_uri] = processed_props

    return normalized_schema


def convert_to_target_format(normalized_schemas):
    """Convert normalized schemas to clean format: <subject>: [<property>: <object>] with max depth 2"""
    final_schema = []

    for source_schemas in normalized_schemas.values():
        for subject_uri, properties in source_schemas.items():
            # Clean subject URI
            subject_uri = (
                subject_uri.split(" ")[0]
                if isinstance(subject_uri, str)
                else str(subject_uri)
            )
            if not subject_uri.startswith("<"):
                continue

            node_entry = {subject_uri: []}

            for prop_uri, values in properties.items():
                # Clean property URI
                prop_uri = (
                    prop_uri.split(" ")[0]
                    if isinstance(prop_uri, str)
                    else str(prop_uri)
                )
                if not prop_uri.startswith("<"):
                    continue

                # Process values to create clean property-object pairs
                if isinstance(values, list):
                    for value in values:
                        if isinstance(value, dict):
                            # Nested property-value pairs - flatten to direct property-object
                            for nested_prop, nested_obj in value.items():
                                # Clean nested property and object
                                clean_nested_prop = (
                                    nested_prop.split(" ")[0]
                                    if isinstance(nested_prop, str)
                                    and "http" in nested_prop
                                    else nested_prop
                                )

                                if isinstance(nested_obj, str) and "http" in nested_obj:
                                    clean_nested_obj = nested_obj.split(" ")[0]
                                    if clean_nested_obj.startswith(
                                        "<"
                                    ) and clean_nested_obj.endswith(">"):
                                        # Use the nested property as the actual property
                                        node_entry[subject_uri].append(
                                            {clean_nested_prop: clean_nested_obj}
                                        )
                                elif isinstance(nested_obj, list):
                                    # Handle list of objects
                                    for obj_item in nested_obj:
                                        if (
                                            isinstance(obj_item, str)
                                            and "http" in obj_item
                                        ):
                                            clean_obj = obj_item.split(" ")[0]
                                            if clean_obj.startswith(
                                                "<"
                                            ) and clean_obj.endswith(">"):
                                                node_entry[subject_uri].append(
                                                    {clean_nested_prop: clean_obj}
                                                )
                        else:
                            # Direct value - use original property
                            if isinstance(value, str) and "http" in value:
                                clean_obj = value.split(" ")[0]
                                if clean_obj.startswith("<") and clean_obj.endswith(
                                    ">"
                                ):
                                    node_entry[subject_uri].append(
                                        {prop_uri: clean_obj}
                                    )
                elif isinstance(values, dict):
                    # Single nested object
                    for nested_prop, nested_obj in values.items():
                        clean_nested_prop = (
                            nested_prop.split(" ")[0]
                            if isinstance(nested_prop, str) and "http" in nested_prop
                            else nested_prop
                        )
                        if isinstance(nested_obj, str) and "http" in nested_obj:
                            clean_nested_obj = nested_obj.split(" ")[0]
                            if clean_nested_obj.startswith(
                                "<"
                            ) and clean_nested_obj.endswith(">"):
                                node_entry[subject_uri].append(
                                    {clean_nested_prop: clean_nested_obj}
                                )
                else:
                    # Direct string value
                    if isinstance(values, str) and "http" in values:
                        clean_obj = values.split(" ")[0]
                        if clean_obj.startswith("<") and clean_obj.endswith(">"):
                            node_entry[subject_uri].append({prop_uri: clean_obj})

            if node_entry[subject_uri]:  # Only add if there are properties
                final_schema.append(node_entry)

    return final_schema


def process_multiple_sources(
    config_dirs: List[str] = None,
    config_base_dir: str = None,
    output_dir: str = ".",
    remove_qualifiers: bool = True,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Process multiple RDF-config sources and generate combined schema graphs.
    
    Args:
        config_dirs: List of config directory names to process
        config_base_dir: Base directory containing config folders
        output_dir: Directory to save output files
        remove_qualifiers: Whether to remove URI qualifiers (* and ?)
        verbose: Whether to print processing information
        
    Returns:
        Dictionary containing combined schema data
    """
    if config_dirs is None:
        config_dirs = DEFAULT_CONFIG_DIRS
    if config_base_dir is None:
        config_base_dir = DEFAULT_CONFIG_BASE_DIR
    
    # Process all schemas using both approaches
    all_normalized = {}
    combined_schema = {}
    source_schemas = {}
    
    logger = logging.getLogger(__name__)
    for source in config_dirs:
        if verbose:
            logger.info("Processing %s...", source)
            
        # Method 1: Clean and normalize
        normalized = clean_and_normalize_schema(
            source, remove_qualifiers=remove_qualifiers
        )
        if normalized:
            all_normalized[source] = normalized
            
        # Method 2: RDFConfigParser approach
        config_dir = os.path.join(config_base_dir, source)
        parser = RDFConfigParser(config_dir)
        schema = parser.parse_to_schema()
        
        if schema:
            if verbose:
                logger.info("  Found %s subject classes", len(schema))
                logger.info("  Generated %s blank nodes", parser.blank_node_counter)
            source_schemas[source] = schema
            
            # Merge into combined schema
            for subject, properties in schema.items():
                if subject not in combined_schema:
                    combined_schema[subject] = {}
                    
                for prop, objects in properties.items():
                    if prop not in combined_schema[subject]:
                        combined_schema[subject][prop] = []
                        
                    for obj in objects:
                        if obj not in combined_schema[subject][prop]:
                            combined_schema[subject][prop].append(obj)
        elif verbose:
            logger.info("  No schema found")
    
    # Convert normalized schemas to target format
    final_clean_schema = convert_to_target_format(all_normalized)
    
    # Construct comprehensive schema data
    schema_data = {
        "combined": combined_schema,
        "by_source": source_schemas,
        "clean_normalized": final_clean_schema,
        "summary": {
            "total_classes": len(combined_schema),
            "sources_processed": len([s for s in source_schemas if source_schemas[s]]),
            "classes_per_source": {s: len(source_schemas[s]) for s in source_schemas},
        },
    }
    
    # Save outputs
    if output_dir:
        # Save full schema with metadata
        output_file = os.path.join(output_dir, "schema_graphs_clean.yaml")
        with open(output_file, "w") as f:
            yaml.dump(
                schema_data,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        
        # Save schema entries only
        output_file_entries_only = os.path.join(output_dir, "schema_entries_only.yaml")
        with open(output_file_entries_only, "w") as f:
            yaml.dump(
                combined_schema,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        
        # Save cleaned normalized schema
        cleaned_output_file = os.path.join(output_dir, "cleaned_schema.yaml")
        with open(cleaned_output_file, "w") as f:
            yaml.dump(
                final_clean_schema,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        
        if verbose:
            logger.info("\nResults saved to:")
            logger.info("  - %s (with metadata)", output_file)
            logger.info("  - %s (entries only)", output_file_entries_only)
            logger.info("  - %s (cleaned normalized)", cleaned_output_file)
    
    return schema_data


def display_schema_sample(schema_data: Dict[str, Any], max_items: int = 3):
    """Display a sample of the schema graph structure."""
    logger = logging.getLogger(__name__)
    logger.info("\n%s", '=' * 60)
    logger.info("Summary:")
    logger.info("  Total classes: %s", schema_data['summary']['total_classes'])
    logger.info("  Sources processed: %s", schema_data['summary']['sources_processed'])

    logger.info("\n%s", '=' * 60)
    logger.info("Sample schema graph structure:")
    logger.info("%s\n", '=' * 60)

    count = 0
    combined_schema = schema_data.get("combined", {})
    for class_uri, properties in list(combined_schema.items())[:max_items]:
        logger.info("Class: %s", class_uri)
        for prop_uri, objects in list(properties.items())[:max_items]:
            logger.info("  Property: %s", prop_uri)
            for obj in objects[:max_items]:
                logger.info("    → %s", obj)
        logger.info("")
        count += 1
        if count >= max_items:
            break


def main():
    """Main execution function for command line usage."""
    schema_data = process_multiple_sources(verbose=True)
    display_schema_sample(schema_data)


if __name__ == "__main__":
    main()
