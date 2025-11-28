"""
Utility functions for schema analysis and visualization.

This module provides helper classes and functions to reduce code duplication
and improve maintainability in schema extraction notebooks.
"""

import os
import tempfile
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from IPython.display import Markdown, display
from linkml_runtime.utils.schemaview import SchemaView


class SchemaAnalyzer:
    """Helper class for LinkML schema analysis and comparison."""

    def __init__(self, vp: Any, dataset_name: str, exports_path: str) -> None:
        """Initialize the schema analyzer."""
        self.vp = vp
        self.dataset_name = dataset_name
        self.exports_path = exports_path

    def generate_linkml_schema(
        self, schema_suffix: str = "", use_jsonld_conversion: bool = True, save_to_file: bool = True
    ) -> Tuple[str, str, SchemaView]:
        """Generate LinkML schema with consistent naming and saving pattern."""
        schema_name = f"{self.dataset_name}_schema{schema_suffix}"

        yaml_text = self.vp.to_linkml_yaml(
            schema_name=schema_name,
            schema_description=f"LinkML schema for {self.dataset_name}",
            filter_void_nodes=True,
            use_jsonld_conversion=use_jsonld_conversion,
        )

        # Save to file if requested
        if save_to_file:
            linkml_file = os.path.join(self.exports_path, f"{schema_name}.yaml")
            with open(linkml_file, "w", encoding="utf-8") as f:
                f.write(yaml_text)
        else:
            # Use temporary file
            temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
            temp_file.write(yaml_text)
            temp_file.close()
            linkml_file = temp_file.name

        # Create SchemaView
        sv = SchemaView(linkml_file)
        return linkml_file, yaml_text, sv

    def analyze_schema_richness(self, sv: SchemaView) -> Dict[str, int]:
        """Analyze semantic richness of a LinkML schema."""
        stats = {
            "classes": len(sv.all_classes()),
            "slots": len(sv.all_slots()),
            "types": len(sv.all_types()),
            "slots_with_domains": 0,
            "slots_with_ranges": 0,
            "multivalued_slots": 0,
            "object_properties": 0,
        }

        for _slot_name, slot in sv.all_slots().items():
            if slot.domain_of:
                stats["slots_with_domains"] += 1
            if slot.range and slot.range != "string":
                stats["slots_with_ranges"] += 1
            if slot.multivalued:
                stats["multivalued_slots"] += 1
            if slot.range and slot.range in sv.all_classes():
                stats["object_properties"] += 1

        return stats

    def compare_schemas(
        self, sv1: SchemaView, sv2: SchemaView, name1: str = "Schema 1", name2: str = "Schema 2"
    ) -> Dict[str, Any]:
        """Compare two LinkML schemas."""
        slots1 = set(sv1.all_slots().keys())
        slots2 = set(sv2.all_slots().keys())

        return {
            "schema1_name": name1,
            "schema2_name": name2,
            "schema1_stats": self.analyze_schema_richness(sv1),
            "schema2_stats": self.analyze_schema_richness(sv2),
            "unique_to_schema1": len(slots1 - slots2),
            "unique_to_schema2": len(slots2 - slots1),
            "common_slots": len(slots1 & slots2),
            "schema1_only_examples": sorted(slots1 - slots2)[:3],
            "schema2_only_examples": sorted(slots2 - slots1)[:3],
        }

    def display_schema_summary(
        self, sv: SchemaView, title: str = "Schema Summary"
    ) -> Dict[str, int]:
        """Display a formatted summary of schema statistics."""
        stats = self.analyze_schema_richness(sv)

        display(
            Markdown(f"""
### {title}
- **Classes:** {stats["classes"]}
- **Slots:** {stats["slots"]}
- **Types:** {stats["types"]}
- **Slots with domains:** {stats["slots_with_domains"]}
- **Object properties:** {stats["object_properties"]}
- **Multivalued slots:** {stats["multivalued_slots"]}
""")
        )

        return stats

    def analyze_relationships(self, sv: SchemaView) -> Dict[str, Any]:
        """Analyze object properties and relationships in the schema."""
        object_properties = []
        data_properties = []
        uri_properties = []

        for slot_name, slot in sv.all_slots().items():
            if slot.range in sv.all_classes():
                object_properties.append((slot_name, slot.range))
            elif slot.range == "uriorcurie":
                uri_properties.append(slot_name)
            else:
                data_properties.append((slot_name, slot.range))

        return {
            "object_properties": object_properties,
            "data_properties": data_properties,
            "uri_properties": uri_properties,
            "relationship_count": len(object_properties),
        }


class CoverageVisualizer:
    """Helper class for coverage visualization."""

    def __init__(self, dataset_name: str):
        """Initialize the coverage visualizer."""
        self.dataset_name = dataset_name

    def create_coverage_chart(self, frequencies_df: pd.DataFrame) -> Optional[go.Figure]:
        """Create standardized coverage visualization."""
        if frequencies_df.empty:
            return None

        df = self._prepare_data(frequencies_df)
        fig = self._create_figure(df)
        return fig

    def _prepare_data(self, frequencies_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for visualization."""
        df = frequencies_df.copy()
        df["coverage_percent"] = pd.to_numeric(df["coverage_percent"], errors="coerce").fillna(0)
        df = df.sort_values("coverage_percent", ascending=False).reset_index(drop=True)

        def make_label(row: pd.Series) -> str:
            """Create formatted HTML label for schema triple visualization."""
            return (
                f"<b>{row['subject_class']}</b> "
                f"<span style='color:#888;'></span> "
                f"<i>{row['property']}</i> "
                f"<span style='color:#888;'></span> "
                f"<b>{row['object_class']}</b>"
            )

        df["styled_label"] = df.apply(make_label, axis=1)
        return df

    def _create_figure(self, df: pd.DataFrame) -> go.Figure:
        """Create the plotly figure."""
        text_positions = ["outside" if v < 95 else "inside" for v in df["coverage_percent"]]

        custom_colorscale = [
            [0.0, "#d36e61"],
            [0.4, "#e5cdbd"],
            [0.7, "#e8e4cf"],
            [1.0, "#c3d9c0"],
        ]

        bar_height = 26
        fig_height = min(2000, bar_height * len(df) + 200)

        fig = go.Figure(
            go.Bar(
                x=df["coverage_percent"],
                y=df["styled_label"],
                orientation="h",
                text=[f"{v:.1f}%" for v in df["coverage_percent"]],
                textposition=text_positions,
                marker={
                    "color": df["coverage_percent"],
                    "colorscale": custom_colorscale,
                    "cmin": 0,
                    "cmax": 100,
                    "line": {"color": "white", "width": 0.6},
                },
                hovertemplate=("<b>%{y}</b><br>Coverage: %{x:.1f}%<extra></extra>"),
            )
        )

        self._configure_layout(fig, fig_height)
        return fig

    def _configure_layout(self, fig: go.Figure, fig_height: int) -> None:
        """Configure the figure layout."""
        fig.update_layout(
            title={
                "text": f"Schema Pattern Coverage for {self.dataset_name}",
                "x": 0.5,
                "font": {"size": 18},
            },
            xaxis={
                "title": "Coverage (%)",
                "range": [0, 100],
                "ticksuffix": "%",
                "showgrid": True,
                "gridcolor": "rgba(220,220,220,0.3)",
            },
            yaxis={
                "title": "",
                "autorange": "reversed",
                "automargin": True,
                "fixedrange": False,
            },
            template="plotly_white",
            autosize=True,
            height=fig_height,
            margin={"t": 80, "b": 50, "l": 480, "r": 150},
            plot_bgcolor="white",
            paper_bgcolor="white",
        )

        fig.update_xaxes(fixedrange=True)

    def display_coverage_summary(self, frequencies_df: pd.DataFrame) -> None:
        """Display coverage summary statistics."""
        if frequencies_df.empty:
            display(Markdown("**No coverage data available**"))
            return

        avg_coverage = frequencies_df["coverage_percent"].mean()
        high_coverage = (frequencies_df["coverage_percent"] > 50).sum()

        display(
            Markdown(f"""
**Pattern Coverage Summary:**
- Average pattern coverage: **{avg_coverage:.1f}%**
- Patterns with >50% coverage: **{high_coverage}/{len(frequencies_df)}**
""")
        )


class PydanticModelAnalyzer:
    """Helper class for analyzing generated Pydantic models."""

    @staticmethod
    def extract_pydantic_models(namespace: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Pydantic model classes from a namespace."""

        def _is_pydantic_model(name: str, val: Any) -> bool:
            """Check if this is likely a generated Pydantic model class."""
            if not isinstance(val, type):
                return False
            try:
                has_model_fields = 0 < len(getattr(val, "model_fields", {}))
            except:
                has_model_fields = False
            return has_model_fields

        return {k: v for k, v in namespace.items() if _is_pydantic_model(k, v)}

    @staticmethod
    def show_model_fields(cls: type) -> str:
        """Generate a string representation of model fields."""
        if hasattr(cls, "model_fields"):
            fields = list(cls.model_fields.items())
            field_list = []
            for name, info in fields:
                field_list.append(f"  - `{name}`: {info.annotation}")
            return "\n".join(field_list)
        return "  No fields found"

    @classmethod
    def display_all_models(cls, pydantic_models: Dict[str, Any]) -> None:
        """Display all Pydantic model classes and their fields."""
        if not pydantic_models:
            display(Markdown("**No pydantic models found**"))
            return

        markdown_output = f"**All {len(pydantic_models)} generated Pydantic classes:**\n\n"

        for name in sorted(pydantic_models.keys()):
            markdown_output += f"### {name}\n"
            markdown_output += cls.show_model_fields(pydantic_models[name]) + "\n\n"

        display(Markdown(markdown_output))


def export_schema_files(
    discovery_df: pd.DataFrame, vp: Any, exports_path: str, dataset_name: str
) -> None:
    """Export schema files in various formats."""
    import json

    json_path = os.path.join(exports_path, f"{dataset_name}_schema.json")
    csv_path = os.path.join(exports_path, f"{dataset_name}_schema.csv")

    discovery_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(vp.to_json(filter_void_nodes=True), fh, indent=2)

    display(
        Markdown(f"""
**Schema files exported:**
- CSV: `{csv_path}`
- JSON: `{json_path}`
""")
    )
