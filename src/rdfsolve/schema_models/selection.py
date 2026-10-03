"""Selected fields and routes with their complete source evidence."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rdfsolve.schema_models.collections import CollectionProfile
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.navigation import NavigationPath
from rdfsolve.schema_models.pattern import SchemaPattern


class SchemaSelection(BaseModel):
    """A saved view of source fields; counts retain their source denominators."""

    model_config = ConfigDict(extra="forbid")

    source: MinedSchema
    fields: list[tuple[str, str]] = Field(default_factory=list)
    paths: list[NavigationPath] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_selection(self) -> SchemaSelection:
        """Require fields and paths present in the retained source."""
        available = {(p.subject_class, p.property_uri) for p in self.source.patterns}
        available.update((p.subject_class, p.property_uri) for p in self.source.collections or [])
        if self.source.shapes is not None:
            available.update(
                (shape.target_class, prop.path)
                for shape in self.source.shapes.node_shapes
                if shape.target_class
                for prop in shape.property_shapes
                if isinstance(prop.path, str) and prop.path
            )
        for field in self.fields:
            if field not in available:
                raise ValueError(f"Unknown selected field: {field}")
        retained = (
            {p.signature(): p for p in self.source.navigation.paths}
            if self.source.navigation
            else {}
        )
        for path in self.paths:
            if path.signature() not in retained:
                raise ValueError(f"Path is not retained in source navigation: {path.label()}")
            if any((s.subject_class, s.property_uri) not in available for s in path.steps):
                raise ValueError(f"Path fields are absent from source patterns: {path.label()}")
        self.fields = list(dict.fromkeys(self.fields))
        self.paths = list({p.signature(): retained[p.signature()] for p in self.paths}.values())
        return self

    def _fields(self) -> set[tuple[str, str]]:
        return set(self.fields) | {
            (step.subject_class, step.property_uri) for path in self.paths for step in path.steps
        }

    @property
    def patterns(self) -> list[SchemaPattern]:
        """Return all observed ranges for the selected class-property pairs."""
        fields = self._fields()
        return [p for p in self.source.patterns if (p.subject_class, p.property_uri) in fields]

    @property
    def collections(self) -> list[CollectionProfile] | None:
        """Return selected list profiles, retaining unknown inspection state."""
        if self.source.collections is None:
            return None
        fields = self._fields()
        return [p for p in self.source.collections if (p.subject_class, p.property_uri) in fields]

    def path_query(self, path: NavigationPath, *, include_unmatched: bool = True) -> str:
        """Build a scoped SELECT exposing every node of one selected path."""
        from rdfsolve.mining.navigation import path_query

        retained = {route.signature(): route for route in self.paths}
        if path.signature() not in retained:
            raise ValueError("Choose a selected path")
        return path_query(
            retained[path.signature()],
            self.source.about.graph_uris or [],
            type_context_graph_uris=(self.source.about.type_graph_uris or [])
            + (self.source.about.type_context_graph_uris or []),
            include_unmatched=include_unmatched,
        )
