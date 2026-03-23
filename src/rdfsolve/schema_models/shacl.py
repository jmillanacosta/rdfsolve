"""SHACL shape generation from JSON-LD.

Converts a rdfsolve JSON-LD schema dict to SHACL Turtle via the
LinkML -> ShaclGenerator pipeline.
"""

from __future__ import annotations

from typing import Any, cast

from linkml.generators.shaclgen import ShaclGenerator
from linkml.generators.yamlgen import YAMLGenerator

from rdfsolve.schema_models.linkml import to_linkml

__all__ = ["to_shacl"]


def to_shacl(
    jsonld: dict[str, Any],
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
    closed: bool = True,
    suffix: str | None = None,
    include_annotations: bool = False,
) -> str:
    """Generate SHACL shapes (Turtle) from a JSON-LD schema dict.

    Parameters
    ----------
    jsonld:
        JSON-LD document (``@context``, ``@graph``, …).
    schema_name:
        Name for the underlying LinkML schema.
    schema_description:
        Human-readable description.
    schema_base_uri:
        Base URI for the schema.
    closed:
        If *True*, produce closed SHACL shapes (``sh:closed true``).
    suffix:
        Suffix appended to every shape name
        (e.g. ``"Shape"`` → ``PersonShape``).
    include_annotations:
        If *True*, carry annotations through to shapes.

    Returns
    -------
    str
        SHACL shapes serialised as Turtle.
    """
    linkml_schema = to_linkml(
        jsonld,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )
    linkml_yaml = YAMLGenerator(linkml_schema).serialize()
    shacl_gen = ShaclGenerator(
        schema=linkml_yaml,
        closed=closed,
        suffix=suffix,
        include_annotations=include_annotations,
    )
    return cast(str, shacl_gen.serialize())
