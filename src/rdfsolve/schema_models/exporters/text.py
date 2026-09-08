"""Limit exported descriptions without changing RDF identifiers or examples."""

from typing import TypeVar

from pydantic import BaseModel

from rdfsolve.schema_models.enrichment import DEFINITION_PREDICATES, TermAnnotation


def clip_description(text: str | None, limit: int | None) -> str | None:
    """Keep at most limit Unicode characters, without an added suffix."""
    return text if limit is None or text is None else text[:limit]


T = TypeVar("T", bound=BaseModel)


def trim_descriptions(model: T, limit: int | None) -> T:
    """Return a text-limited copy. Keep the original model unchanged."""
    if limit is None:
        return model
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("trim_descriptions must be a nonnegative integer or None")
    result = model.model_copy(deep=True)

    def visit(value: object) -> None:
        """Trim supported description fields in the copied model."""
        from rdfsolve.schema_models.metadata import MetadataDocument

        if isinstance(value, MetadataDocument):
            from rdflib import Literal
            from rdflib.namespace import DC, DCTERMS

            for subject, predicate, text in list(value.graph):
                if predicate in {DC.description, DCTERMS.description} and isinstance(text, Literal):
                    value.graph.remove((subject, predicate, text))
                    value.graph.add(
                        (
                            subject,
                            predicate,
                            Literal(str(text)[:limit], lang=text.language, datatype=text.datatype),
                        )
                    )
        elif isinstance(value, TermAnnotation):
            if value.predicate in DEFINITION_PREDICATES:
                value.text.value = value.text.value[:limit]
        elif isinstance(value, BaseModel):
            for name in type(value).model_fields:
                child = getattr(value, name)
                if name == "description" and isinstance(child, str):
                    setattr(value, name, child[:limit])
                else:
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
        elif isinstance(value, dict):
            for child in value.values():
                visit(child)

    visit(result)
    return result
