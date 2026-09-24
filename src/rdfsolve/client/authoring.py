"""Construct generated records from RDF terms and table values."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator
from rdflib import RDF, XSD, BNode, Graph, Literal, URIRef

from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import absolute_iri


def _check_term(term: RdfTerm, patterns: list[dict[str, Any]], field: str) -> None:
    if term.kind == "uri":
        absolute_iri(term.value)
    if term.kind == "literal":
        if term.datatype:
            absolute_iri(term.datatype)
        if term.datatype == str(RDF.langString) and not term.language:
            raise ValueError(f"{field}: rdf:langString needs a language")
        literal = term.to_rdf()
        if isinstance(literal, Literal) and literal.ill_typed:
            raise ValueError(f"{field}: invalid lexical form for {term.datatype}")
    if not patterns:
        return
    for pattern in patterns:
        kind = pattern["object_class"]
        if term.kind == "literal" and kind == "Literal":
            datatype = str(RDF.langString) if term.language else term.datatype or str(XSD.string)
            if datatype == (pattern.get("datatype") or str(XSD.string)):
                return
        elif term.kind != "literal" and kind != "Literal":
            if (
                kind not in {"Resource", "BlankNode"}
                or (kind == "Resource" and term.kind == "uri")
                or (kind == "BlankNode" and term.kind == "bnode")
            ):
                return
    raise ValueError(f"{field}: RDF term does not match its field definition")


class RDFRecord(BaseModel):
    """An authored or retrieved record with retained RDF terms."""

    rdf_contract: ClassVar[bool] = False

    model_config = ConfigDict(populate_by_name=True, extra="allow")
    uri: str = Field(default_factory=lambda: "_:" + str(BNode()), alias="@id", min_length=1)
    rdf_type: list[str] = Field(default_factory=list, alias="@type")
    rdf_terms: dict[str, list[dict[str, Any]]] = Field(default_factory=dict, repr=False)
    rdf_loaded_fields: list[str] = Field(default_factory=list, repr=False)
    rdf_source: dict[str, Any] = Field(default_factory=dict, repr=False)

    @field_validator("uri", mode="before")
    @classmethod
    def check_identifier(cls, value: Any) -> str:
        """Accept an absolute IRI or a labeled blank node."""
        if isinstance(value, BNode):
            return "_:" + str(value)
        if not isinstance(value, str):
            raise ValueError("Use an IRI or blank node as the identifier")
        if value.startswith("_:"):
            if not value[2:]:
                raise ValueError("A blank node needs a label")
        else:
            absolute_iri(value)
        return value

    @model_validator(mode="before")
    @classmethod
    def read_terms(cls, value: Any) -> Any:
        """Retain explicit RDF terms while validating their Python values."""
        from rdfsolve.client.hydration import _value, field_metadata
        from rdfsolve.client.model_rdf import _new_term

        if not isinstance(value, dict):
            return value
        data = dict(value)
        retained = dict(data.get("rdf_terms", {}))
        for name, info in cls.model_fields.items():
            extra = field_metadata(info)
            if not extra.get("rdf_path"):
                continue
            key = info.alias if info.alias in data else name
            if key not in data:
                continue
            raw = data[key]
            values = raw if isinstance(raw, list) else [raw]
            if not any(isinstance(item, (RdfTerm, Literal, URIRef, BNode)) for item in values):
                if name in retained:
                    native = [_value(RdfTerm.model_validate(term)) for term in retained[name]]
                    if TypeAdapter(list[Any]).dump_python(native, mode="json") == values:
                        data[key] = native if isinstance(raw, list) else native[0]
                continue
            terms = []
            converted = []
            for item in values:
                if isinstance(item, RdfTerm):
                    term = item
                elif isinstance(item, (Literal, URIRef, BNode)):
                    term = RdfTerm.from_rdf(item)
                elif isinstance(item, BaseModel):
                    identifier = str(vars(item)["uri"])
                    term = RdfTerm.from_rdf(
                        BNode(identifier[2:]) if identifier.startswith("_:") else URIRef(identifier)
                    )
                else:
                    term = RdfTerm.from_rdf(_new_term(item, extra, ""))
                _check_term(term, extra.get("rdf_patterns", []), name)
                terms.append(term.model_dump(mode="json"))
                converted.append(
                    item
                    if isinstance(item, BaseModel) and not isinstance(item, RdfTerm)
                    else _value(term)
                )
            retained[name] = terms
            data[key] = converted if isinstance(raw, list) else converted[0]
        data["rdf_terms"] = retained
        return data

    @model_validator(mode="after")
    def check_contract(self) -> RDFRecord:
        """Validate authored contract records before returning them."""
        if self.rdf_contract:
            self.to_graph()
        return self

    def to_graph(self, *, fields: list[str] | None = None) -> Graph:
        """Write populated RDF fields with their retained terms."""
        from rdfsolve.client.model_rdf import model_to_graph

        return model_to_graph(self, fields=fields)


def create_record(
    model: type[BaseModel],
    values: Mapping[str, Any],
    *,
    uri: str | BNode | None = None,
    blank_node_scope: str | None = None,
) -> BaseModel:
    """Create a named or anonymous record with explicit blank-node scope."""
    from rdfsolve.client.hydration import class_iri

    data = dict(values)
    if uri is not None:
        identifier = "_:" + str(uri) if isinstance(uri, BNode) else uri
        if not identifier.startswith("_:"):
            absolute_iri(identifier)
        elif not identifier[2:]:
            raise ValueError("A blank node needs a label")
        data["uri"] = identifier
    data["rdf_type"] = [class_iri(model)]
    if blank_node_scope is not None:
        if not blank_node_scope:
            raise ValueError("Use a nonempty blank_node_scope")
        data["rdf_source"] = {"blank_node_scope": blank_node_scope}
    return model.model_validate(data)


def table_literal(value: Any, *, language: str | None, datatype: str | None) -> Any:
    """Apply column defaults to plain values; explicit RDF terms keep their metadata."""
    import pandas as pd

    if language and datatype:
        raise ValueError("Choose language or datatype for a column")
    if value is None or (pd.api.types.is_scalar(value) and pd.isna(value)):
        return None
    if isinstance(value, (RdfTerm, Literal, URIRef, BNode)):
        return value
    if isinstance(value, list):
        return [table_literal(item, language=language, datatype=datatype) for item in value]
    if language or datatype:
        return Literal(
            value, lang=language, datatype=URIRef(datatype) if datatype else None, normalize=False
        )
    return value
