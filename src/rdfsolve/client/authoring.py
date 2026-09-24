"""Construct generated records from RDF terms and table values."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator
from rdflib import RDF, XSD, BNode, Graph, Literal, URIRef

from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import absolute_iri


def _date_lexical(text: str, datatype: str) -> bool | None:
    """Check calendar fields and timezone offsets for XML Schema dates."""
    suffix = {
        str(XSD.gYear): "",
        str(XSD.gYearMonth): r"-(?P<month>[0-9]{2})",
        str(XSD.date): r"-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})",
    }.get(datatype)
    if suffix is None:
        return None
    match = re.fullmatch(
        r"(?P<year>-?(?:[0-9]{4}|[1-9][0-9]{4,}))" + suffix + r"(?P<zone>Z|[+-][0-9]{2}:[0-9]{2})?",
        text,
    )
    if match is None:
        return False
    fields = match.groupdict()
    zone = fields.get("zone")
    if zone and zone != "Z":
        hours, minutes = map(int, zone[1:].split(":"))
        if hours > 14 or minutes > 59 or (hours == 14 and minutes):
            return False
    month = int(fields.get("month") or 1)
    year = int(fields["year"])
    if not 1 <= month <= 12:
        return False
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    days = (31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    return 1 <= int(fields.get("day") or 1) <= days[month - 1]


def coerce_value(
    value: Any,
    extra: dict[str, Any],
    field: str,
    language: str | None = None,
) -> Any:
    """Resolve plain values only when the field permits one RDF interpretation."""
    from rdfsolve.client.collections import RDFList, member_patterns

    if value is None:
        return None
    if isinstance(value, list):
        return [coerce_value(item, extra, field, language) for item in value]
    if isinstance(value, RDFList):
        metadata = {"rdf_patterns": member_patterns(extra)}
        return value.model_copy(
            update={
                "items": [
                    coerce_value(item, metadata, f"{field}[{i}]", language)
                    for i, item in enumerate(value.items)
                ]
            }
        )
    if isinstance(value, (RdfTerm, Literal, URIRef, BNode, BaseModel)):
        return value
    patterns = extra.get("rdf_patterns", [])
    if not patterns:
        return value
    candidates: dict[tuple[str, str | None, str | None], RdfTerm] = {}
    for pattern in patterns:
        kind = pattern["object_class"]
        if kind != "Literal":
            if not isinstance(value, str):
                continue
            term = RdfTerm(
                kind="bnode" if value.startswith("_:") else "uri",
                value=value.removeprefix("_:"),
            )
            try:
                _check_term(term, [pattern], field)
            except ValueError:
                continue
        else:
            datatype = pattern.get("datatype") or str(XSD.string)
            if datatype == str(RDF.langString):
                if not language or not isinstance(value, str):
                    continue
                term = RdfTerm(kind="literal", value=value, language=language)
            else:
                literal = Literal(value, datatype=URIRef(datatype), normalize=False)
                lexical = _date_lexical(str(literal), datatype)
                parsed = Literal(str(literal), datatype=URIRef(datatype), normalize=False)
                if datatype != str(XSD.string) and (
                    lexical is False or (lexical is None and parsed.ill_typed is not False)
                ):
                    continue
                term = RdfTerm.from_rdf(literal)
            try:
                _check_term(term, [pattern], field)
            except ValueError:
                continue
        candidates[(term.kind, term.datatype, term.language)] = term
    if len(candidates) == 1:
        return next(iter(candidates.values()))
    if candidates:
        raise ValueError(f"{field}: Ambiguous RDF value; use an explicit RDF term")
    raise ValueError(f"{field}: no valid RDF interpretation; check datatype, language or IRI")


def _check_term(term: RdfTerm, patterns: list[dict[str, Any]], field: str) -> None:
    if term.kind == "uri":
        absolute_iri(term.value)
    if term.kind == "literal":
        if term.datatype:
            absolute_iri(term.datatype)
        if term.datatype == str(RDF.langString) and not term.language:
            raise ValueError(f"{field}: rdf:langString needs a language")
        literal = term.to_rdf()
        if (isinstance(literal, Literal) and literal.ill_typed) or _date_lexical(
            term.value, term.datatype or ""
        ) is False:
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
    language: str | None = None,
    extra_types: Sequence[str] = (),
) -> BaseModel:
    """Create a named or anonymous record with explicit blank-node scope."""
    from rdfsolve.client.hydration import class_iri, field_metadata

    data = {
        name: coerce_value(value, field_metadata(model.model_fields[name]), name, language)
        for name, value in values.items()
    }
    if uri is not None:
        identifier = "_:" + str(uri) if isinstance(uri, BNode) else uri
        if not identifier.startswith("_:"):
            absolute_iri(identifier)
        elif not identifier[2:]:
            raise ValueError("A blank node needs a label")
        data["uri"] = identifier
    if isinstance(extra_types, str):
        raise ValueError("extra_types must be a sequence of IRIs")
    for iri in extra_types:
        absolute_iri(iri)
    data["rdf_type"] = list(dict.fromkeys([class_iri(model), *extra_types]))
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
