"""Construct generated records from RDF terms and table values."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, get_args

import pyoxigraph as ox
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
    TypeAdapter,
    field_validator,
    model_validator,
)
from pydantic_core import core_schema
from rdflib import RDF, RDFS, XSD, BNode, Graph, Literal, URIRef

from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.paths import absolute_iri

_YEAR = r"(?P<year>-?(?:[0-9]{4}|[1-9][0-9]{4,}))"
_DAY = r"-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
_CLOCK = r"(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})(?:\.[0-9]+)?"


_INTEGER = r"[+-]?[0-9]+"
_DECIMAL = r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
# Numbers and booleans: the lexical forms of XML Schema. Other datatypes are not known here.
_NUMBERS = {
    str(XSD.boolean): r"true|false|1|0",
    str(XSD.decimal): _DECIMAL,
    str(XSD.double): _DECIMAL + r"(?:[eE][+-]?[0-9]+)?|[+-]?INF|NaN",
    str(XSD.float): _DECIMAL + r"(?:[eE][+-]?[0-9]+)?|[+-]?INF|NaN",
    **{
        str(XSD[name]): _INTEGER
        for name in (
            "integer",
            "int",
            "long",
            "short",
            "byte",
            "nonNegativeInteger",
            "positiveInteger",
            "nonPositiveInteger",
            "negativeInteger",
            "unsignedInt",
            "unsignedLong",
            "unsignedShort",
            "unsignedByte",
        )
    },
}


def _lexical(text: str, datatype: str) -> bool | None:
    """Check the lexical form of a literal for XML Schema dates, times, numbers and booleans.

    None means that the datatype is not known here. The value is not parsed, so no parser
    warnings are given for values that are only tried.
    """
    if datatype in _NUMBERS:
        return re.fullmatch(_NUMBERS[datatype], text) is not None
    form = {
        str(XSD.gYear): _YEAR,
        str(XSD.gYearMonth): _YEAR + r"-(?P<month>[0-9]{2})",
        str(XSD.date): _YEAR + _DAY,
        str(XSD.dateTime): _YEAR + _DAY + "T" + _CLOCK,
        str(XSD.time): _CLOCK,
    }.get(datatype)
    if form is None:
        return None
    match = re.fullmatch(form + r"(?P<zone>Z|[+-][0-9]{2}:[0-9]{2})?", text)
    if match is None:
        return False
    fields = match.groupdict()
    clock = [int(fields.get(key) or 0) for key in ("hour", "minute", "second")]
    if clock[0] > 23 or clock[1] > 59 or clock[2] > 59:
        return False
    zone = fields.get("zone")
    if zone and zone != "Z":
        hours, minutes = map(int, zone[1:].split(":"))
        if hours > 14 or minutes > 59 or (hours == 14 and minutes):
            return False
    month = int(fields.get("month") or 1)
    year = int(fields.get("year") or 2000)
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
        missing = [i for i, item in enumerate(value) if item is None]
        if missing:
            raise ValueError(f"{field}[{missing[0]}]: missing value; leave it out of the list")
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
    if isinstance(value, BaseModel) and not isinstance(value, (RdfTerm, RDFList)):
        _check_record(value, extra.get("rdf_patterns", []), field)
        return value
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
            if datatype == str(RDFS.Literal):
                continue  # any literal: accepted when explicit, never guessed
            if datatype == str(RDF.langString):
                if not language or not isinstance(value, str):
                    continue
                term = RdfTerm(kind="literal", value=value, language=language)
            else:
                if _lexical(str(value), datatype) is False:
                    continue  # check the date form before rdflib tries to parse it
                literal = Literal(value, datatype=URIRef(datatype), normalize=False)
                lexical = _lexical(str(literal), datatype)
                parsed = Literal(str(literal), datatype=URIRef(datatype), normalize=False)
                if datatype != str(XSD.string) and (
                    lexical is False
                    or (lexical is None and parsed.ill_typed is not False)
                    or parsed.ill_typed is True  # For example, -5 as a nonNegativeInteger.
                ):
                    continue
                term = RdfTerm.from_rdf(literal)
            try:
                _check_term(term, [pattern], field)
            except ValueError:
                continue
        candidates[(term.kind, term.datatype, term.language)] = term
    if language and len(candidates) == 2:
        tagged = [t for t in candidates.values() if t.language]
        plain = [t for t in candidates.values() if not t.language and t.datatype == str(XSD.string)]
        if len(tagged) == 1 and len(plain) == 1:
            return tagged[0]  # a language default makes text a language-tagged literal
    if len(candidates) == 1:
        return next(iter(candidates.values()))
    if candidates:
        raise ValueError(f"{field}: Ambiguous RDF value; use an explicit RDF term")
    raise ValueError(f"{field}: no valid RDF interpretation; check datatype, language or IRI")


def _check_record(record: BaseModel, patterns: list[dict[str, Any]], field: str) -> None:
    """Check that a linked record has a class that the field accepts."""
    targets = {p["object_class"] for p in patterns} - {"Literal"}
    if not patterns or targets & {"Resource", "BlankNode"}:
        return
    kinds = {getattr(k, "rdf_class_iri", None) for k in type(record).__mro__}
    kinds |= set(vars(record).get("rdf_type") or [])
    if targets & kinds:
        return
    names = sorted(re.split(r"[/#:]", target)[-1] for target in targets)
    use = f"Use a record of {', '.join(names)}, or an IRI." if names else "Use a literal."
    raise ValueError(f"{field}: a {type(record).__name__} record cannot be the value. {use}")


def _check_term(term: RdfTerm, patterns: list[dict[str, Any]], field: str) -> None:
    if term.kind == "uri":
        absolute_iri(term.value)
    if term.kind == "literal":
        if term.datatype:
            absolute_iri(term.datatype)
        if term.datatype == str(RDF.langString) and not term.language:
            raise ValueError(f"{field}: rdf:langString needs a language")
        literal = term.to_rdf()
        if (isinstance(literal, Literal) and literal.ill_typed) or _lexical(
            term.value, term.datatype or ""
        ) is False:
            raise ValueError(f"{field}: invalid lexical form for {term.datatype}")
    if not patterns:
        return
    for pattern in patterns:
        kind = pattern["object_class"]
        if term.kind == "literal" and kind == "Literal":
            datatype = str(RDF.langString) if term.language else term.datatype or str(XSD.string)
            if pattern.get("datatype") == str(RDFS.Literal) or datatype == (
                pattern.get("datatype") or str(XSD.string)
            ):
                return
        elif term.kind != "literal" and kind != "Literal":
            if (
                kind not in {"Resource", "BlankNode"}
                or (kind == "Resource" and term.kind == "uri")
                or (kind == "BlankNode" and term.kind == "bnode")
            ):
                return
    raise ValueError(f"{field}: RDF term does not match its field definition")


class LinkedValue:
    """Mark a field whose values can be linked records.

    The type hint names the classes of the linked records for readers and editors. The values
    are checked by rdfsolve against the RDF patterns of the field, not by pydantic. Pydantic
    then does not build a validator for each linked class, which is slow for large
    vocabularies.
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Accept values as they are. Restore records and lists that are given as JSON objects."""
        options = _leaf_types(source)
        return core_schema.no_info_plain_validator_function(lambda value: _restore(value, options))

    @classmethod
    def __get_pydantic_json_schema__(
        cls, schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> dict[str, Any]:
        """Describe the value as one IRI or a list of IRIs."""
        iri = {"type": "string", "description": "The IRI of a linked resource"}
        return {"anyOf": [iri, {"type": "array", "items": iri}]}


def _leaf_types(hint: Any) -> list[Any]:
    """Return the types in a union, a list or an optional type hint."""
    args = get_args(hint)
    return [leaf for arg in args for leaf in _leaf_types(arg)] if args else [hint]


def _restore(value: Any, options: list[Any]) -> Any:
    """Make records and RDF lists from JSON objects, for example after ``model_dump_json``."""
    from rdfsolve.client.collections import RDFList

    if isinstance(value, list):
        return [_restore(item, options) for item in value]
    if not isinstance(value, dict):
        return value
    classes: list[type[BaseModel]] = [
        o for o in options if isinstance(o, type) and issubclass(o, BaseModel)
    ]
    if "items" in value:
        lists = [c for c in classes if issubclass(c, RDFList)]
        return (lists[0] if lists else RDFList).model_validate(value)
    types = set(value.get("@type") or value.get("rdf_type") or [])
    modules = {sys.modules.get(c.__module__) for c in classes if hasattr(c, "rdf_class_iri")}
    found = [
        model
        for module in modules
        if module is not None
        for model in vars(module).values()
        if isinstance(model, type) and getattr(model, "rdf_class_iri", None) in types
    ]
    records: list[type[BaseModel]] = found or [c for c in classes if hasattr(c, "rdf_class_iri")]
    return records[0].model_validate(value) if records else value


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
            from rdfsolve.client.model_rdf import check_record

            check_record(self)
        return self

    def to_graph(self, *, fields: list[str] | None = None) -> Graph:
        """Write populated RDF fields with their retained terms."""
        from rdfsolve.client.model_rdf import model_to_graph

        return model_to_graph(self, fields=fields)

    def to_oxigraph(self, *, fields: list[str] | None = None) -> ox.Dataset:
        """Write the statements of ``to_graph`` as an Oxigraph dataset."""
        from rdfsolve.local_rdf import to_oxigraph

        return to_oxigraph(self.to_graph(fields=fields))


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
