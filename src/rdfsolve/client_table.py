"""Build tables from retrieved RDF terms, not generated answer text."""

from __future__ import annotations

from typing import Any

import pandas as pd
from pydantic import BaseModel
from rdflib import BNode, URIRef

from rdfsolve.schema_models.enrichment import RdfTerm


def record_table(
    records: list[BaseModel],
    *,
    labels: dict[str, str],
    fields: list[str] | None = None,
    context: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Keep term lists in cells, typed records in attrs, and unread fields as NA.

    Context holds session evidence, not a claim that every query produced every row.
    This function reads retained terms only and never sends queries.
    """
    from rdfsolve.client_api import _title

    names = (
        fields
        if fields is not None
        else sorted({name for record in records for name in vars(record)["rdf_loaded_fields"]})
    )
    rows = []
    for record in records:
        payload = record.model_dump(mode="json")
        iri = str(getattr(type(record), "rdf_class_iri", ""))
        identifier = payload["uri"]
        row: dict[str, Any] = {
            "Name": _title(record),
            "Class": labels.get(iri, type(record).__name__),
            "Identifier": BNode(identifier[2:])
            if identifier.startswith("_:")
            else URIRef(identifier),
            "Type": URIRef(iri),
        }
        for name in names:
            row[name] = (
                [
                    RdfTerm.model_validate(term).to_rdf()
                    for term in payload["rdf_terms"].get(name, [])
                ]
                if name in payload["rdf_loaded_fields"]
                else pd.NA
            )
        rows.append(row)
    table = pd.DataFrame(rows, columns=["Name", "Class", "Identifier", "Type", *names])
    table.attrs.update(context or {})
    table.attrs["records"] = records
    return table
