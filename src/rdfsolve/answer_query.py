"""Compile selected RDF routes into one answer query and retain its bindings."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
from rdflib import DCTERMS, RDF, RDFS, SH, BNode, Graph, Literal, URIRef

from rdfsolve.hydration import _iri, _term, _value
from rdfsolve.query_collection import QueryCollection
from rdfsolve.rdf_operations import PathFilter
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import DEFINITION_PREDICATES, LABEL_PREDICATES, RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_rdf, path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.schema_models.readers.paths import read_path
from rdfsolve.sparql_helper import SparqlHelper

if TYPE_CHECKING:
    from pydantic import BaseModel

    from rdfsolve.client_session import ClientSession

logger = logging.getLogger(__name__)


def execute_answer(
    session: ClientSession,
    references: list[str],
    name: str,
    fields: dict[str, list[str]],
    *,
    paths: list[str] | None = None,
    where: list[PathFilter] | None = None,
) -> dict[str, Any]:
    """Query selected paths and filters, then their fields without multiplying rows."""
    if len(references) == 1 and references[0] in session.final_queries:
        previous = session.final_queries[references[0]]
        paths = paths or previous["paths"]
        where = where or [PathFilter.model_validate(item) for item in previous["where"]]
    references = list(
        dict.fromkeys(
            original
            for ref in references
            for original in session.final_queries.get(ref, {}).get("references", [ref])
        )
    )
    selected = [session.result(ref) for ref in references]
    routes: dict[str, set[str]] = {}
    for result in selected:
        for key in result.coverage.get("selected_paths", []):
            source_type = session.routes[key][0][0]
            for source in result.coverage.get("sources", []):
                if source["type"] == source_type:
                    routes.setdefault(key, set()).add(source["iri"])
        for match in result.evidence:
            if match.get("nodes") and match["nodes"][0]["kind"] == "uri":
                routes.setdefault(match["path"], set()).add(match["nodes"][0]["value"])
    for key in paths or []:
        if key not in session.routes:
            raise ValueError("Unknown path ID; choose a path from plan or paths")
        source_type = session.routes[key][0][0]
        sources = {
            str(vars(record)["uri"])
            for result in selected
            for record in result
            if getattr(type(record), "rdf_class_iri", "") == source_type
        }
        if not sources and not where:
            raise ValueError("Supply where filters or retained source records for selected paths")
        routes.setdefault(key, set()).update(sources)
    if not routes:
        raise ValueError("Supply paths from plan or paths, or references from a route read")
    if len(routes) > 20:
        raise ValueError("Select at most 20 answer routes")
    chosen = {
        str(getattr(session.client.model(kind), "rdf_class_iri", "")): names
        for kind, names in fields.items()
    }
    used_classes = {
        cls for key in routes for edge in session.routes[key] for cls in (edge[0], edge[2])
    }
    if chosen.keys() - used_classes:
        raise ValueError("Requested fields include a class outside the selected routes")
    collection = QueryCollection()
    branches, bodies, columns = [], [], {}
    for index, (key, sources) in enumerate(sorted(routes.items())):
        route = session.routes[key]
        branch, body = _branch(session, route, sorted(sources), chosen, index, collection.graph)
        body += _filters(session, branch, where or [])
        branches.append(branch)
        bodies.append("{ " + session.client._scope(body) + " }")
        columns.update(branch["columns"])
    variables = ["_route", *[var for var in columns if "_field_" not in var], "_graph"]
    query = (
        "SELECT DISTINCT "
        + " ".join("?" + var for var in variables)
        + " WHERE {\n"
        + "\nUNION\n".join(bodies)
        + "\n}"
    )
    saved = collection.add(
        name,
        query,
        description=(
            "Read the selected routes and filters. Record fields are queried separately. "
            "Relationship names and definitions are copied from the saved schema."
        ),
        endpoint=str(session.registry.binding.get("endpoint") or ""),
    )
    for shape in list(collection.graph.subjects(RDF.type, SH.PropertyShape)):
        collection.graph.add((saved.node, DCTERMS.references, shape))
    with session.client.step(name):
        keys = [
            var
            for var in variables
            if var in {"_route", "_graph", "source", "target"}
            or (var.startswith("via") and "_" not in var)
        ]
        bindings, query_ids = _read_all(session, query, name, cursor_keys=keys)
        metadata, metadata_ids = _read_fields(session, bindings, branches, bodies, collection, name)
    partial = any(
        item.coverage.get("source", item.coverage).get("status") == "partial" for item in selected
    )
    if partial:
        logger.warning("Answer selection is partial: an earlier exploration reached a limit")
    return {
        "references": list(dict.fromkeys(references)),
        "paths": list(routes),
        "where": [item.model_dump() for item in where or []],
        "name": name,
        "query": query,
        "shacl": collection.to_turtle(),
        "bindings": bindings,
        "metadata": metadata,
        "metadata_query_ids": metadata_ids,
        "branches": branches,
        "columns": columns,
        "query_id": len(session.client._records()),
        "binding_query_ids": query_ids,
        "coverage": {
            "status": "partial" if partial else "complete",
            "basis": "Selected routes and explicit filters, not exhaustive topic coverage",
            "retrieval": "complete",
        },
    }


def _read_all(
    session: ClientSession,
    query: str,
    name: str,
    *,
    page_size: int | None = None,
    cursor_keys: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[int]]:
    """Read all rows and keep the page query ID for each row."""
    client = session.client
    if isinstance(client.source, Graph):
        rows = client._select(query)
        return rows, [len(client._records())] * len(rows)
    helper: SparqlHelper = client.source
    rows, ids = [], []
    start = len(client._records())
    try:
        for page in helper.select_chunked(
            helper.prepare_paginated_query(query),
            chunk_size=page_size or client.max_rows,
            max_pages=None,
            until_empty=True,
            stable_terms=True,
            pagination="cursor",
            cursor_keys=cursor_keys,
            purpose=name,
        ):
            rows.extend(page)
            ids.extend([len(client._records())] * len(page))
    finally:
        client.queries.extend(record.query for record in client._records()[start:])
    return rows, ids


def _filters(session: ClientSession, branch: dict[str, Any], filters: list[PathFilter]) -> str:
    """AND filters together; OR values within each filter. Do not join matching text."""
    expressions = []
    for index, criterion in enumerate(filters):
        model = session.client.model(criterion.kind)
        slots = [
            node["variable"]
            for node in branch["nodes"]
            if node["type"] == getattr(model, "rdf_class_iri", "")
        ]
        if not slots:
            raise ValueError(f"Filter class {criterion.kind} is absent from a selected route")
        if criterion.iris:
            values = ", ".join(_iri(iri) for iri in criterion.iris)
            expressions.append(" || ".join(f"?{slot} IN ({values})" for slot in slots))
            continue
        names = criterion.fields or [
            name
            for name, field in model.model_fields.items()
            if isinstance(field.json_schema_extra, dict)
            and field.json_schema_extra.get("rdf_property_iri")
            in (*LABEL_PREDICATES, *DEFINITION_PREDICATES)
        ]
        predicates = []
        for name in names:
            extra = model.model_fields[session.client.field_name(model, name)].json_schema_extra
            if not isinstance(extra, dict) or not extra.get("rdf_path"):
                raise ValueError(f"No RDF path for {criterion.kind}.{name}")
            predicates.append(path_to_sparql(PropertyPath.model_validate(extra["rdf_path"])))
        if not predicates:
            raise ValueError(
                f"Choose text fields for {criterion.kind}; no name or description fields are recorded"
            )
        text = f"?match{index}"
        terms = " || ".join(
            f"CONTAINS(LCASE(STR({text})), LCASE({Literal(term).n3()}))" for term in criterion.terms
        )
        expressions.append(
            " || ".join(
                f"EXISTS {{ ?{slot} ({'|'.join(predicates)}) {text} . FILTER({terms}) }}"
                for slot in slots
            )
        )
    return " ".join(f"FILTER({expression})" for expression in expressions)


def _read_fields(
    session: ClientSession,
    bindings: list[dict[str, Any]],
    branches: list[dict[str, Any]],
    bodies: list[str],
    collection: QueryCollection,
    name: str,
) -> tuple[list[dict[str, Any]], list[int]]:
    """Read each resource/property pair once, keeping graph and RDF term identity."""
    resources: dict[str, set[str]] = {}
    for row in bindings:
        for node in branches[int(row["_route"]["value"])]["nodes"]:
            if not node["fields"]:
                continue
            subject = _term(row[node["variable"]]).to_rdf().n3()
            resources.setdefault(subject, set()).update(
                field["predicate"] for field in node["fields"]
            )
    if not any(resources.values()):
        return [], []
    rows: list[dict[str, Any]] = []
    ids: list[int] = []
    if any(resource.startswith("_:") for resource in resources):
        if not isinstance(session.client.source, Graph):
            raise ValueError(
                "Remote blank nodes must be read with their anchored path in one response"
            )
        choices = []
        for branch, body in zip(branches, bodies, strict=True):
            for node in branch["nodes"]:
                if node["fields"]:
                    predicates = " ".join(_iri(field["predicate"]) for field in node["fields"])
                    choices.append(
                        "{ "
                        + body
                        + f" BIND(?{node['variable']} AS ?resource) FILTER(isBlank(?resource)) "
                        + session.client._scope(
                            f"VALUES ?predicate {{ {predicates} }} ?resource ?predicate ?value"
                        )
                        + " }"
                    )
        query = (
            "SELECT DISTINCT ?resource ?predicate ?value ?_graph WHERE { "
            + " UNION ".join(choices)
            + " }"
        )
        purpose = f"{name}: blank-node fields"
        collection.add(purpose, query)
        rows, ids = _read_all(session, query, purpose)
        resources = {key: value for key, value in resources.items() if not key.startswith("_:")}
    batch_size = min(session.client.max_rows, 500)
    ordered = sorted(resources)
    for start in range(0, len(ordered), batch_size):
        groups: dict[tuple[str, ...], list[str]] = {}
        for resource in ordered[start : start + batch_size]:
            if resources[resource]:
                groups.setdefault(tuple(sorted(resources[resource])), []).append(resource)
        body = " UNION ".join(
            "{ VALUES ?resource { "
            + " ".join(subjects)
            + " } VALUES ?predicate { "
            + " ".join(_iri(predicate) for predicate in predicates)
            + " } ?resource ?predicate ?value }"
            for predicates, subjects in groups.items()
        )
        query = (
            "SELECT DISTINCT ?resource ?predicate ?value ?_graph WHERE { "
            + session.client._scope(body)
            + " }"
        )
        purpose = f"{name}: fields {start // batch_size + 1}"
        collection.add(purpose, query, endpoint=str(session.registry.binding.get("endpoint") or ""))
        found, query_ids = _read_all(session, query, purpose, page_size=500)
        rows.extend(found)
        ids.extend(query_ids)
    return rows, ids


def _branch(
    session: ClientSession,
    route: list[tuple[str, str, str, bool]],
    sources: list[str],
    chosen: dict[str, list[str]],
    index: int,
    graph: Graph,
) -> tuple[dict[str, Any], str]:
    """Compile one typed route and its optional fields."""
    items = [PropertyPath(operator="predicate", iri=edge[1]) for edge in route]
    items = [
        PropertyPath(operator="inverse", items=[item]) if edge[3] else item
        for item, edge in zip(items, route, strict=True)
    ]
    path = PropertyPath(operator="sequence", items=items) if len(items) > 1 else items[0]
    shape = BNode()
    graph.add((shape, RDF.type, SH.PropertyShape))
    path_node = path_to_rdf(path, graph)
    graph.add((shape, SH.path, path_node))
    graph.add((shape, SH.name, Literal(f"Route {index + 1}")))
    # Read the exported path before compiling its steps to retain intermediate bindings.
    restored = read_path(graph, path_node)
    steps = restored.items if restored.operator == "sequence" else [restored]
    classes = [route[0][0], *[edge[2] for edge in route]]
    slots = ["source", *[f"via{i}" for i in range(1, len(route))], "target"]
    body = f"BIND({index} AS ?_route) "
    if sources:
        body += f"VALUES ?source {{ {' '.join(_iri(iri) for iri in sources)} }} "
    columns, nodes, edges = {}, [], []
    for i, (slot, cls) in enumerate(zip(slots, classes, strict=True)):
        model = session.client.model(cls)
        label = session.client.type_name(model)
        columns[slot] = f"{slot.capitalize()} IRI"
        columns[slot + "_class"] = f"{slot.capitalize()} class"
        body += f"?{slot} a {_iri(cls)} . BIND({_iri(cls)} AS ?{slot}_class) "
        names = chosen.get(cls)
        available = {
            name: info.json_schema_extra
            for name, info in model.model_fields.items()
            if isinstance(info.json_schema_extra, dict) and info.json_schema_extra.get("rdf_path")
        }
        if names is None:
            names = [
                name
                for name, extra in available.items()
                if isinstance(extra, dict)
                and extra.get("rdf_property_iri") in (*LABEL_PREDICATES, *DEFINITION_PREDICATES)
            ]
        projections = []
        for requested in dict.fromkeys(names):
            name = session.client.field_name(model, requested)
            extra = available.get(name)
            if not isinstance(extra, dict):
                raise ValueError(f"No RDF field definition for {label}.{name}")
            field_path = PropertyPath.model_validate(extra["rdf_path"])
            if field_path.operator != "predicate":
                raise ValueError(f"Final answer fields need direct predicates: {label}.{name}")
            var = f"{slot}_field_{name}"
            columns[var] = f"{slot.capitalize()} {name}"
            projections.append({"variable": var, "field": name, "predicate": field_path.iri})
        nodes.append({"variable": slot, "type": cls, "fields": projections})
        if i < len(route):
            body += f"?{slot} {path_to_sparql(steps[i])} ?{slots[i + 1]} . "
            predicate, reverse = route[i][1], route[i][3]
            var = f"relationship{i + 1}"
            columns[var] = f"Relationship {i + 1} IRI"
            body += f"BIND({_iri(predicate)} AS ?{var}) "
            texts = session.client._schema.enrichment
            label_text = next(
                (item.text.value for item in texts.labels if item.term_iri == predicate),
                predicate.rsplit("/", 1)[-1].rsplit("#", 1)[-1],
            )
            for suffix, text in (
                ("name", label_text),
                ("definition", texts.description(predicate)),
            ):
                columns[f"{var}_{suffix}"] = f"Relationship {i + 1} {suffix}"
                if text:
                    body += f"BIND({Literal(text).n3()} AS ?{var}_{suffix}) "
            edges.append(
                {"source": slot, "target": slots[i + 1], "predicate": predicate, "inverse": reverse}
            )
    for i, slot in enumerate(slots):
        for other in slots[:i]:
            body += f"FILTER(!sameTerm(?{slot}, ?{other})) "
    return {"nodes": nodes, "edges": edges, "columns": columns, "sources": sources}, body


class QueryAnswer:
    """Observed connections, record fields, query examples and an RDF subset."""

    def __init__(self, payload: dict[str, Any]) -> None:
        """Restore an answer without endpoint requests."""
        self.payload = payload
        self.execution = payload["answer"]
        self.query: str = self.execution["query"]
        self.name: str = self.execution["name"]
        self.bindings: list[dict[str, Any]] = self.execution["bindings"]
        self.coverage: dict[str, Any] = self.execution["coverage"]
        self._fields: dict[tuple[str, str, str], list[Any]] = {}
        for row in self.execution["metadata"]:
            key = (
                row.get("_graph", {}).get("value", ""),
                row["resource"]["value"],
                row["predicate"]["value"],
            )
            self._fields.setdefault(key, []).append(_term(row["value"]).to_rdf())

    def _values(
        self, row: dict[str, Any], node: dict[str, Any], field: dict[str, Any]
    ) -> list[Any]:
        """Look up a field in the same graph as its observed connection."""
        return self._fields.get(
            (
                row.get("_graph", {}).get("value", ""),
                row[node["variable"]]["value"],
                field["predicate"],
            ),
            [],
        )

    def table(self) -> pd.DataFrame:
        """Show one row per connection, with lists for fields that have several values."""
        columns = self.execution["columns"]
        rows = []
        for row in self.bindings:
            displayed: dict[str, Any] = {
                label: _term(row[var]).to_rdf() if var in row else None
                for var, label in columns.items()
            }
            for node in self.execution["branches"][int(row["_route"]["value"])]["nodes"]:
                for field in node["fields"]:
                    values = self._values(row, node, field)
                    displayed[columns[field["variable"]]] = (
                        values[0] if len(values) == 1 else values or None
                    )
            rows.append(displayed)
        table = pd.DataFrame(rows, columns=list(columns.values()))
        table.attrs.update(
            title=self.name, coverage=self.coverage, bindings=self.bindings, query=self.query
        )
        return table

    def to_shacl(self) -> Graph:
        """Return the standalone SELECT example and referenced descriptive paths."""
        return Graph().parse(data=self.execution["shacl"], format="turtle")

    def _observed(self) -> tuple[Graph, dict[tuple[str, str], set[str]]]:
        graph = Graph()
        nodes: dict[tuple[str, str], set[str]] = {}
        for row in self.bindings:
            branch = self.execution["branches"][int(row["_route"]["value"])]
            for node in branch["nodes"]:
                subject = _term(row[node["variable"]]).to_rdf()
                graph.add((subject, RDF.type, URIRef(node["type"])))
                fields = nodes.setdefault((node["type"], subject.n3()), set())
                for field in node["fields"]:
                    fields.add(field["field"])
                    for value in self._values(row, node, field):
                        graph.add((subject, URIRef(field["predicate"]), value))
            for edge in branch["edges"]:
                source, target = (_term(row[edge[key]]).to_rdf() for key in ("source", "target"))
                if edge["inverse"]:
                    source, target = target, source
                graph.add((source, URIRef(edge["predicate"]), target))
        return graph, nodes

    def records(self) -> list[BaseModel]:
        """Build generated records from returned terms only, including path edges."""
        from rdflib.util import from_n3

        graph, nodes = self._observed()
        models = MinedSchema.from_dict(self.payload["schema"]).to_pydantic_classes()
        by_type = {str(getattr(model, "rdf_class_iri", "")): model for model in models.values()}
        records = []
        for (cls, subject_n3), loaded in nodes.items():
            subject = from_n3(subject_n3)
            if not isinstance(subject, (URIRef, BNode)):
                raise ValueError("A typed answer record must be an RDF resource")
            model = by_type[cls]
            values: dict[str, Any] = {}
            terms = {}
            for name, info in model.model_fields.items():
                extra = info.json_schema_extra
                if not isinstance(extra, dict) or not extra.get("rdf_path"):
                    continue
                path = PropertyPath.model_validate(extra["rdf_path"])
                if path.operator != "predicate":
                    continue
                found = sorted(
                    graph.objects(subject, URIRef(path.iri or "")), key=lambda term: term.n3()
                )
                if found or name in loaded:
                    retained = [RdfTerm.from_rdf(term) for term in found]
                    values[name] = [_value(term) for term in retained]
                    terms[name] = [term.model_dump(mode="json") for term in retained]
            records.append(
                model.model_validate(
                    {
                        "@id": RdfTerm.from_rdf(subject).json_value(),
                        "@type": [cls],
                        **values,
                        "rdf_terms": terms,
                        "rdf_loaded_fields": list(values),
                        "rdf_source": {
                            "query_ids": sorted(
                                set(
                                    self.execution["binding_query_ids"]
                                    + self.execution["metadata_query_ids"]
                                )
                            ),
                            "coverage": self.coverage,
                        },
                    }
                )
            )
        return records

    def to_graph(self) -> Graph:
        """Return observed triples, without making a direct edge from a multi-hop path.

        Named graphs are combined into a subset graph. Original graph bindings
        remain in bindings. Schema labels used in the table are not added as data.
        """
        graph, _ = self._observed()
        return graph

    def connections(self, *, row: int | None = None) -> pd.DataFrame:
        """Return observed route evidence for all rows or one selected row."""
        from rdfsolve.connection_table import connection_table

        evidence = []
        indexes = range(len(self.bindings)) if row is None else [row]
        for index in indexes:
            binding = self.bindings[index]
            branch = self.execution["branches"][int(binding["_route"]["value"])]
            nodes = []
            for node in branch["nodes"]:
                labels = [
                    str(value)
                    for field in node["fields"]
                    if field["predicate"] in LABEL_PREDICATES
                    for value in self._values(binding, node, field)
                ]
                nodes.append(
                    {
                        "type": node["type"],
                        **_term(binding[node["variable"]]).model_dump(),
                        "label": labels[0] if labels else None,
                    }
                )
            evidence.append(
                {
                    "nodes": nodes,
                    "links": branch["edges"],
                    "graph": binding.get("_graph", {}).get("value"),
                    "query_id": self.execution["binding_query_ids"][index],
                }
            )
        return connection_table({**self.payload, "evidence": evidence, "coverage": self.coverage})

    def diagram(self, *, instances: bool = False, row: int | None = None) -> str:
        """Draw only paths observed by this final query; optionally select one row."""
        from rdfsolve.client_diagram import connection_diagram

        return connection_diagram(self.connections(row=row), instances=instances)
