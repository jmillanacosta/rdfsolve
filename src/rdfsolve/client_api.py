"""Explore RDF data with small, named sets of typed records."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterator
from html import escape
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from typing import Literal as FormatLiteral

import pandas as pd
from pydantic import BaseModel
from rdflib import Graph, Literal, URIRef

from rdfsolve.exploration import SEARCH_PREDICATES, DatasetClient
from rdfsolve.hydration import HydrationLimitError, _iri, _term
from rdfsolve.model_rdf import model_to_graph
from rdfsolve.query_log import QueryLog
from rdfsolve.registry import Registry
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

if TYPE_CHECKING:
    from rdfsolve.client_session import ClientSession


def _key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.casefold())


def _name(name: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name).replace("_", " ").capitalize()


def _name_fields(model: type[BaseModel]) -> list[str]:
    return [name for name in ("title", "label") if name in model.model_fields]


def _title(record: BaseModel) -> str:
    for field in _name_fields(type(record)):
        value = getattr(record, field)
        if value:
            return str(value[0] if isinstance(value, list) else value)
    return str(vars(record)["uri"])


class Client(DatasetClient):
    """Find records without first choosing their type, then explore their links."""

    @classmethod
    def open(
        cls,
        schema: MinedSchema | str | Path,
        source: str | SparqlHelper | Graph | None = None,
        *,
        format: FormatLiteral["json", "shacl", "void"] | None = None,
        data_file: str | Path | None = None,
        **kwargs: Any,
    ) -> Client:
        """Open a saved schema without mining or making source requests.

        JSON uses the canonical or VoID JSON-LD reader. For Turtle, choose
        format="shacl" or "void". RDF imports retain only supported fields.
        The source defaults to the schema endpoint. data_file selects local RDF.
        """
        if isinstance(schema, MinedSchema):
            if format is not None:
                raise ValueError("format applies to schema files, not MinedSchema objects")
        else:
            path = Path(schema)
            if format is None:
                if path.suffix.lower() not in {".json", ".jsonld"}:
                    raise ValueError('For Turtle, set format="shacl" or format="void"')
                format = "json"
            if format == "json":
                schema = MinedSchema.from_json(path)
            elif format == "shacl":
                schema = MinedSchema.from_shacl(path.read_text(encoding="utf-8"))
            elif format == "void":
                schema = MinedSchema.from_void(path.read_text(encoding="utf-8"))
            else:
                raise ValueError("Use json, shacl, or void as the schema format")
        if not schema.get_classes():
            raise ValueError("The schema has no classes to query; metadata alone is not enough")
        if data_file is not None:
            if source is not None:
                raise ValueError("Choose source or data_file, not both")
            if kwargs.get("graph_uris"):
                raise ValueError("A single local RDF graph has no named graph scope")
            source = Graph().parse(data_file)
            kwargs["graph_uris"] = []
        return cls(schema, source, **kwargs)

    def registry(self, *, source_id: str) -> Registry:
        """Build a versioned operation registry without querying the source."""
        from rdfsolve.rdf_operations import build_registry

        return build_registry(self, source_id)

    def session(
        self,
        *,
        source_id: str,
        preview_rows: int = 20,
        max_results: int = 50,
        max_records: int = 1000,
    ) -> ClientSession:
        """Run registered reads with retained results and a shared query log."""
        from rdfsolve.client_session import ClientSession

        return ClientSession(
            self,
            source_id=source_id,
            preview_rows=preview_rows,
            max_results=max_results,
            max_records=max_records,
        )

    def paths_between(
        self,
        source: str | BaseModel,
        target: str | None = None,
        *,
        target_value: str | None = None,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int = 1000,
    ) -> pd.DataFrame:
        """List class routes, or query connections to a matching record name.

        Two class names list schema routes without queries. A source record
        restricts observed paths to that record's IRI. Use target_value
        instead to verify mined class routes against records whose names or
        identifiers contain that text, ignoring case. Paths stay in one graph
        and do not repeat classes or resources. Class routes are queried in
        batches and bounded by max_paths. Raise rather than return partial paths.
        """
        from rdfsolve.client_paths import class_paths
        from rdfsolve.client_value_paths import value_paths

        if (target is None) == (target_value is None):
            raise ValueError("Supply either a target class or target_value")
        source_resource = None
        if isinstance(source, BaseModel):
            source_resource = getattr(source, "uri", None)
            if not isinstance(source_resource, str):
                raise ValueError("Use a client record with a resource IRI")
            _iri(source_resource)
            source = str(getattr(type(source), "rdf_class_iri", ""))
        if target_value is not None or source_resource is not None:
            source_class = str(getattr(self.model(source), "rdf_class_iri", ""))
            target_class = (
                str(getattr(self.model(target), "rdf_class_iri", ""))
                if target is not None
                else None
            )
            return value_paths(
                self,
                source_class,
                target_value,
                source_iri=source_resource,
                target_class=target_class,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
            )
        if target is None:
            raise ValueError("Supply a target class")

        return class_paths(
            self,
            source,
            target,
            max_hops=max_hops,
            both_directions=both_directions,
            max_paths=max_paths,
        )

    def connections(
        self,
        source: str | BaseModel,
        target: str | BaseModel | None = None,
        *,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int | None = None,
    ) -> pd.DataFrame:
        """Find actual paths between records, or around one record.

        Show every intermediate resource and link. Paths do not repeat resources
        and stay in one graph. Requests run in sequence. By default, stop at
        the client's row budget and return a marked partial view with a warning.
        Set max_paths explicitly to require a strict limit and raise on overflow.
        Set both_directions=False to follow outgoing links only.
        Without a target, return linked resources within max_hops. Do not follow
        rdf:type links or literal values; show classes as node annotations.
        """
        from rdfsolve.client_paths import resource_paths

        return resource_paths(
            self,
            source,
            target,
            max_hops=max_hops,
            both_directions=both_directions,
            max_paths=max_paths,
        )

    def diagram(
        self,
        *kinds: str,
        paths: pd.DataFrame | None = None,
        path: int | None = None,
        instances: bool = True,
    ) -> str:
        """Draw selected models or the selected rows of a paths table, without queries."""
        from rdfsolve.client_diagram import model_diagram, path_diagram

        if paths is not None:
            if kinds:
                raise ValueError("Choose model names or a paths table, not both")
            return path_diagram(self, paths, path=path, instances=instances)
        if path is not None:
            raise ValueError("Supply a paths table to choose a path")
        return model_diagram(self, kinds)

    def query_log(self) -> QueryLog:
        """Show every session query and its retained response without running it again."""
        return QueryLog(self.session_metadata())

    def model(self, name_or_iri: str) -> type[BaseModel]:
        """Accept generated names, spaced names, or full class IRIs."""
        matches = [
            model
            for name, model in self.models.items()
            if _key(name_or_iri)
            in {_key(name), _key(re.split(r"[/#:]", getattr(model, "rdf_class_iri", ""))[-1])}
            or getattr(model, "rdf_class_iri", "") == name_or_iri
        ]
        if len(matches) != 1:
            raise ValueError(f"Choose a type from data.types(): {name_or_iri}")
        return matches[0]

    def type_name(self, model: type[BaseModel]) -> str:
        """Display a source label without changing the generated type."""
        iri = getattr(model, "rdf_class_iri", "")
        labels = [
            item.text.value
            for item in self._schema.enrichment.labels
            if item.term_iri == iri and item.text.language in (None, "en")
        ]
        if labels:
            return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", min(labels))
        local = re.split(r"[/#:]", iri)[-1]
        return local if re.search(r"\\d", local) else _name(model.__name__)

    def link_name(self, model: type[BaseModel], field: str) -> str:
        """Use a source label or readable predicate name for a link."""
        extra = model.model_fields[field].json_schema_extra
        if isinstance(extra, dict):
            iri = str(extra.get("rdf_property_iri", ""))
            labels = [
                item.text.value
                for item in self._schema.enrichment.labels
                if item.term_iri == iri and item.text.language in (None, "en")
            ]
            if labels:
                return min(labels)
            if iri:
                return _name(re.split(r"[/#:]", iri)[-1])
        return _name(field)

    def field_name(self, model: type[BaseModel], text: str) -> str:
        """Resolve a field by its Python name or displayed source label."""
        matches = [
            name
            for name in model.model_fields
            if _key(text) in {_key(name), _key(self.link_name(model, name))}
        ]
        if len(matches) != 1:
            raise ValueError(f"Choose a field from results.fields: {text}")
        return matches[0]

    def from_table(
        self, kind: str, table: pd.DataFrame, *, id_column: str, **columns: str
    ) -> Results:
        """Create typed records from named table columns without querying."""
        model = self.model(kind)
        missing = {id_column, *columns.values()} - set(table.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        fields = {self.field_name(model, field): column for field, column in columns.items()}
        records = []
        for row in table.to_dict(orient="records"):
            iri = row[id_column]
            if not isinstance(iri, str):
                raise ValueError("The identifier column must contain IRIs")
            _iri(iri)
            records.append(
                model.model_validate(
                    {"uri": iri, **{field: row[column] for field, column in fields.items()}}
                )
            )
        return Results(self, records)

    def types(self) -> pd.DataFrame:
        """List available record types without sending a query."""
        return pd.DataFrame(
            {"Class": sorted(self.type_name(model) for model in self.models.values())}
        )

    @classmethod
    def from_session(
        cls, path: str | Path, *, data_file: str | Path | None = None, **kwargs: Any
    ) -> Client:
        """Reuse a saved session's schema. Do not replay its queries."""
        session = json.loads(Path(path).read_text(encoding="utf-8"))
        if data_file is not None:
            kwargs["source"] = Graph().parse(data_file)
            kwargs["graph_uris"] = []
        return cls(MinedSchema.from_dict(session["schema"]), **kwargs)

    def find(self, text: str, *, kind: str | None = None, field: str | None = None) -> Results:
        """Find names and identifiers across types. Optionally search a chosen field."""
        if not text.strip():
            raise ValueError("Enter a word or name to find")
        models = [self.model(kind)] if kind else list(self.models.values())
        by_iri = {vars(model)["rdf_class_iri"]: model for model in models}
        if not by_iri:
            return Results(self, [])
        classes = " ".join(_iri(iri) for iri in by_iri)
        predicates = SEARCH_PREDICATES
        if field is not None:
            predicates = {
                str(info.json_schema_extra["rdf_property_iri"])
                for model in models
                for name, info in model.model_fields.items()
                if _key(name) == _key(field)
                and isinstance(info.json_schema_extra, dict)
                and info.json_schema_extra.get("rdf_property_iri")
            }
            if not predicates:
                raise ValueError(f"No searchable field named {field}")
        labels = " ".join(_iri(iri) for iri in sorted(predicates))
        body = self._scope(
            f"VALUES ?type {{ {classes} }} VALUES ?p {{ {labels} }} "
            f"?s a ?type ; ?p ?label . FILTER(isIRI(?s) && !isBlank(?label) && "
            f"CONTAINS(LCASE(STR(?label)), LCASE({Literal(text).n3()})))"
        )
        groups: dict[str, set[str]] = defaultdict(set)
        records = []
        with self.step(f"Find {text}"):
            rows = self._select(
                f"SELECT DISTINCT ?s ?type WHERE {{ {body} }} LIMIT {self.max_rows + 1}"
            )
            if len(rows) > self.max_rows:
                raise HydrationLimitError("Too many matches. Enter a more specific name.")
            for row in rows:
                subject, cls = _term(row.get("s", {})), _term(row.get("type", {}))
                if subject.kind != "uri" or cls.kind != "uri" or cls.value not in by_iri:
                    raise EndpointError("Unexpected search result")
                groups[cls.value].add(subject.value)
            if len({iri for group in groups.values() for iri in group}) > self.max_subjects:
                raise HydrationLimitError("Too many matches. Enter a more specific name.")
            for iri, subjects in sorted(groups.items()):
                model = by_iri[iri]
                records.extend(self.get_many(model, sorted(subjects), fields=_name_fields(model)))
        return Results(self, records)

    def save(self, path: str | Path, *groups: Results) -> None:
        """Save selected records and direct links between them as RDF."""
        records = [record for group in groups for record in group]
        if any(group.client is not self for group in groups):
            raise ValueError("Save results from one client at a time")
        graph = Graph()
        ids = {str(vars(record)["uri"]) for record in records}
        for record in records:
            graph += model_to_graph(record)
        for link in self._matches:
            if link["source"] not in ids or link["target"] not in ids:
                continue
            path_model = PropertyPath.model_validate(link["path"])
            source, target = URIRef(link["source"]), URIRef(link["target"])
            if path_model.operator == "inverse":
                source, target = target, source
                path_model = path_model.items[0]
            if path_model.operator != "predicate" or path_model.iri is None:
                raise ValueError("Select the intermediate records before saving a multi-step link")
            graph.add((source, URIRef(path_model.iri), target))
        graph.serialize(destination=path, format="turtle")


class Results:
    """A reusable set of generated records. Display and completion do not query."""

    def __init__(self, client: Client, records: list[BaseModel]) -> None:
        """Keep the client and typed records together."""
        self.client = client
        self.records = records

    def __len__(self) -> int:
        """Return the number of typed records."""
        return len(self.records)

    def __iter__(self) -> Iterator[BaseModel]:
        """Iterate over the generated Pydantic objects."""
        return iter(self.records)

    def __getitem__(self, index: int) -> BaseModel:
        """Read a generated object, with its usual field completion."""
        return self.records[index]

    def __repr__(self) -> str:
        """Preview names without fetching more fields."""
        return f"{len(self)} matches\n" + self._table().head(20).to_string(index=False)

    def _repr_html_(self) -> str:
        note = " · showing the first 20" if len(self) > 20 else ""
        return f"<p>{len(self)} matches{note}</p>" + self._table().head(20).to_html(
            index=False, escape=True
        )

    def _table(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"Name": _title(record), "Class": self.client.type_name(type(record))}
                for record in self.records
            ],
            columns=["Name", "Class"],
        )

    def types(self) -> pd.DataFrame:
        """List the types found and their record counts."""
        groups: dict[type[BaseModel], list[BaseModel]] = defaultdict(list)
        for record in self.records:
            groups[type(record)].append(record)
        return pd.DataFrame(
            [
                {
                    "Class": self.client.type_name(model),
                    "Matches": len(records),
                    "Example": _title(records[0]),
                }
                for model, records in groups.items()
            ],
            columns=["Class", "Matches", "Example"],
        )

    def of_type(self, kind: str) -> Results:
        """Keep matches of one type without sending a query."""
        model = self.client.model(kind)
        return Results(self.client, [r for r in self.records if type(r) is model])

    def without(self, other: Results) -> Results:
        """Remove records with IRIs already present in another result set."""
        ids = {vars(record)["uri"] for record in other}
        return Results(self.client, [r for r in self.records if vars(r)["uri"] not in ids])

    @property
    def fields(self) -> SimpleNamespace:
        """Offer field names for tab completion, without fetching their values."""
        names = {
            name
            for record in self.records
            for name, info in type(record).model_fields.items()
            if isinstance(info.json_schema_extra, dict) and info.json_schema_extra.get("rdf_path")
        }
        aliases = {name: name for name in sorted(names)}
        for record in self.records:
            for name in names & type(record).model_fields.keys():
                label = re.sub(
                    r"\\W+", "_", self.client.link_name(type(record), name).lower()
                ).strip("_")
                if label.isidentifier() and label not in aliases:
                    aliases[label] = name
        return SimpleNamespace(**aliases)

    def _routes(self, incoming: bool) -> list[tuple[type[BaseModel], str, type[BaseModel]]]:
        routes = []
        models = {type(record) for record in self.records}
        for source in self.client.models.values() if incoming else models:
            for row in self.client.links(source).itertuples(index=False):
                target = self.client.model(str(row.target))
                if not incoming or target in models:
                    routes.append(
                        (target, str(row.field), source)
                        if incoming
                        else (source, str(row.field), target)
                    )
        return routes

    def paths(self, *, incoming: bool = False) -> pd.DataFrame:
        """Show the types and named links available from these records."""
        return pd.DataFrame(
            [
                {
                    "From": self.client.type_name(source),
                    "Link": self.client.link_name(target if incoming else source, field),
                    "To": self.client.type_name(target),
                }
                for source, field, target in self._routes(incoming)
            ],
            columns=["From", "Link", "To"],
        ).drop_duplicates()

    def related(
        self,
        kind: str | None = None,
        *,
        via: str | None = None,
        value: str | None = None,
        incoming: bool = False,
    ) -> Results:
        """Follow a link or an intermediate class, optionally matching a name or identifier.

        A class in via means two hops. A link name selects one direct link.
        Text is a case-insensitive substring, tested only on connected targets.
        """
        if kind is None and value is None:
            raise ValueError("Choose kind or value")
        if value is not None and not value.strip():
            raise ValueError("Enter a word or name to find")
        if via is not None:
            try:
                intermediate = self.client.model(via)
            except ValueError:
                intermediate = None
            if intermediate is not None:
                middle = self.related(intermediate.__name__, incoming=incoming)
                return middle.related(kind, value=value, incoming=incoming)
        if kind is None:
            targets = sorted({target.__name__ for _, _, target in self._routes(incoming)})
            records: list[BaseModel] = []
            for name in targets:
                records.extend(self.related(name, via=via, value=value, incoming=incoming))
            return Results(self.client, records)
        target = self.client.model(kind)
        routes = [
            (source, field)
            for source, field, dest in self._routes(incoming)
            if dest is target
            and (
                via is None
                or _key(via)
                in {_key(field), _key(self.client.link_name(dest if incoming else source, field))}
            )
        ]
        by_source: dict[type[BaseModel], set[str]] = defaultdict(set)
        for source, field in routes:
            by_source[source].add(field)
        if value is None and any(len(fields) > 1 for fields in by_source.values()):
            choices = sorted(
                {
                    self.client.link_name(target if incoming else source, field)
                    for source, field in routes
                }
            )
            raise ValueError(f"Choose one link with via= from: {choices}")
        if self.records and not routes:
            raise ValueError("No matching link. Use paths() to see where these records can lead.")
        found: dict[str, BaseModel] = {}
        with self.client.step(
            f"Read related {self.client.type_name(target)}"
            + (f" matching {value}" if value is not None else "")
        ):
            for source, fields in by_source.items():
                records = [record for record in self.records if type(record) is source]
                for field in sorted(fields):
                    for record in self.client.follow(
                        records,
                        field,
                        target,
                        inverse=incoming,
                        fields=_name_fields(target),
                        value=value,
                    ):
                        found[str(vars(record)["uri"])] = record
        return Results(self.client, list(found.values()))

    def _load(self, field: str) -> None:
        if field == "uri":
            return
        models = {type(record) for record in self.records}
        names = {model: self.client.field_name(model, field) for model in models}
        with self.client.step(f"Read {field}"):
            for model, name in names.items():
                group = [
                    r
                    for r in self.records
                    if type(r) is model
                    and name not in vars(r).get("rdf_loaded_fields", [])
                    and name not in r.model_fields_set
                ]
                if not group:
                    continue
                selected = set(_name_fields(model)) | {name}
                selected.update(
                    field for r in group for field in vars(r).get("rdf_loaded_fields", [])
                )
                loaded = self.client.get_many(
                    model, [str(vars(r)["uri"]) for r in group], fields=sorted(selected)
                )
                replacements = {vars(r)["uri"]: r for r in loaded}
                self.records = [
                    replacements[vars(r)["uri"]] if type(r) is model else r for r in self.records
                ]

    def show(self, *fields: str) -> pd.DataFrame:
        """Show names and requested fields, retrieving those fields if needed."""
        for field in fields:
            self._load(field)
        rows = []
        for record in self.records:
            row: dict[str, Any] = {
                "Name": _title(record),
                "Class": self.client.type_name(type(record)),
            }
            for field in fields:
                name = self.client.field_name(type(record), field)
                value = getattr(record, name)
                row[_name(field)] = (
                    " | ".join(map(str, value)) if isinstance(value, list) else value
                )
            rows.append(row)
        return pd.DataFrame(rows, columns=["Name", "Class", *(_name(f) for f in fields)])

    def values(self, field: str) -> pd.DataFrame:
        """List values of one field across these records, not the whole endpoint."""
        self._load(field)
        values: dict[str, set[str]] = defaultdict(set)
        for record in self.records:
            name = self.client.field_name(type(record), field)
            terms = vars(record).get("rdf_terms", {}).get(name)
            if terms is None:
                from rdfsolve.schema_models.enrichment import RdfTerm

                extra = type(record).model_fields[name].json_schema_extra
                if not isinstance(extra, dict) or not extra.get("rdf_property_iri"):
                    raise ValueError("No direct RDF field definition")
                graph = model_to_graph(record, fields=[name])
                terms = [
                    RdfTerm.from_rdf(value).model_dump(mode="json")
                    for value in graph.objects(
                        URIRef(vars(record)["uri"]), URIRef(str(extra["rdf_property_iri"]))
                    )
                ]
            for term in terms:
                values[json.dumps(term, sort_keys=True)].add(str(vars(record)["uri"]))
        rows = []
        for raw, subjects in values.items():
            term = json.loads(raw)
            rows.append(
                {
                    "Value": term["value"],
                    "Records": len(subjects),
                    "Language": term.get("language"),
                    "Datatype": term.get("datatype"),
                }
            )
        result = pd.DataFrame(rows, columns=["Value", "Records", "Language"])
        result.attrs["rdf_terms"] = [json.loads(raw) for raw in values]
        return result.dropna(axis=1, how="all")


def explore(endpoint: str, *, graph: str | None = None, timeout: float = 30) -> Client:
    """Read an endpoint's schema and open a query-recording client."""
    from rdfsolve.miner import SchemaMiner

    miner = SchemaMiner(
        endpoint,
        graph_uris=[graph] if graph else None,
        counts=False,
        enrich=True,
        examples_per_pattern=0,
        timeout=timeout,
    )
    miner.helper.enable_query_collection(include_results=True)
    try:
        schema = miner.mine()
        if miner.last_report is None or miner.last_report.completion_state != "complete":
            raise EndpointError("Could not finish reading the available fields")
        client = Client(schema, miner.helper, max_subjects=500)
        client._owns_helper = True
        return client
    except BaseException:
        miner.close()
        raise
