"""Explore RDF data with small, named sets of typed records."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict
from functools import cached_property
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from typing import Literal as FormatLiteral

import pandas as pd
import pyoxigraph as ox
from pydantic import BaseModel
from rdflib import BNode, Dataset, Graph, Literal, URIRef

from rdfsolve._uri import curie_from_prefixes, prefix_map, uri_to_curie
from rdfsolve.client.exploration import DatasetClient
from rdfsolve.client.hydration import _iri, _term, class_iri, field_metadata
from rdfsolve.client.model_rdf import model_to_graph
from rdfsolve.client.query_log import QueryLog
from rdfsolve.client.registry import Registry
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import LABEL_PREDICATES, NAME_PREDICATES, SYNONYM_PREDICATES
from rdfsolve.schema_models.paths import PropertyPath
from rdfsolve.sparql_helper import EndpointError, SparqlHelper

if TYPE_CHECKING:
    from rdfsolve.client.catalogue import Catalogue
    from rdfsolve.client.extraction import Extraction
    from rdfsolve.client.identify import Identification
    from rdfsolve.client.query import QueryResult
    from rdfsolve.client.query_fragments import PreparedQuery, QueryPattern
    from rdfsolve.client.resolution import Resolution
    from rdfsolve.ontology import Ontologies, Term
    from rdfsolve.property_graph import Conversion, Fold, Identity, PropertyGraph
    from rdfsolve.schema_models.selection import SchemaSelection


def _key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.casefold())


def _curie_key(iri: str, prefixes: dict[str, str]) -> str:
    """Return the CURIE of a property without punctuation: ``foaf:name`` gives ``foafname``."""
    found = curie_from_prefixes(iri, prefixes) or uri_to_curie(iri)
    return re.sub(r"[^A-Za-z0-9]", "", found[0])


def _namespace(iri: str) -> str:
    """Return the IRI up to its last ``/``, ``#`` or ``:``."""
    return re.sub(r"[^/#:]*$", "", iri)


def _name(name: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name).replace("_", " ").capitalize()


def _name_fields(model: type[BaseModel]) -> list[str]:
    priority = [*LABEL_PREDICATES[2:], *LABEL_PREDICATES[:2], *SYNONYM_PREDICATES]
    fields = {
        name: field_metadata(info).get("rdf_property_iri", "")
        for name, info in model.model_fields.items()
    }
    return sorted(
        (name for name, iri in fields.items() if iri in NAME_PREDICATES),
        key=lambda name: priority.index(fields[name]),
    )


def _title(record: BaseModel) -> str:
    for field in _name_fields(type(record)):
        iri = field_metadata(type(record).model_fields[field]).get("rdf_property_iri", "")
        if SYNONYM_PREDICATES.get(iri) in {"broad", "narrow", "related"}:
            continue
        value = getattr(record, field)
        if value:
            return str(value[0] if isinstance(value, list) else value)
    return str(vars(record)["uri"])


class Client(DatasetClient):
    """Find records and types, then explore their links."""

    def __init__(
        self,
        *args: Any,
        ontology_grounding: bool | Ontologies = False,
        source_id: str = "rdf",
        class_mappings: Iterable[Any] = (),
        related_registries: Iterable[Registry] = (),
        **kwargs: Any,
    ) -> None:
        """Open typed retrieval with optional, separately retained ontology evidence."""
        super().__init__(*args, **kwargs)
        from rdfsolve.ontology import Ontologies

        self.ontology: Ontologies | None
        if ontology_grounding is True:
            self.ontology = Ontologies()
        elif not ontology_grounding:
            self.ontology = None
        elif isinstance(ontology_grounding, Ontologies):
            self.ontology = ontology_grounding
        else:
            raise TypeError("ontology_grounding must be a boolean or Ontologies")
        self.vocabulary_evidence: dict[str, Term | None] = {}
        self.description_lookups: list[dict[str, Any]] = []
        self.resolutions: list[dict[str, Any]] = []
        self.source_id = source_id
        self.class_mappings = tuple(class_mappings)
        self.related_registries = tuple(related_registries)
        self._prepared: dict[str, tuple[PreparedQuery, Any, tuple[str, ...]]] = {}
        self._field_names: dict[tuple[type[BaseModel], str], str] = {}

    @cached_property
    def catalogue(self) -> Catalogue:
        """Retain this client's schema, mappings, entities and executable paths."""
        from rdfsolve.client.catalogue import Catalogue

        return Catalogue(
            self,
            self.source_id,
            class_mappings=self.class_mappings,
            related_registries=self.related_registries,
        )

    @property
    def schema(self) -> MinedSchema:
        """Give the mined schema of this client: its patterns, collections and metadata."""
        return self._schema

    def vocabulary(self, iri: str) -> Term | None:
        """Explain a local vocabulary IRI through the configured ontology provider.

        External labels, definitions and named parents remain an evidence overlay.
        The mined schema, generated fields and source scope remain authoritative.
        """
        if iri not in self.vocabulary_evidence and self.ontology:
            evidence = self.ontology.lookup(iri)
            if evidence is not None or self.ontology.events[-1]["status"] == "not_found":
                self.vocabulary_evidence[iri] = evidence
        return self.vocabulary_evidence.get(iri)

    def _vocabulary_names(self, iri: str) -> set[str]:
        term = self.vocabulary_evidence.get(iri)
        names = {v.casefold() for v in [term["label"], *term["synonyms"]]} if term else set()
        return names | {
            item.text.value.casefold()
            for item in self._schema.enrichment.labels
            if item.term_iri == iri
            and (
                item.predicate in LABEL_PREDICATES
                or SYNONYM_PREDICATES.get(item.predicate) in {"exact", "alternative"}
            )
        }

    def close(self) -> None:
        """Close source and ontology connections."""
        super().close()
        if self.ontology:
            self.ontology.close()

    def session_metadata(self, *, include_results: bool = True) -> dict[str, Any]:
        """Retain external evidence separately from the mined schema and source journal."""
        data = super().session_metadata(include_results=include_results)
        if self.ontology:
            data["ontology"] = {
                **self.ontology.diagnostics(),
                "events": self.ontology.events,
                "evidence": self.vocabulary_evidence,
                "queries": [dict(vars(q)) for q in self.ontology.helper.get_collected_queries()]
                if self.ontology.helper
                else [],
            }
        data["description_lookups"] = self.description_lookups
        data["resolutions"] = self.resolutions
        data["prepared_queries"] = {ref: asdict(q) for ref, (q, _, _) in self._prepared.items()}
        return data

    @classmethod
    def open(
        cls,
        schema: MinedSchema | str | Path,
        source: str | SparqlHelper | Graph | ox.Dataset | ox.Store | None = None,
        *,
        format: FormatLiteral["json", "shacl", "void"] | None = None,
        data_file: str | Path | Sequence[str | Path] | None = None,
        **kwargs: Any,
    ) -> Client:
        """Open a saved schema without mining or making source requests.

        JSON uses the canonical or VoID JSON-LD reader. For Turtle, choose
        format="shacl" or "void". RDF imports retain only supported fields.
        The source defaults to the schema endpoint. data_file selects local RDF: one file or
        several (the dumps of one release), read into one store; or the name of a registry
        entry, whose RDF downloads are fetched once (local_rdf.registry_files) and read together.
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
                schema = MinedSchema.from_void(
                    path.read_text(encoding="utf-8"),
                    local_backend=kwargs.get("local_backend", "oxigraph"),
                )
            else:
                raise ValueError("Use json, shacl, or void as the schema format")
            _add_release(schema, path)
        if not schema.get_classes():
            raise ValueError("The schema has no classes to query; metadata alone is not enough")
        if data_file is not None:
            if source is not None:
                raise ValueError("Choose source or data_file, not both")
            from rdfsolve.local_rdf import load_store, registry_files

            if isinstance(data_file, str) and not Path(data_file).exists():
                data_file = registry_files(data_file)
            store = load_store(data_file)
            if next(iter(store.named_graphs()), None) is None:
                if kwargs.get("graph_uris"):
                    raise ValueError("A single local RDF graph has no named graph scope")
                kwargs["graph_uris"] = []
            source = store
        return cls(schema, source, **kwargs)

    def registry(self, *, source_id: str) -> Registry:
        """Describe generated classes and source bindings from retained metadata."""
        from rdfsolve.client.registry import build_registry

        return build_registry(self, source_id)

    def describe(
        self,
        concept: str | Literal = "",
        *,
        owners: Iterable[str] = (),
        targets: Iterable[str] = (),
        source: bool = True,
        identifier: str | None = None,
        ontology_fallback: bool = False,
    ) -> pd.DataFrame:
        """Find schema entries and indexed literal matches in the selected source.

        Strings try supplied, lower, upper and title case as plain literals.
        An RDF Literal retains its language and datatype for exact matching.
        identifier restricts source subjects to an IRI or registered CURIE.
        Source matches retain identity, types, literal and graph evidence.
        Set source=False for schema-only inspection. Target filters select schema
        fields only. ontology_fallback verifies externally named classes in the
        selected source when no class label matches. External names and source
        checks remain separate evidence in the table and saved session.
        """
        from rdfsolve.client.description import describe

        return describe(
            self, concept, tuple(owners), tuple(targets), source, identifier, ontology_fallback
        )

    def resolve(
        self,
        concept: str,
        *,
        kind: str = "resource",
        identifier: str | None = None,
        external_names: bool = False,
    ) -> Resolution:
        """Resolve a name, IRI or CURIE into a class or exact-resource constraint.

        kind="class" constrains nodes typed with the class; kind="resource"
        matches one exact RDF term. Every candidate from schema labels, source
        labels, registered identifiers and (with external_names) external
        ontology names is checked in the selected graphs. Nothing is chosen by
        precedence: two eligible candidates give status "ambiguous". Read notes
        for the meaning of the constraint and warnings for incomplete evidence.
        """
        from rdfsolve.client.resolution import resolve_term

        result = resolve_term(
            self, concept, kind=kind, identifier=identifier, external_names=external_names
        )
        self.resolutions.append(result.model_dump(mode="json"))
        return result

    def resolve_many(
        self, concepts: Iterable[str], *, kind: str = "resource", batch: int = 500, sample: int = 3
    ) -> dict[str, Resolution]:
        """Resolve many IRIs or CURIEs at once, as :meth:`resolve` does each.

        The IRI forms the source uses are learned from *sample* identifiers of each namespace
        and applied to the others, so a few queries resolve many identifiers (a name that is
        not an IRI or CURIE is resolved on its own).
        """
        from rdfsolve.client.resolution import resolve_terms

        results = resolve_terms(self, concepts, kind=kind, batch=batch, sample=sample)
        self.resolutions.extend(r.model_dump(mode="json") for r in results.values())
        return results

    def fetch(self, identifiers: Iterable[str], *fields: str, kind: str | None = None) -> Results:
        """Return the records that the identifiers (IRIs or CURIEs) name here, with their values.

        Each is resolved to the IRI this source writes (resolve_many). *kind* is by default the
        class this source gives the identifiers it issues (issued_kinds: owl:Class for ChEBI,
        up:Protein for UniProt). *fields* are by default the record's literal values and its
        cross-references (see cross_references), so that to_oxigraph gives them.
        """
        if kind is None:
            issued = sorted({c for classes in self.issued_kinds().values() for c in classes})
            if len(issued) != 1:
                raise ValueError(f"Give kind=: this source issues {issued or 'no kind'}")
            kind = issued[0]
        found = self.resolve_many(identifiers)
        iris = sorted({r.iri for r in found.values() if r.iri})
        records = self.from_table(kind, pd.DataFrame({"iri": iris}), id_column="iri")
        return records.load(*(fields or self.record_fields(kind)))

    def naming(self, identifiers: Iterable[str], *, kind: str, via: str) -> Results:
        """Return the records of *kind* whose link *via* names one of the identifiers.

        Any registered IRI form is matched: a source cites an identifier in its own form, not
        in the one asked for. links() gives each identifier, as given, its records.
        """
        from rdfsolve.client.exploration import _path
        from rdfsolve.identifiers import candidates
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        model = self.model(kind)
        path = path_to_sparql(_path(model, self.field_name(model, via)))
        forms = {form: given for given in identifiers for form in candidates(given)[0]}
        names, pairs = sorted(forms), set()
        with self.step(f"Read {self.type_name(model)} naming the identifiers"):
            for start in range(0, len(names), self.batch_size):
                values = " ".join(_iri(name) for name in names[start : start + self.batch_size])
                body = self._scope(
                    f"VALUES ?named {{ {values} }} ?record {path} ?named . "
                    + self._type_pattern("?record", _iri(class_iri(model)))
                )
                rows = self._select(f"SELECT DISTINCT ?record ?named WHERE {{ {body} }}")
                pairs |= {(r["record"]["value"], forms[r["named"]["value"]]) for r in rows}
            records = self.get_many(
                model, sorted({r for r, _ in pairs}), fields=_name_fields(model)
            )
        return Results(
            self,
            records,
            evidence=[{"source": given, "target": r, "via": via} for r, given in sorted(pairs)],
        )

    def record_fields(self, kind: str) -> list[str]:
        """Return the fields of a class that hold its values: literals and cross-references."""
        crossing = set(self.cross_references())
        names = []
        for name, info in self.model(kind).model_fields.items():
            extra = info.json_schema_extra
            if not isinstance(extra, dict):
                continue
            patterns: list[dict[str, Any]] = list(extra.get("rdf_patterns") or [])  # type: ignore[arg-type]
            if extra.get("rdf_property_iri") in crossing or any(
                p.get("pattern_type") == "datatype_property" for p in patterns
            ):
                names.append(name)
        return names

    def cross_references(self) -> list[str]:
        """Return the properties of this source that cite identifiers of one other source.

        From the mined schema: every identifier value of the property is in one registered
        namespace that this source does not issue (WikiPathways' bdbChEBI, bdbUniprot), and some
        of its values are resources the source does not describe (object class Resource). An
        invalid identifier counts in its namespace (lipidmaps/LMSP02 is a LIPID MAPS id that
        fails the pattern). Not cross-references: properties whose values span namespaces
        (dcterms:isPartOf, rdfs:seeAlso), or whose values the source describes itself
        (dcterms:references to its PublicationReference records).
        """
        from rdfsolve.identifiers import parse
        from rdfsolve.mappings.signatures import RDF_TYPE, VOCABULARIES

        own = set(self.issued_kinds())
        spaces: dict[str, set[str]] = defaultdict(set)
        for example in self._schema.enrichment.examples if self._schema.enrichment else []:
            if example.property_uri != RDF_TYPE:
                found = parse(example.value.value)
                prefix = found.prefix if found else "-"
                spaces[example.property_uri].add("-" if prefix in VOCABULARIES else prefix)
        cited = {p.property_uri for p in self._schema.patterns if p.object_class == "Resource"}
        return sorted(
            p
            for p, found in spaces.items()
            if len(found) == 1 and not found & (own | {"-"}) and p in cited
        )

    def identify(self, identifiers: Iterable[str]) -> list[Identification]:
        """Find the resources that carry each identifier (CURIE or IRI) in the selected graphs.

        Every registered spelling is tried: IRIs, IRIs written as strings, the bare
        identifier and its case variants. Each match says which property carries it and
        how the source writes it. A match that links to another match (a qualified
        statement, for instance) keeps the other as an intermediate.
        """
        from rdfsolve.client.identify import identify

        return identify(self, identifiers)

    def statements(
        self, iris: Iterable[str], *, languages: Iterable[str] = (), names: bool = True
    ) -> Graph:
        """Read what the source states about these resources, with names of what they link to.

        languages keeps language-tagged literals in those languages (untagged ones always).
        names=False leaves out the names of what they link to (one query less per batch of
        values, and many pages less when names are in all languages).
        """
        from rdfsolve.client.identify import statements

        return statements(self, iris, languages, names=names)

    def prepare(
        self,
        sparql: str,
        *,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
        output_variables: Sequence[str] = (),
    ) -> PreparedQuery:
        """Prepare a scoped retrieval query using this client's retained evidence.

        Class, field, path and entity references returned by discovery expand
        here. Optional requirements describe outputs, relationships and filters;
        all queries are checked for source scope, field ownership and joins.
        output_variables requires the specified column names. Preparation makes
        no source request. Pass the artifact to select().
        """
        from rdfsolve.client.query_fragments import compile_query, identifier
        from rdfsolve.client.retrieval import verify_query

        entries = (
            requirements.items()
            if isinstance(requirements, dict)
            else ((f"g{i}", value) for i, value in enumerate(requirements, 1))
        )
        goals = {key: self.catalogue.requirement(value, output_variables) for key, value in entries}
        if grounding and not set(grounding) <= goals.keys():
            raise ValueError("Grounding must refer to a supplied requirement")
        with self.step("Prepare retrieval query"):
            query = verify_query(
                sparql,
                goals,
                grounding or {},
                self.catalogue,
                output_variables=output_variables,
            )
        if self._schema.about.type_context_graph_uris or self._schema.about.type_graph_uris:
            scoped = compile_query(
                sparql,
                self.catalogue.fragments,
                self._scope,
                self.catalogue.known_iris,
                type_pattern=self._type_pattern,
            )
            query.sparql, query.uses = scoped.sparql, scoped.uses
        query.sparql = self._scope_query(query.sparql)
        query.ref = identifier("q", query.sparql)
        self._prepared[query.ref] = query, self.source, tuple(self.graph_uris)
        return query

    def navigation(self, *, max_hops: int = 6, observed_only: bool = False) -> pd.DataFrame:
        """Retrieve retained elongated routes and their snapshot support summaries."""
        from rdfsolve.client.query_fragments import Fragment

        summary = self._schema.navigation
        rows = []
        for route in summary.paths if summary else []:
            if len(route.steps) > max_hops or (
                observed_only and route.instance_support != "matched"
            ):
                continue
            fragment = Fragment(
                "path",
                route.label(),
                path=route.property_path(),
                basis="mined joined path",
            )
            for i, step in enumerate(route.steps):
                target: str | None = step.object_class
                if target in {"Literal", "Resource", "BlankNode"}:
                    fragment.term_kinds[i + 1] = (target, step.datatype)
                    target = None
                fragment.steps.append((step.subject_class, step.property_uri, target, False))
            ref = self.catalogue._put(fragment, route.signature())
            self.catalogue.metadata[ref] = {
                "status": route.instance_support,
                "source_count": route.source_count,
                "matched_sources": route.matched_sources,
                "observed_at": route.observed_at,
            }
            rows.append(
                {
                    "Reference": ref,
                    "Label": route.label(),
                    "Hops": len(route.steps),
                    "Source": route.steps[0].subject_class,
                    "Target": route.steps[-1].object_class,
                    "Support": route.instance_support,
                    "Sources": route.source_count,
                    "Matched": route.matched_sources,
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "Reference",
                "Label",
                "Hops",
                "Source",
                "Target",
                "Support",
                "Sources",
                "Matched",
            ],
        )

    def prepare_network(
        self,
        patterns: Sequence[QueryPattern | dict[str, Any]],
        *,
        outputs: Sequence[str],
        values: Mapping[str, str] | None = None,
        text: Mapping[str, str] | None = None,
        distinct: bool = True,
        resolve: bool = False,
        external_names: bool = False,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
    ) -> PreparedQuery:
        """Compose connected class, field and path selections into one verified query.

        Each QueryPattern names retained evidence and its roles. Reuse a role to
        share a node; expose every path port to constrain an intermediate record.
        Optional descendants remain inside their parent's optional scope.
        resolve=True accepts class names, IRIs or CURIEs (one binding) and field
        names (two bindings). Unresolved or ambiguous classes raise ResolutionError.
        external_names=True also considers external ontology class names.
        The supplied bindings define the network; resolution does not add edges.
        Warnings state constraints the resolution added, such as a field owner type.
        diagnostics["network"] lists each role's classes and each field or path link.
        """
        from rdfsolve.client.network_resolution import describe_network, resolve_patterns
        from rdfsolve.client.query_fragments import network_query

        evidence: list[dict[str, Any]] = []
        warnings: list[str] = []
        if external_names and not resolve:
            raise ValueError("external_names requires resolve=True")
        if resolve:
            patterns, evidence, warnings = resolve_patterns(self, patterns, external_names)
        query = network_query(
            self.catalogue, patterns, outputs, values=values, text=text, distinct=distinct
        )
        prepared = self.prepare(
            query, requirements=requirements, grounding=grounding, output_variables=outputs
        )
        prepared.diagnostics["resolutions"] = evidence
        prepared.diagnostics["network"] = describe_network(self, patterns)
        prepared.warnings.extend(w for w in warnings if w not in prepared.warnings)
        return prepared

    def prepare_path(
        self,
        path: str,
        *,
        source: str = "source",
        target: str = "target",
        fields: dict[str, list[str]] | None = None,
        requirements: Mapping[str, Any] | Iterable[Any] = (),
        grounding: Mapping[str, dict[str, Any]] | None = None,
        output_variables: Sequence[str] = (),
    ) -> PreparedQuery:
        """Prepare a retained route, with optional fields on either endpoint.

        source and target name the result columns. fields maps either column to
        generated field names or unique labels. Entity anchors and graph scope
        come from the selected route. Preparation makes no source request.
        """
        from rdfsolve.client.query_fragments import path_query

        text = path_query(self.catalogue, path, source, target, fields or {})
        return self.prepare(
            text,
            requirements=requirements,
            grounding=grounding,
            output_variables=output_variables,
        )

    def retrieve(
        self,
        path: str,
        *,
        source: str = "source",
        target: str = "target",
        fields: dict[str, list[str]] | None = None,
        limit: int | None = None,
    ) -> QueryResult:
        """Retrieve a retained route and its available endpoint fields.

        Return exact RDF cells with their row associations. Supply limit for a
        bounded sample; the default retrieves all rows through the shared helper.
        """
        query = self.prepare_path(path, source=source, target=target, fields=fields)
        return self.select(query, exhaustive=limit is None, limit=limit)

    def select(
        self, query: PreparedQuery | str, *, exhaustive: bool = False, limit: int | None = None
    ) -> QueryResult:
        """Execute SELECT through this client's shared helper and return typed cells.

        Accept a prepared artifact from this client, or ordinary SPARQL with
        its scope already present. The returned QueryResult preserves the query's row associations and RDF terms.
        exhaustive=True uses the helper's adaptive pager until an empty page.
        limit bounds a probe without changing the prepared artifact.
        Endpoint execution uses the existing SparqlHelper.select_with_fallback.
        """
        from time import perf_counter

        from rdflib.plugins.sparql import prepareQuery

        from rdfsolve.client.query import QueryResult, ResultCell
        from rdfsolve.client.query_fragments import PreparedQuery, identifier

        if isinstance(query, PreparedQuery):
            retained = self._prepared.get(query.ref)
            if (
                retained is None
                or retained[0] is not query
                or retained[1] is not self.source
                or retained[2] != tuple(self.graph_uris)
                or query.ref != identifier("q", query.sparql)
            ):
                raise ValueError(
                    "Prepare this query with the current client and source scope before execution"
                )
            query = query.sparql
        if limit is not None:
            if type(limit) is not int or limit < 1:
                raise ValueError("Use a positive integer probe limit")
            query += f"\nLIMIT {limit}"
        query = self._scope_query(query)
        parsed = prepareQuery(query)
        if parsed.algebra.name != "SelectQuery":
            raise ValueError("Client.select requires a SELECT query")
        variables = [str(v) for v in parsed.algebra.PV]
        started = perf_counter()
        bindings = self._select(query, exhaustive=True) if exhaustive else self._select(query)
        rows: list[dict[str, ResultCell]] = []
        for binding in bindings:
            row: dict[str, ResultCell] = {}
            for name, cell in binding.items():
                term = _term(cell)
                row[name] = ResultCell(
                    value=term.value, type=term.kind, lang=term.language, datatype=term.datatype
                )
            rows.append(row)
        return QueryResult(
            query=query,
            endpoint=self.source.endpoint_url if isinstance(self.source, SparqlHelper) else "",
            variables=variables,
            rows=rows,
            row_count=len(rows),
            duration_ms=round((perf_counter() - started) * 1000),
        )

    def superclasses(self, classes: Iterable[str]) -> dict[str, set[str]]:
        """Return all named superclasses of each class, as this source's endpoint states them.

        Read from the source's own vocabulary (rdfs:subClassOf, by levels): WikiPathways states
        wp:Protein under wp:GeneProduct under wp:DataNode. A class the endpoint says nothing
        about has none.
        """
        from rdfsolve.ontology.hierarchy import fetch_superclasses

        if not isinstance(self.source, SparqlHelper):
            return {c: set() for c in classes}
        parents = fetch_superclasses(self.source, classes, graph_uris=list(self.graph_uris) or None)
        out: dict[str, set[str]] = {}
        for start in classes:
            seen: set[str] = set()
            frontier = list(parents.get(start, ()))
            while frontier:
                current = frontier.pop()
                if current not in seen:
                    seen.add(current)
                    frontier.extend(parents.get(current, ()))
            out[start] = seen
        return out

    def construct(self, query: str, *, data: ox.Dataset | ox.Store | None = None) -> ox.Dataset:
        """Run a SPARQL CONSTRUCT on this client's endpoint or local RDF; return the statements.

        The query is recorded in the session like any other (the log can rebuild a product
        from its CONSTRUCTs, such as Fold.to_construct). A client scoped to several named
        graphs is refused: the scope would have to be written into the query's WHERE.

        *data* runs it on a workflow's own statements instead: a choice that changes them, or a
        check of them, is then in the session too.
        """
        from rdfsolve.local_rdf import to_oxigraph

        if data is not None:
            return self._construct_local(query, data=data)
        if self.is_local:
            return self._construct_local(query)
        if not isinstance(self.source, SparqlHelper):
            raise ValueError("CONSTRUCT needs a SPARQL endpoint or local RDF")
        context = (self._schema.about.type_graph_uris or []) + (
            self._schema.about.type_context_graph_uris or []
        )
        if len(self.graph_uris) > 1 or (self.graph_uris and context):
            raise ValueError("Write the graph scope into the CONSTRUCT (FROM or GRAPH) yourself")
        return to_oxigraph(self.source.construct_graph(query))

    def _construct_local(
        self, query: str, *, data: ox.Dataset | ox.Store | None = None
    ) -> ox.Dataset:
        """Run a CONSTRUCT on the client's local RDF (or on *data*), recorded in the session."""
        import time

        from rdfsolve.local_rdf import to_oxigraph
        from rdfsolve.sparql_helper import QueryRecord

        if data is None and self._local_rdf is None:
            raise RuntimeError("Local RDF backend is not initialized")
        if data is None:
            query = self._scope_query(query)
        self.queries.append(query)
        record = QueryRecord(query, "CONSTRUCT", "", success=False, purpose="construct")
        if isinstance(self.source, SparqlHelper):
            self.source._record_query(record)
        else:
            self._local_records.append(record)
        started = time.monotonic()
        try:
            if data is not None:
                store = data if isinstance(data, ox.Store) else ox.Store()
                if store is not data:
                    store.extend(data)
                triples = store.query(query)
                if not isinstance(triples, ox.QueryTriples):
                    raise ValueError("Give a CONSTRUCT query")
                found = ox.Dataset(ox.Quad(t.subject, t.predicate, t.object) for t in triples)
                record.success = True
                return found
            graph = self._local_rdf.query(query).graph  # type: ignore[union-attr]
            record.success = True
        except Exception as error:
            record.error = type(error).__name__
            raise
        finally:
            record.elapsed_seconds = time.monotonic() - started
        return to_oxigraph(graph if graph is not None else Graph())

    def extract(
        self,
        selection: SchemaSelection,
        *,
        root_class: str | type[BaseModel],
        roots: list[str] | None = None,
    ) -> Extraction:
        """Retrieve selected connected fields, list cells and original graph evidence."""
        from rdfsolve.client.extraction import extract

        model = self.model(root_class) if isinstance(root_class, str) else root_class
        self._check_model_scope(model)
        return extract(self, selection, class_iri(model), roots)

    def field_values(
        self, kind: str, field: str, *, text: str = "", limit: int = 8, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Sample actual subject/value pairs through a generated field's full path.

        Request limit+1 to detect another page. Blank-node identifiers belong
        to the response that supplied them.
        """
        from rdfsolve.client.exploration import _path
        from rdfsolve.schema_models.exporters.paths import path_to_sparql

        if type(limit) is not int or not 1 <= limit <= self.max_rows or offset < 0:
            raise ValueError("Use a positive bounded limit and nonnegative offset")
        model = self.model(kind)
        name = self.field_name(model, field)
        path = _path(model, name)
        pattern = (
            self._type_pattern("?subject", _iri(class_iri(model)))
            + f" ?subject {path_to_sparql(path)} ?value ."
        )
        if text:
            pattern += f" FILTER(!isBlank(?value) && CONTAINS(LCASE(STR(?value)), LCASE({Literal(text).n3()})))"
        return self._select(
            f"SELECT DISTINCT ?subject ?value WHERE {{ {self._scope(pattern)} }} "
            f"ORDER BY ?subject ?value LIMIT {limit} OFFSET {offset}"
        )

    def paths_between(
        self,
        source: str | BaseModel | Results,
        target: str | BaseModel | Results | None = None,
        *,
        target_value: str | None = None,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int = 1000,
        allow_partial: bool = False,
        allow_repeated_classes: bool = False,
        meaning: str = "",
        via: tuple[str, ...] = (),
    ) -> pd.DataFrame:
        """Discover schema routes or evaluate paths for selected typed records.

        Two class names use retained model fields without querying. A record or
        Results on either side evaluates paths for those exact identities in each
        selected data scope. target_value searches names with find first. Results
        preserve their whole selected scope; no example record is substituted.
        allow_partial returns bounded evidence with explicit coverage.
        """
        from rdfsolve.client.paths import class_paths
        from rdfsolve.client.value_paths import value_paths

        if (target is None) == (target_value is None):
            raise ValueError("Supply either a target class or target_value")
        if isinstance(source, str) and isinstance(target, str):
            table = class_paths(
                self,
                source,
                target,
                max_hops=max_hops,
                both_directions=both_directions,
                max_paths=max_paths,
                allow_partial=allow_partial,
                allow_repeated_classes=allow_repeated_classes,
                meaning=meaning,
                via=via,
            )
            self.catalogue.retain_paths(table)
            return table

        def endpoints(value: Any) -> dict[str, list[str] | None]:
            """Group class names, records or results by their RDF class."""
            if isinstance(value, str):
                return {class_iri(self.model(value)): None}
            records = Results(self, [value]) if isinstance(value, BaseModel) else value
            if not isinstance(records, Results) or records.client is not self:
                raise ValueError(
                    "Use a class name, a generated record, or Results from this Client"
                )
            groups: defaultdict[str, set[str]] = defaultdict(set)
            for record in records:
                if type(record) not in self.models.values():
                    raise ValueError("Use records generated by this Client")
                uri = str(vars(record)["uri"])
                _iri(uri)
                groups[class_iri(record)].add(uri)
            return {cls: sorted(iris) for cls, iris in sorted(groups.items())}

        sources = endpoints(source)
        target_records = self.find(target_value) if target_value is not None else target
        table = value_paths(
            self,
            sources,
            endpoints(target_records),
            max_hops=max_hops,
            both_directions=both_directions,
            max_paths=max_paths,
            allow_partial=allow_partial,
            allow_repeated_classes=allow_repeated_classes,
            meaning=meaning,
            via=via,
        )
        table.attrs["target_value"] = target_value
        selection_partial = any(
            isinstance(v, Results) and v.coverage.get("status") == "partial"
            for v in (source, target_records)
        )
        if selection_partial:
            table.attrs.update(status="partial", truncated=True)
            table.attrs["warnings"].append(
                "The input selection is partial; connections cover only its retained identities."
            )
        self.catalogue.retain_paths(table)
        return table

    def connections(
        self,
        source: str | BaseModel | pd.DataFrame,
        target: str | BaseModel | pd.DataFrame | None = None,
        *,
        max_hops: int = 3,
        both_directions: bool = True,
        max_paths: int | None = None,
    ) -> pd.DataFrame:
        """Find actual paths between records, or around one record.

        Unique source descriptions are accepted as endpoints.
        Show every intermediate resource and link. Paths do not repeat resources
        and can cross selected data graphs. Requests run in sequence. By default, stop at
        the client's row budget and return a marked partial view with a warning.
        Set max_paths explicitly to require a strict limit and raise on overflow.
        Set both_directions=False to follow outgoing links only.
        Without a target, return linked resources within max_hops. Do not follow
        rdf:type links or literal values; show classes as node annotations.
        """
        from rdfsolve.client.paths import resource_paths

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
        fenced: bool = True,
        namespaces: Iterable[str] = (),
        iris: str = "curie",
        merge: bool = True,
        links: Iterable[str] | None = None,
        kind: str | None = None,
    ) -> str:
        """Draw selected models or the selected rows of a paths table, without queries.

        With *links*, draw one record type (*kind*), those of its links (field names or labels;
        "^name" for a link pointing to it) and the record types each reaches, with counts.

        For models: namespaces keeps the classes in these namespaces (IRIs or prefixes);
        iris shows class IRIs as "curie", "full" or "none"; merge draws one edge per pair
        of classes with the names of all their links.
        """
        from rdfsolve.client.diagram import link_diagram, model_diagram, path_diagram

        if kind is not None:
            kinds = (*kinds, kind)
        if links is not None:
            if len(kinds) != 1:
                raise ValueError("Draw the links of one record type")
            return link_diagram(self, kinds[0], links, fenced=fenced)
        if paths is not None:
            if kinds:
                raise ValueError("Choose model names or a paths table, not both")
            return path_diagram(self, paths, path=path, instances=instances, fenced=fenced)
        if path is not None:
            raise ValueError("Supply a paths table to choose a path")
        return model_diagram(
            self, kinds, fenced=fenced, namespaces=namespaces, iris=iris, merge=merge
        )

    def _uses(self, cls: str, links: Iterable[str], to: str | None) -> bool:
        """Return whether the mined statements of *cls* use every link (towards *to*)."""
        from rdfsolve.conversion import _link

        for link in links:
            inverse = link.startswith("^")
            try:
                prop = _link(self, link.lstrip("^"), cls)
            except ValueError:
                return False
            found = False
            for pattern in self.schema.patterns:
                if pattern.property_uri != prop:
                    continue
                own, other = (
                    (pattern.object_class, pattern.subject_class)
                    if inverse
                    else (pattern.subject_class, pattern.object_class)
                )
                if own == cls and (to is None or other == to) and pattern.count != 0:
                    found = True
                    break
            if not found:
                return False
        return True

    def query_log(self) -> QueryLog:
        """Show every session query and its retained response without running it again."""
        return QueryLog(self.session_metadata())

    @staticmethod
    def title(record: BaseModel) -> str:
        """Read a loaded title or label, falling back to the exact resource IRI."""
        return _title(record)

    def trace(self) -> dict[str, Any]:
        """Explain the investigation using named steps and retained query identifiers."""
        records = self._records()
        return {
            "source": self.source.endpoint_url
            if isinstance(self.source, SparqlHelper)
            else "local RDF",
            "graphs": list(self.graph_uris),
            "source_queries": len(records),
            "endpoint_requests": sum(r.attempts for r in records),
            "steps": [dict(step) for step in self._steps],
            "ontology": self.ontology.diagnostics() if self.ontology else {"enabled": False},
        }

    def model(
        self, name_or_iri: str, *, links: Iterable[str] = (), to: str | None = None
    ) -> type[BaseModel]:
        """Accept generated names, spaced names, CURIEs of the mined prefixes, or class IRIs.

        A local name that several classes share (in two namespaces of one source) is decided
        by *links*: the class whose mined statements use each link (a field name or label; as
        subject, or as object for "^name"), towards the class *to* when given.
        """
        name_or_iri = str(name_or_iri)
        prefix, sep, local = name_or_iri.partition(":")
        if sep and not local.startswith("//") and prefix in self.schema.get_prefixes():
            name_or_iri = self.schema.get_prefixes()[prefix] + local
        if links and not name_or_iri.startswith(("http://", "https://")):
            # A name several classes share, as a generated name or a local name: the links decide.
            shared = [
                model
                for model in self.models.values()
                if _key(name_or_iri) == _key(re.split(r"[/#:]", class_iri(model))[-1])
            ]
            used = [m for m in shared if self._uses(class_iri(m), links, to)]
            if len(shared) > 1 and len(used) == 1:
                return used[0]
        if name_or_iri in self.models:
            return self.models[name_or_iri]
        for model in self.models.values():
            if class_iri(model) == name_or_iri:
                return model
        local_matches = [
            model
            for model in self.models.values()
            if _key(name_or_iri) == _key(re.split(r"[/#:]", class_iri(model))[-1])
        ]
        if len(local_matches) > 1:
            raise ValueError(
                f"Unknown or ambiguous class {name_or_iri!r}; use a full IRI: "
                + ", ".join(sorted(class_iri(model) for model in local_matches))
            )
        matches = [
            model
            for name, model in self.models.items()
            if _key(name_or_iri)
            in {
                _key(name),
                _key(re.split(r"[/#:]", getattr(model, "rdf_class_iri", ""))[-1]),
                _key(self.type_name(model)),
                "".join(w[0] for w in self.type_name(model).split()).casefold(),
            }
            or getattr(model, "rdf_class_iri", "") == name_or_iri
        ]
        if not matches:
            from rdfsolve.client.catalogue import words

            matches = [
                model
                for model in self.models.values()
                if words(name_or_iri) == words(self.type_name(model))
            ]
        if not matches:
            matches = [
                model
                for model in self.models.values()
                if name_or_iri.casefold() in self._vocabulary_names(class_iri(model))
            ]
        if len(matches) != 1:
            names = sorted(self.models)
            candidates = [name for name in names if _key(name_or_iri) in _key(name)] or names
            raise ValueError(
                f"Unknown or ambiguous class {name_or_iri!r}. Use an exact class name or IRI. "
                f"Available class names: {', '.join(candidates[:8])}"
            )
        return matches[0]

    def type_name(self, model: type[BaseModel] | str) -> str:
        """Display a source label without changing the generated type."""
        model = self.model(model) if isinstance(model, str) else model
        iri = getattr(model, "rdf_class_iri", "")
        labels = [
            item.text.value
            for item in self._schema.enrichment.labels
            if item.term_iri == iri
            and item.text.language in (None, "en")
            and item.predicate in LABEL_PREDICATES
        ]
        if labels:
            return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", min(labels))
        local = re.split(r"[/#:]", iri)[-1]
        return local if re.search(r"\d", local) else _name(local)

    def link_name(self, model: type[BaseModel] | str, field: str) -> str:
        """Use a source label or readable predicate name for a link."""
        model = self.model(model) if isinstance(model, str) else model
        extra = model.model_fields[field].json_schema_extra
        if isinstance(extra, dict):
            iri = str(extra.get("rdf_property_iri", ""))
            labels = [
                item.text.value
                for item in self._schema.enrichment.labels
                if item.term_iri == iri
                and item.text.language in (None, "en")
                and item.predicate in LABEL_PREDICATES
            ]
            if labels:
                return min(labels)
            if iri:
                return _name(re.split(r"[/#:]", iri)[-1])
        return _name(field)

    def fields(self, model: type[BaseModel] | str) -> pd.DataFrame:
        """List the fields of a model: property, value types, targets, links and lists.

        links is True when the field accepts IRIs (of a class, or of no class); list is True
        when its values can be an ordered rdf:List. Values are datatypes as CURIEs, or
        "IRI" for IRIs of no class. The table makes no source request.
        """
        from rdfsolve._uri import curie_from_prefixes
        from rdfsolve.client.exploration import field_targets

        model = self.model(model) if isinstance(model, str) else model
        self._check_model_scope(model)
        prefixes = self._schema.get_prefixes()

        def short(iri: str) -> str:
            """Write an IRI as a CURIE when a prefix is known."""
            found = curie_from_prefixes(iri, prefixes)
            return found[0] if found else iri

        rows = []
        for name, info in sorted(model.model_fields.items()):
            meta = field_metadata(info)
            if not meta.get("rdf_property_iri"):
                continue
            patterns = meta.get("rdf_patterns", [])
            lists = meta.get("rdf_collections", [])
            values = sorted(
                {
                    short(p["datatype"]) if p.get("datatype") else "Literal"
                    for p in patterns
                    if p.get("object_class") == "Literal"
                }
                | {"IRI" for p in patterns if p.get("object_class") in ("Resource", "BlankNode")}
            )
            targets = sorted(field_targets(model, name))
            rows.append(
                {
                    "field": name,
                    "property": meta["rdf_property_iri"],
                    "curie": short(meta["rdf_property_iri"]),
                    "label": self.link_name(model, name),
                    "values": values,
                    "targets": targets,
                    "links": bool(targets or "IRI" in values),
                    "list": bool(lists),
                }
            )
        columns = ["field", "property", "curie", "label", "values", "targets", "links", "list"]
        return pd.DataFrame(rows, columns=columns)

    def field_name(self, model: type[BaseModel] | str, text: str) -> str:
        """Resolve a field by its Python name, source label or exact predicate IRI."""
        text = str(text)
        model = self.model(model) if isinstance(model, str) else model
        if (model, text) not in self._field_names:
            self._field_names[model, text] = self._find_field_name(model, text)
        return self._field_names[model, text]

    def _find_field_name(self, model: type[BaseModel], text: str) -> str:
        """Find a field: exact name, IRI or CURIE, local name, other spelling, then labels."""
        if text in model.model_fields:
            return text
        spelled = re.sub(r"[^A-Za-z0-9]", "", text)
        iris = {
            name: str(field.json_schema_extra.get("rdf_property_iri"))
            for name, field in model.model_fields.items()
            if isinstance(field.json_schema_extra, dict)
            and field.json_schema_extra.get("rdf_property_iri")
        }
        prefixes = getattr(model, "rdf_prefixes", {})
        exact = [
            name
            for name, iri in iris.items()
            if iri == text
            or ((":" in text or "_" in text) and _curie_key(iri, prefixes) == spelled)
        ]
        if len(exact) == 1:
            return exact[0]  # An IRI or a CURIE identifies one property.
        local = [name for name, iri in iris.items() if re.split(r"[/#:]", iri)[-1] == text]
        own = [name for name in local if iris[name].startswith(_namespace(class_iri(model)))]
        for found in (own, local):
            if len(found) == 1:
                return found[
                    0
                ]  # The local name of the property, in the vocabulary of the class first.
        spelled_names = [name for name in model.model_fields if _key(name) == _key(text)]
        if len(spelled_names) == 1:
            return spelled_names[0]  # A field name with a different spelling is used before labels.
        matches = [
            name
            for name, field in model.model_fields.items()
            if _key(text) in {_key(name), _key(self.link_name(model, name))}
            or (
                isinstance(field.json_schema_extra, dict)
                and (
                    field.json_schema_extra.get("rdf_property_iri") == text
                    or (
                        isinstance(path := field.json_schema_extra.get("rdf_path"), dict)
                        and path.get("iri") == text
                    )
                )
            )
        ]
        if not matches:
            matches = [
                name
                for name, field in model.model_fields.items()
                if isinstance(field.json_schema_extra, dict)
                and text.casefold()
                in self._vocabulary_names(str(field_metadata(field).get("rdf_property_iri", "")))
            ]
        if len(matches) != 1:
            names = [
                name
                for name, field in model.model_fields.items()
                if isinstance(field.json_schema_extra, dict)
                and field.json_schema_extra.get("rdf_path")
            ]
            raise ValueError(
                f"Unknown or ambiguous field {text!r} for {self.type_name(model)}. "
                f"Available fields: {', '.join(names)}"
            )
        return matches[0]

    def create(
        self,
        kind: str,
        *,
        uri: str | BNode | None = None,
        blank_node_scope: str | None = None,
        language: str | None = None,
        extra_types: list[str] | tuple[str, ...] = (),
        **values: Any,
    ) -> BaseModel:
        """Create a record with schema-guided values and optional extra type IRIs.

        Language supplies a default for language-tagged fields. Explicit RDF
        terms retain their metadata. Extra types add assertions to this node.
        """
        from rdfsolve.client.authoring import create_record

        model = self.model(kind)
        fields = {self.field_name(model, field): value for field, value in values.items()}
        return create_record(
            model,
            fields,
            uri=uri,
            blank_node_scope=blank_node_scope,
            language=language,
            extra_types=extra_types,
        )

    def from_table(
        self,
        kind: str,
        table: pd.DataFrame,
        *,
        id_column: str | None = None,
        language: str | None = None,
        languages: Mapping[str, str] | None = None,
        datatypes: Mapping[str, str] | None = None,
        blank_node_scope: str | None = None,
        **columns: str,
    ) -> Results:
        """Create records from columns, with optional literal defaults keyed by field.

        Missing identifiers create distinct blank nodes; missing field values are
        omitted. Explicit RDF terms retain their metadata. Errors identify the row.
        """
        from rdfsolve.client.authoring import create_record, table_literal

        model = self.model(kind)
        missing = ({id_column} if id_column is not None else set()) | set(columns.values())
        missing -= set(table.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        fields = {self.field_name(model, field): column for field, column in columns.items()}
        languages = {self.field_name(model, k): v for k, v in (languages or {}).items()}
        datatypes = {self.field_name(model, k): v for k, v in (datatypes or {}).items()}
        if (languages.keys() | datatypes.keys()) - fields.keys():
            raise ValueError("Literal defaults require a mapped field")
        records = []
        for position, (index, row) in enumerate(table.iterrows()):
            try:
                identifier = (
                    table_literal(row[id_column], language=None, datatype=None)
                    if id_column is not None
                    else None
                )
                if identifier is not None and not isinstance(identifier, (str, BNode)):
                    raise ValueError("The identifier column must contain IRIs or blank nodes")
                values = {
                    field: table_literal(
                        row[column], language=languages.get(field), datatype=datatypes.get(field)
                    )
                    for field, column in fields.items()
                }
                records.append(
                    create_record(
                        model,
                        values,
                        uri=identifier,
                        blank_node_scope=blank_node_scope,
                        language=language,
                    )
                )
            except (TypeError, ValueError) as error:
                raise ValueError(f"row {position} (index {index!r}): {error}") from error
        return Results(self, records, coverage={"status": "complete", "basis": "Authored records"})

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
        return cls.open(MinedSchema.from_dict(session["schema"]), data_file=data_file, **kwargs)

    def find(
        self,
        text: str,
        *,
        kind: str | None = None,
        field: str | None = None,
        allow_partial: bool = False,
    ) -> Results:
        """Find names and identifiers, optionally retaining a marked partial selection."""
        from rdfsolve.client.search import search_records

        result = search_records(
            self,
            [text],
            kind,
            [field] if field else [],
            names_only=True,
            allow_partial=allow_partial,
        )
        if result or not self.ontology or field:
            return result
        import bioregistry

        from rdfsolve.identifiers import curie, parse

        registered = parse(class_iri(self.model(kind))) if kind else None
        prefix = registered.prefix if registered else None
        ontology = bioregistry.get_ols_prefix(prefix) if prefix else None
        looked_up = [
            self.vocabulary(c["iri"]) for c in self.ontology.search(text, ontology=ontology)
        ]
        candidates = [
            c
            for c in looked_up
            if c is not None
            and text.casefold() in {v.casefold() for v in [c["label"], *c["synonyms"]]}
        ]
        aliases = list(dict.fromkeys(c.get("label", "") for c in candidates))
        aliases = [a for a in aliases if a and len(a) <= 200 and a.casefold() != text.casefold()]
        if not aliases:
            return result
        found = search_records(
            self, aliases[:12], kind, [], names_only=True, allow_partial=allow_partial
        )
        keys = {curie(c["iri"]) for c in candidates}
        records = [r for r in found if curie(str(vars(r)["uri"])) in keys]
        identities = {str(vars(r)["uri"]) for r in records}
        evidence = [e for e in found.evidence if e["id"] in identities]
        return Results(
            self,
            records,
            evidence=evidence,
            coverage={
                **found.coverage,
                "terms": [text],
                "ontology_candidates": candidates,
                "identity_check": "exact IRI or registered namespace and identifier",
            },
        )

    def search(
        self, terms: Sequence[str], *, kind: str | None = None, fields: Sequence[str] | None = None
    ) -> Results:
        """Find text candidates and keep matching passages in result.evidence.

        Search any phrase in names, identifiers and mined text fields. Supply fields
        to narrow the search. This does not infer synonyms or scientific relevance.
        """
        from rdfsolve.client.search import search_records

        return search_records(self, terms, kind, fields or [])

    def save(self, path: str | Path, *groups: Results | BaseModel) -> None:
        """Save individual records or result sets and retained direct links as RDF."""
        self.to_graph(*groups).serialize(destination=path, format="turtle")

    def to_graph(self, *groups: Results | BaseModel) -> Graph:
        """Return records or result sets and their retained direct links as an RDF graph."""
        records: list[BaseModel] = []
        model_types = tuple(self.models.values())
        for group in groups:
            if isinstance(group, Results):
                if group.client is not self:
                    raise ValueError("Save results from one client at a time")
                records.extend(group)
            elif isinstance(group, model_types):
                records.append(group)
            else:
                raise ValueError("Save records created with this client's models")
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
        # Schema prefixes, and registered names for the other namespaces of the records;
        # a namespace without a registered name stays written in full.
        import bioregistry

        schema_prefixes = self._schema.get_prefixes()
        iris = {str(term) for triple in graph for term in triple if isinstance(term, URIRef)}
        for prefix, namespace in prefix_map(iris, schema_prefixes).items():
            if prefix in schema_prefixes or bioregistry.get_resource(prefix) is not None:
                graph.bind(prefix, namespace, replace=True)
        return graph

    def to_oxigraph(self, *groups: Results | BaseModel) -> ox.Dataset:
        """Return records or result sets and their retained direct links as an Oxigraph dataset."""
        from rdfsolve.local_rdf import to_oxigraph

        return to_oxigraph(self.to_graph(*groups))

    def property_graph(
        self,
        *groups: Results | BaseModel,
        folds: Iterable[Fold] = (),
        types: Mapping[str, Conversion | None] | None = None,
        native: bool = True,
        names: Any = "local",
        identity: Identity | None = None,
    ) -> PropertyGraph:
        """Return records or result sets as a property graph, checked against their RDF.

        Folds turn n-ary nodes (a catalysis, an axiom) into edges with properties; see
        suggest_folds. *types* overrides the literal conversions per datatype (None keeps the
        lexical form) and native=False keeps every literal as written. *names*: "local" (the
        default), "curie", "label", "iri", a function of the IRI, or a mapping of IRIs to names.
        *identity* merges the IRIs of one identifier and decides how mappings are shown.
        """
        from rdfsolve.property_graph import PropertyGraph

        return PropertyGraph.from_rdf(
            self.to_oxigraph(*groups),
            schema=self._schema,
            folds=folds,
            types=types,
            native=native,
            names=names,
            identity=identity,
        )

    def issued_kinds(self) -> dict[str, list[str]]:
        """Return the classes this source gives the identifiers it issues: {prefix: classes}.

        A source issues the identifiers of its own registered prefix (Bioregistry, from the
        dataset name of the schema); their classes are the schema classes whose example subjects
        carry that prefix (rdfsolve.mappings.signatures). An identifier the source only cites
        (WikiPathways and Ensembl genes) is not its own.
        """
        import bioregistry

        from rdfsolve.mappings.signatures import signatures

        name = self._schema.about.dataset_name
        prefix = bioregistry.normalize_prefix(name) if name else None
        classes = signatures(self._schema).subjects.get(prefix, set()) if prefix else set()
        return {prefix: sorted(classes)} if prefix and classes else {}

    def suggest_folds(self) -> list[Fold]:
        """Propose classes whose instances can become edges, from the mined schema."""
        from rdfsolve.property_graph import suggest_folds

        return suggest_folds(self._schema)


class Results:
    """A reusable set of generated records. Display and completion do not query."""

    def __init__(
        self,
        client: Client,
        records: list[BaseModel],
        *,
        evidence: list[dict[str, Any]] | None = None,
        coverage: dict[str, Any] | None = None,
    ) -> None:
        """Keep the client and typed records together."""
        self.client = client
        self.records = records
        self.evidence = evidence or []
        self.coverage = coverage or {"status": "complete", "basis": "Retrieved records"}
        self.references = client.catalogue.retain_records(self)

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

    def summary(self) -> dict[str, Any]:
        """Describe the selected set and loaded fields without exposing record values."""
        counts: defaultdict[str, int] = defaultdict(int)
        fields: defaultdict[str, set[str]] = defaultdict(set)
        for record in self.records:
            kind = self.client.type_name(type(record))
            counts[kind] += 1
            fields[kind].update(vars(record).get("rdf_loaded_fields", []))
        return {
            "records": len(self),
            "types": [
                {"class": kind, "records": count, "loaded_fields": sorted(fields[kind])}
                for kind, count in counts.items()
            ],
            "coverage": {
                k: self.coverage[k]
                for k in ("status", "basis", "query_ids", "limit_reached")
                if k in self.coverage
            },
            "scope": "All records in this retained selection",
        }

    def paths_between(self, target: Any, **kwargs: Any) -> pd.DataFrame:
        """Evaluate paths from every selected record through this set's Client."""
        return self.client.paths_between(self, target, **kwargs)

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
        return Results(
            self.client,
            [r for r in self.records if type(r) is model],
            evidence=self.evidence,
            coverage=self.coverage,
        )

    def load(self, *fields: str) -> Results:
        """Read these fields of every record (in a few queries) and return the same set.

        Only read fields are exported (to_oxigraph, property graphs). Without fields, each
        record's values and cross-references are read (Client.record_fields).
        """
        if fields:
            self._load(*fields)
            return self
        loaded: dict[Any, BaseModel] = {}
        for model in {type(r) for r in self.records}:
            part = Results(self.client, [r for r in self.records if type(r) is model])
            part._load(*self.client.record_fields(str(getattr(model, "rdf_class_iri", ""))))
            loaded.update((vars(r)["uri"], r) for r in part.records)  # _load gives new records
        self.records = [loaded.get(vars(r)["uri"], r) for r in self.records]
        return self

    def where(self, field: str, value: Any) -> Results:
        """Keep the records whose *field* has *value* (UniProt entries with reviewed true)."""
        self._load(field)
        kept = []
        for record in self.records:
            name = self.client.field_name(type(record), field)
            found = vars(record).get(name)
            values = found if isinstance(found, list) else [found]
            if any(v == value or str(v).lower() == str(value).lower() for v in values):
                kept.append(record)
        return Results(self.client, kept, evidence=self.evidence, coverage=self.coverage)

    def without(self, other: Results) -> Results:
        """Remove records with IRIs already present in another result set."""
        ids = {vars(record)["uri"] for record in other}
        return Results(
            self.client,
            [r for r in self.records if vars(r)["uri"] not in ids],
            evidence=self.evidence,
            coverage=self.coverage,
        )

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
                    r"\W+", "_", self.client.link_name(type(record), name).lower()
                ).strip("_")
                if label.isidentifier() and label not in aliases:
                    aliases[label] = name
        return SimpleNamespace(**aliases)

    def _routes(self, incoming: bool) -> list[tuple[type[BaseModel], str, type[BaseModel]]]:
        routes = []
        models = {type(record) for record in self.records}
        for source in self.client.models.values() if incoming else models:
            for row in self.client.links(source).itertuples(index=False):
                target = self.client.models[str(row.target)]
                if not incoming or target in models:
                    routes.append(
                        (target, str(row.field), source)
                        if incoming
                        else (source, str(row.field), target)
                    )
        return routes

    def paths(self, *, incoming: bool = False) -> pd.DataFrame:
        """Show the types and named links available from these records.

        A type whose name other types share is shown as a CURIE, so every name
        shown can be given back to related().
        """
        return pd.DataFrame(
            [
                {
                    "From": self._shown(source),
                    "Link": self.client.link_name(target if incoming else source, field),
                    "To": self._shown(target),
                }
                for source, field, target in self._routes(incoming)
            ],
            columns=["From", "Link", "To"],
        ).drop_duplicates()

    def links(self, via: str | None = None) -> dict[str, set[str]]:
        """Return the records each record reached (source -> targets), from this set's evidence.

        A related() step is read back by the record it started from; the evidence holds it.
        With *via* (as given to related()), that link only. Only targets in this set are given (after where()).
        """
        kept = {vars(record)["uri"] for record in self.records}
        found: defaultdict[str, set[str]] = defaultdict(set)
        for link in self.evidence:
            if link["target"] in kept and (
                via is None or _key(via.lstrip("^")) == _key(str(link.get("via")).lstrip("^"))
            ):
                found[link["source"]].add(link["target"])
        return dict(found)

    def _shown(self, model: type[BaseModel]) -> str:
        """Return a type's name, or its CURIE when the name is shared by other types."""
        name = self.client.type_name(model)
        try:
            if self.client.model(name) is model:
                return name
        except ValueError:
            pass
        iri = class_iri(model)
        for prefix, namespace in self.client.schema.get_prefixes().items():
            if namespace and iri.startswith(namespace):
                return f"{prefix}:{iri[len(namespace) :]}"
        return iri

    def related(
        self,
        kind: str | None = None,
        *,
        via: str | Sequence[str] | None = None,
        value: str | None = None,
        incoming: bool = False,
        depth: int = 1,
    ) -> Results:
        """Follow a link or an intermediate class, optionally matching a name or identifier.

        A class in via means two hops. A link name selects one direct link. Without kind, every
        type the link reaches (types() then counts them).
        Text is a case-insensitive substring, tested only on connected targets.

        via may be a path, a list of links that each reach *kind* ("^name" backwards). depth
        repeats the path on what it reaches, until nothing new is reached; every record
        reached is returned, with the evidence of every hop (links()).
        """
        if depth > 1 or not (via is None or isinstance(via, str)):
            hops = (
                [(via, incoming)]
                if via is None or isinstance(via, str)
                else [(hop.lstrip("^"), hop.startswith("^")) for hop in via]
            )
            reached: dict[str, BaseModel] = {}
            evidence: list[dict[str, Any]] = []
            level = self
            for _ in range(depth):
                for hop, backwards in hops:
                    level = level.related(kind, via=hop, value=value, incoming=backwards)
                    evidence += level.evidence
                new = [r for r in level if vars(r)["uri"] not in reached]
                reached.update((vars(r)["uri"], r) for r in new)
                if not new:
                    break
                level = Results(self.client, new)
            return Results(self.client, list(reached.values()), evidence=evidence)
        if kind is None and value is None and via is None:
            raise ValueError("Choose kind, via or value")
        if value is not None and not value.strip():
            raise ValueError("Enter a word or name to find")

        def named(source: type[BaseModel], field: str, dest: type[BaseModel]) -> bool:
            """Whether via names this link (its field or its label)."""
            owner = dest if incoming else source
            return _key(str(via)) in {_key(field), _key(self.client.link_name(owner, field))}

        if via is not None:
            # A link of these records with that name is followed directly, also when a class
            # has the same name.
            direct = any(named(*route) for route in self._routes(incoming))
            try:
                intermediate = None if direct else self.client.model(via)
            except ValueError:
                intermediate = None
            if intermediate is not None:
                middle = self.related(class_iri(intermediate), incoming=incoming)
                return middle.related(kind, value=value, incoming=incoming)
        if kind is None:
            targets = sorted(
                {
                    class_iri(target)
                    for source, field, target in self._routes(incoming)
                    if via is None or named(source, field, target)
                }
            )
            records: list[BaseModel] = []
            for name in targets:
                records.extend(self.related(name, via=via, value=value, incoming=incoming))
            return Results(self.client, records)
        target = self.client.model(kind)
        routes = [
            (source, field)
            for source, field, dest in self._routes(incoming)
            if dest is target and (via is None or named(source, field, dest))
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
        return Results(
            self.client,
            list(found.values()),
            evidence=[
                {**link, "via": via}
                for link in self.client._matches
                if link["query_id"] in self.client._steps[-1]["query_ids"]
            ],
            coverage={
                "status": self.coverage.get("status", "complete"),
                "basis": "Related resources from the whole selected set",
                "source_records": len(self),
                "via": via,
                "query_ids": self.client._steps[-1]["query_ids"],
            },
        )

    def _load(self, *fields: str) -> None:
        fields = tuple(field for field in fields if field != "uri")
        if not fields:
            return
        models = {type(record) for record in self.records}
        names = {
            model: {self.client.field_name(model, field) for field in fields} for model in models
        }
        with self.client.step(f"Read {', '.join(fields)}"):
            for model, requested in names.items():
                group = [
                    r
                    for r in self.records
                    if type(r) is model
                    and requested - set(vars(r).get("rdf_loaded_fields", [])) - r.model_fields_set
                ]
                if not group:
                    continue
                selected = set(_name_fields(model)) | requested
                selected.update(
                    field for r in group for field in vars(r).get("rdf_loaded_fields", [])
                )
                iris = [str(vars(r)["uri"]) for r in group]
                size = self.client.max_subjects  # one call reads at most this many subjects
                loaded = [
                    record
                    for start in range(0, len(iris), size)
                    for record in self._get_many(model, iris[start : start + size], selected)
                ]
                replacements = {vars(r)["uri"]: r for r in loaded}
                self.records = [
                    replacements.get(vars(r)["uri"], r) if type(r) is model else r
                    for r in self.records
                ]

    def _get_many(
        self, model: type[BaseModel], iris: list[str], fields: set[str]
    ) -> list[BaseModel]:
        """Read the records of *iris*; a batch over the value budget is split in two and retried.

        A UniProt entry can carry thousands of citations, so a batch may exceed the budget that
        one entry does not. When one entry alone exceeds it, the fields whose values alone
        exceed it are left out for that entry, and the coverage of the set says so (partial).
        """
        from rdfsolve.client.hydration import HydrationLimitError

        try:
            return list(self.client.get_many(model, iris, fields=sorted(fields)))
        except HydrationLimitError:
            if len(iris) > 1:
                half = len(iris) // 2
                return self._get_many(model, iris[:half], fields) + self._get_many(
                    model, iris[half:], fields
                )
            too_many = []
            for name in sorted(fields):
                try:
                    self.client.get_many(model, iris, fields=[name])
                except HydrationLimitError:
                    too_many.append(name)
            if not too_many or set(too_many) == set(fields):
                raise
            left_out = dict(self.coverage.get("left_out", {}))
            left_out[iris[0]] = too_many
            self.coverage = {
                **self.coverage,
                "status": "partial",
                "basis": "fields with more values than the budget were left out",
                "left_out": left_out,
            }
            return self._get_many(model, iris, fields - set(too_many))

    def show(self, *fields: str) -> pd.DataFrame:
        """Show names and requested fields, retrieving those fields if needed."""
        self._load(*fields)
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

    def table(self, *fields: str) -> pd.DataFrame:
        """Return RDF term lists and typed records; load requested fields if needed.

        With no fields, use only what has been read. NA means unread or inapplicable;
        an empty list means the field was read but no value was returned.
        attrs retains records, session queries with original bindings, and links.
        """
        from rdfsolve.client.table import record_table

        self._load(*fields)
        models = {type(record) for record in self.records}
        names = sorted(
            {self.client.field_name(model, field) for model in models for field in fields}
        )
        metadata = self.client.session_metadata()
        return record_table(
            self.records,
            labels={
                str(getattr(model, "rdf_class_iri", "")): self.client.type_name(model)
                for model in models
            },
            fields=names if fields else None,
            context={
                "queries": metadata["queries"],
                "links": metadata["links"],
                "evidence": self.evidence,
                "coverage": self.coverage,
            },
        )

    def values(self, field: str) -> pd.DataFrame:
        """List values of one field across the selected records."""
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
    from rdfsolve.mining.miner import SchemaMiner

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


_RELEASE_FIELDS = ("source_version", "source_version_iri", "source_issued", "source_modified")


def _add_release(schema: MinedSchema, path: Path) -> None:
    """Fill the source's release from the metadata saved next to its schema file.

    The miner asks the endpoint for the dataset's description; a source whose description
    comes with its download (a VoID file of each release) has it in
    ``<name>_metadata.ttl`` beside ``<name>_schema.json`` (or ``<name>.metadata.ttl`` beside
    ``<name>.schema.json``) instead. The described dataset is the
    void:Dataset whose subjects are most of the schema's classes (the file also describes the
    ontologies the source imports). Fields the schema already has are kept.
    """
    about = schema.about
    if any(getattr(about, f, None) for f in _RELEASE_FIELDS):
        return
    found = next(
        (
            path.with_name(path.name.replace(schema_end, metadata_end))
            for schema_end, metadata_end in (
                ("_schema.json", "_metadata.ttl"),
                (".schema.json", ".metadata.ttl"),
            )
            if path.name.endswith(schema_end)
        ),
        None,
    )
    if found is None or not found.exists():
        return
    from rdflib import RDF, Namespace, URIRef

    from rdfsolve.metadata import query_endpoint_metadata
    from rdfsolve.mining.local_graph import LocalGraphHelper

    void, dcterms = Namespace("http://rdfs.org/ns/void#"), Namespace("http://purl.org/dc/terms/")
    data = Dataset()
    data.parse(found)
    classes = set(schema.get_classes())

    def shared(dataset: Any) -> int:
        """Return how many of the schema's classes the dataset names as its subjects."""
        named = {str(o) for p in (dcterms.subject, void["class"]) for o in data.objects(dataset, p)}
        return len(named & classes)

    candidates = sorted(set(data.subjects(RDF.type, void.Dataset)), key=shared, reverse=True)
    if not candidates or shared(candidates[0]) == 0:
        return
    helper = LocalGraphHelper(found.resolve().as_uri(), data)
    described = query_endpoint_metadata(helper, subject_iri=str(candidates[0]))
    for field_name in _RELEASE_FIELDS:
        if described.get(field_name) and not getattr(about, field_name, None):
            setattr(about, field_name, described[field_name])
