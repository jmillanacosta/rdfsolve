"""Schema from the VoID that an endpoint publishes, with light mining of what it leaves out.

An endpoint that publishes a full VoID description (void-generator: IDSM, UniProt, Rhea, Bgee,
SwissLipids) states its classes, the properties of each class with their datatypes, and the
links between classes (linksets), with counts. The schema is read from that description,
scoped to the graphs of the source (rdfsolve.schema_models.readers.void). Mining the endpoint
for the same patterns is long and is cut by proxies and time limits.

What the description leaves out is found from its own counts, with no query: the triples of a
class and property that its linksets and datatype partitions do not account for have objects
without a class (or blank nodes, when its distinct objects exceed its IRIs and literals); the
triples of a property beyond those of its class partitions have subjects without a class. Each
such gap is mined with a few small queries under a time limit (helper.budget); a query that is
refused leaves a measurement gap in the report, and mining goes on. When the endpoint cuts the
queries of one purpose at a fixed limit (rdfsolve.sparql_helper.QueryCuts), no more queries of
that purpose are sent: the record (stopped) and a measurement gap say why, and how many queries
were cut and not sent.

Also asked: the language tags of language-tagged literals (from a sample), five subjects of
every class (their IRI namespaces), one example of the largest patterns, and a re-count of the
largest and of randomly chosen patterns, which measures how the endpoint drifted from its VoID
since the VoID was issued. The drift is reported; the VoID is used (the owner's decision).

Light means light: each query has query_seconds (30 s; 99 % of the answered void/* queries of
the rehearsal of 2026-10-06 took under 20 s, most of them 2 s), and a purpose whose queries run
past that limit five times in a row is stopped (QueryCuts with client_timeouts): the queries it
does not send are each recorded as a measurement gap, with the reason. The class members that
the VoID states are checked against its own property partitions (void_class_populations): a
count that they contradict is kept as a lower bound, never as an exact number.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from rdflib import RDF, Graph, Namespace, URIRef

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.sparql_helper import QueryCuts

if TYPE_CHECKING:
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

VOID = Namespace("http://rdfs.org/ns/void#")
VOID_EXT = Namespace("http://ldf.fi/void-ext#")
RDF_LANG_STRING = str(RDF.langString)


@dataclass
class VoidGap:
    """A part of the data that the VoID description does not give a class for."""

    kind: str  # "objects" (untyped IRI objects), "blank" (blank-node objects), "subjects"
    property_uri: str
    subject_class: str | None
    triples: int


def void_gaps(void: Graph) -> list[VoidGap]:
    """Find, from the counts of a scoped VoID description alone, what it leaves without a class.

    A class and property: its triples, less those of its linksets (the largest description of
    each object class) and of its datatype partitions, have objects without a class; when its
    distinct objects exceed its distinct IRI objects and literals, some are blank nodes.
    A property of the dataset: its triples, less those of its class partitions, have subjects
    without a class (a lower bound: a subject of two classes is counted twice).
    """
    gaps: list[VoidGap] = []
    linked: dict[tuple[str, str], dict[str, int]] = {}
    for linkset in void.subjects(VOID.linkPredicate, None):
        source = void.value(void.value(linkset, VOID.subjectsTarget), VOID["class"])
        target = void.value(void.value(linkset, VOID.objectsTarget), VOID["class"])
        predicate = void.value(linkset, VOID.linkPredicate)
        count = optional_count(void.value(linkset, VOID.triples)) or 0
        if source is None or target is None or predicate is None:
            continue
        ends = linked.setdefault((str(source), str(predicate)), {})
        ends[str(target)] = max(ends.get(str(target), 0), count)
    by_property: dict[str, int] = {}
    for partition in set(void.objects(None, VOID.classPartition)):
        cls = void.value(partition, VOID["class"])
        if cls is None:
            continue
        for part in void.objects(partition, VOID.propertyPartition):
            predicate = void.value(part, VOID.property)
            triples = optional_count(void.value(part, VOID.triples))
            if predicate is None or triples is None:
                continue
            by_property[str(predicate)] = by_property.get(str(predicate), 0) + triples
            literal = sum(
                optional_count(void.value(d, VOID.triples)) or 0
                for d in void.objects(part, VOID_EXT.datatypePartition)
            )
            links = sum(linked.get((str(cls), str(predicate)), {}).values())
            rest = triples - literal - links
            if rest <= 0 or predicate == RDF.type:
                continue
            objects = optional_count(void.value(part, VOID.distinctObjects))
            iris = optional_count(void.value(part, VOID_EXT.distinctIRIReferenceObjects)) or 0
            literals = optional_count(void.value(part, VOID_EXT.distinctLiterals)) or 0
            blank = objects is not None and objects > iris + literals
            kind = "blank" if blank and iris == 0 else "objects"
            gaps.append(VoidGap(kind, str(predicate), str(cls), rest))
            if blank and iris and objects is not None:
                gaps.append(VoidGap("blank", str(predicate), str(cls), objects - iris - literals))
    for dataset in set(void.subjects(VOID.classPartition, None)) - set(
        void.objects(None, VOID.classPartition)
    ):
        for part in void.objects(dataset, VOID.propertyPartition):
            predicate = void.value(part, VOID.property)
            triples = optional_count(void.value(part, VOID.triples))
            if predicate is None or triples is None or predicate == RDF.type:
                continue
            rest = triples - by_property.get(str(predicate), 0)
            if rest > 0:
                gaps.append(VoidGap("subjects", str(predicate), None, rest))
    return sorted(gaps, key=lambda g: -g.triples)


def void_property_usage(void: Graph) -> dict[tuple[str, str], dict[str, Any]]:
    """Return what a VoID states of each class and property: the measures of property usage.

    A class-property partition gives the triples, distinct subjects and distinct objects of
    the property on the members of the class, and its datatype partitions the triples of each
    datatype: the counts that rdfsolve.evidence.observed would otherwise query. A measure
    that the partition leaves out is None. A class described by two partitions (two graphs)
    is left out: their counts do not add up over the merge of the graphs.
    """
    stated: dict[tuple[str, str], dict[str, Any]] = {}
    twice: set[tuple[str, str]] = set()
    for partition in set(void.objects(None, VOID.classPartition)):
        cls = void.value(partition, VOID["class"])
        if cls is None:
            continue
        for part in void.objects(partition, VOID.propertyPartition):
            prop = void.value(part, VOID.property)
            if prop is None or prop == RDF.type:
                continue
            key = (str(cls), str(prop))
            if key in stated:
                twice.add(key)
            datatypes: dict[str, int] = {}
            for d in void.objects(part, VOID_EXT.datatypePartition):
                datatype = void.value(d, VOID_EXT.datatype)
                triples = optional_count(void.value(d, VOID.triples))
                if datatype is not None and triples is not None:
                    datatypes[str(datatype)] = datatypes.get(str(datatype), 0) + triples
            stated[key] = {
                "triples": optional_count(void.value(part, VOID.triples)),
                "subjects": optional_count(void.value(part, VOID.distinctSubjects)),
                "objects": optional_count(void.value(part, VOID.distinctObjects)),
                "literals": optional_count(void.value(part, VOID_EXT.distinctLiterals)),
                "datatypes": datatypes,
            }
    return {key: value for key, value in stated.items() if key not in twice}


def _term(binding: dict[str, Any]) -> RdfTerm:
    """Return an RdfTerm from a SPARQL JSON binding."""
    kind = {"uri": "uri", "literal": "literal", "typed-literal": "literal"}.get(
        binding["type"], "bnode"
    )
    return RdfTerm(
        kind=kind,
        value=binding["value"],
        datatype=binding.get("datatype"),
        language=binding.get("xml:lang"),
    )


class VoidStrategy(MiningStrategy):
    """Read the schema from a published VoID description; mine only what it leaves out."""

    # The strategy gives its own view: counts, labels and examples come from the VoID and from
    # its own queries, not from the miner's phases over all the data.
    scoped = True

    def __init__(
        self,
        void: Graph,
        *,
        void_graph: str,
        issued: str | None = None,
        read_by: str = "construct",
        query_seconds: float = 30.0,
        class_samples: int = 5,
        example_patterns: int = 200,
        drift_largest: int = 20,
        drift_random: int = 20,
        language_sample: int = 10_000,
        object_sample: int = 1_000,
        seed: int = 0,
    ) -> None:
        """Keep the scoped VoID description and the limits of the light mining."""
        self.void = void
        self.void_graph = void_graph
        self.issued = issued
        self.read_by = read_by
        self.query_seconds = query_seconds
        self.class_samples = class_samples
        self.example_patterns = example_patterns
        self.drift_largest = drift_largest
        self.drift_random = drift_random
        self.language_sample = language_sample
        self.object_sample = object_sample
        self.random = random.Random(seed)  # noqa: S311 (a sample of patterns, not a secret)
        self.examples: list[PatternExample] = []
        self.labels: list[Any] = []
        self.class_examples: dict[str, list[RdfTerm]] = {}
        self.entity_counts: dict[str, int] = {}
        self.entity_count_states: dict[str, str] = {}
        self.record: dict[str, Any] = {}
        self.untyped_subjects: list[dict[str, Any]] = []
        self.object_samples: list[dict[str, Any]] = []
        self._void_patterns: list[SchemaPattern] = []
        self.cuts = QueryCuts(client_timeouts=True)

    @property
    def name(self) -> str:
        """Return the strategy name for reporting."""
        return "void"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Read the patterns of the VoID, then mine its gaps, samples and drift."""
        from rdfsolve.schema_models.readers.void import (
            void_class_populations,
            void_graph_to_minedschema,
        )

        phase = context.report.start_phase("void")
        schema = void_graph_to_minedschema(self.void, report_untyped=False)
        left_out = schema.source_metadata.iri_findings if schema.source_metadata else None
        if left_out:
            found = context.report.report.config.setdefault("iri_findings", {})
            found.setdefault("graphs", {})["void"] = left_out
        patterns = list(schema.patterns)
        for pattern in patterns:
            pattern.count_semantics = "endpoint_default"
        self._void_patterns = list(patterns)
        # void:entities, else void:distinctSubjects, checked against the property partitions.
        counts, states, contradicted = void_class_populations(self.void, subjects_fallback=True)
        self.entity_counts = counts
        self.entity_count_states = {c: s for c, s in states.items() if s != "complete"}
        gaps = void_gaps(self.void)
        self.record = {
            "void_graph": self.void_graph,
            "issued": self.issued,
            "read_by": self.read_by,
            "void_patterns": len(patterns),
            "gaps": [g.__dict__ for g in gaps],
        }
        if contradicted:
            self.record["class_counts_contradicted"] = contradicted
        context.report.finish_phase(phase, items=len(patterns))
        failures: list[QueryFailure] = []
        self.cuts = QueryCuts(client_timeouts=True)
        patterns += self._mine_gaps(context.helper, gaps, failures, context.graph_uris)
        self._languages(context.helper, patterns, failures, context.graph_uris)
        self._class_samples(context.helper, patterns, failures, context.graph_uris)
        self._examples(context.helper, patterns, failures, context.graph_uris)
        self.record["untyped_subjects"] = self.untyped_subjects
        self.record["object_samples"] = self.object_samples
        self.record["drift"] = self._drift(context.helper, patterns, failures, context.graph_uris)
        stopped = self.cuts.record()
        if stopped:
            self.record["stopped"] = stopped
            self.record["drift"]["stopped"] = stopped.get("void/drift", {}).get("stopped")
        for purpose, stop in stopped.items():
            failures.append(
                QueryFailure(
                    "timeout",
                    f"{stop['stopped']} ({stop['cut_by']}): {stop['queries_cut']} queries cut, "
                    f"{stop['queries_not_sent']} not sent",
                    purpose,
                    [],
                    context.graph_uris,
                )
            )
        context.report.record_outcome(QueryOutcome(state="complete", gaps=failures))
        context.report.report.config["void_source"] = self.record
        return patterns

    @staticmethod
    def _edge(graph_uris: list[str] | None, property_uri: str) -> str:
        """Return the triples of a property in the graphs of the source.

        The types of their subjects and objects are matched anywhere (_typed): void-generator
        types the ends of a link in the union of the graphs (ISDB's compounds are typed in
        PubChem's graph), and a query limited with FROM would see no named graph.
        """
        from rdfsolve.mining.query_builders import _graph_clause

        opening, closing = _graph_clause(graph_uris)
        return f"{opening} ?s <{property_uri}> ?o . {closing}"

    @staticmethod
    def _typed(variable: str, cls: str) -> str:
        """Return a filter: the term has the class in the default graph or in a named graph."""
        return f"FILTER EXISTS {{ {{ {variable} a <{cls}> }} UNION {{ GRAPH ?_t {{ {variable} a <{cls}> }} }} }}"

    @staticmethod
    def _member(variable: str, cls: str) -> str:
        """Return the members of a class, typed in the default graph or in a named graph.

        Samples start from the class (the type index): starting from all the triples of a
        property and filtering their subjects does not end when the class is rare (Bgee). A
        member typed in two places is matched twice; samples are DISTINCT or LIMITed.
        """
        return f"{{ {{ {variable} a <{cls}> }} UNION {{ GRAPH ?_m {{ {variable} a <{cls}> }} }} }}"

    @staticmethod
    def _untyped(variable: str) -> str:
        """Return a filter: the term has no class in the default graph or in a named graph."""
        return f"FILTER NOT EXISTS {{ {{ {variable} a ?_c }} UNION {{ GRAPH ?_t {{ {variable} a ?_c }} }} }}"

    def _ask(
        self,
        helper: SparqlHelper,
        query: str,
        purpose: str,
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
        classes: list[str],
    ) -> list[dict[str, Any]] | None:
        """Send one light query under the time limit; record a refusal as a gap.

        A query of a purpose that the endpoint cuts at a fixed limit, or that ran past the time
        limit of the step five times in a row, is not sent (self.cuts): it is recorded as a gap.
        """
        from rdfsolve.sparql_helper import (
            EndpointError,
            EndpointRateLimitError,
            EndpointTimeoutError,
        )

        if self.cuts.skip(purpose):
            failures.append(
                QueryFailure(
                    "timeout",
                    f"not sent: {self.cuts.stopped(purpose)}",
                    purpose,
                    classes,
                    graph_uris,
                )
            )
            return None
        try:
            with helper.budget(self.query_seconds):
                rows: list[dict[str, Any]] = helper.select(query, purpose=purpose)["results"][
                    "bindings"
                ]
        except EndpointTimeoutError as error:
            self.cuts.failed(purpose, error)
            failures.append(QueryFailure("timeout", str(error)[:300], purpose, classes, graph_uris))
        except EndpointRateLimitError as error:
            failures.append(
                QueryFailure("rate_limited", str(error)[:300], purpose, classes, graph_uris)
            )
        except (EndpointError, KeyError) as error:
            self.cuts.failed(purpose, error)
            failures.append(
                QueryFailure("endpoint", str(error)[:300], purpose, classes, graph_uris)
            )
        else:
            self.cuts.answered(purpose)
            return rows
        return None

    def _mine_gaps(
        self,
        helper: SparqlHelper,
        gaps: list[VoidGap],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> list[SchemaPattern]:
        """Find one witness of each gap; a found gap becomes a pattern with the VoID's count."""
        found: list[SchemaPattern] = []
        known = {(p.subject_class, p.property_uri, p.object_class) for p in self._void_patterns}
        for gap in gaps:
            edge = self._edge(graph_uris, gap.property_uri)
            if gap.kind == "objects":
                found += self._type_objects(helper, gap, known, failures, graph_uris)
                continue
            if gap.kind == "subjects":
                query = f"SELECT ?s ?o WHERE {{ {edge} {self._untyped('?s')} }} LIMIT 1"
                classes = []
            else:
                test = "isBlank(?o)" if gap.kind == "blank" else "isIRI(?o)"
                query = (
                    f"SELECT ?s ?o WHERE {{ {self._member('?s', gap.subject_class or '')} {edge} "
                    f"FILTER({test}) "
                    + (self._untyped("?o") if gap.kind == "objects" else "")
                    + " } LIMIT 1"
                )
                classes = [gap.subject_class or ""]
            rows = self._ask(helper, query, f"void/gap/{gap.kind}", failures, graph_uris, classes)
            if not rows:
                continue
            obj = rows[0]["o"]
            if gap.kind == "subjects":
                # A subject without a class has no place in a class pattern: recorded with the
                # VoID's lower bound of its triples and the example found.
                self.untyped_subjects.append(
                    {
                        "property": gap.property_uri,
                        "triples_at_least": gap.triples,
                        "example": {"subject": rows[0]["s"], "value": obj},
                    }
                )
                continue
            object_class = (
                "BlankNode"
                if obj["type"] == "bnode"
                else "Literal"
                if obj["type"] in ("literal", "typed-literal")
                else "Resource"
            )
            pattern = SchemaPattern(
                subject_class=gap.subject_class or "",
                property_uri=gap.property_uri,
                object_class=object_class,
                count=gap.triples,
                count_semantics="upper_bound",
                evidence_source="mined",
            )
            if object_class == "BlankNode":
                pattern.blank_node_predicates = self._blank_predicates(
                    helper, gap, failures, graph_uris
                )
            found.append(pattern)
            self.examples.append(
                PatternExample(
                    subject_class=pattern.subject_class,
                    property_uri=pattern.property_uri,
                    subject=_term(rows[0]["s"]),
                    value=_term(obj),
                )
            )
        return found

    def _type_objects(
        self,
        helper: SparqlHelper,
        gap: VoidGap,
        known: set[tuple[str, str, str]],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> list[SchemaPattern]:
        """Read the classes of a sample of the IRI objects that the VoID gives no class for.

        A VoID with few linksets (Bgee's of 2023: 22) leaves most links without an object class;
        searching the whole property for an object without a class does not end in time (Bgee:
        814 M triples). A sample of the objects says which classes they have and how many have
        none; each class not in the VoID becomes a pattern, without a count (the sample is not
        one), and the sample is recorded.
        """
        edge = self._edge(graph_uris, gap.property_uri)
        member = self._member("?s", gap.subject_class or "")
        query = (
            f"SELECT ?c (COUNT(DISTINCT ?o) AS ?n) WHERE {{ {{ SELECT DISTINCT ?o WHERE {{ {member} "
            f"{edge} FILTER(isIRI(?o)) }} LIMIT {self.object_sample} }} OPTIONAL {{ "
            f"{{ ?o a ?c }} UNION {{ GRAPH ?_t {{ ?o a ?c }} }} }} }} GROUP BY ?c"
        )
        rows = self._ask(
            helper, query, "void/gap/objects", failures, graph_uris, [gap.subject_class or ""]
        )
        if rows is None:
            return []
        sample = {r["c"]["value"] if "c" in r else "Resource": int(r["n"]["value"]) for r in rows}
        self.object_samples.append(
            {
                "subject_class": gap.subject_class,
                "property": gap.property_uri,
                "triples_without_object_class_in_void": gap.triples,
                "sample": self.object_sample,
                "objects_by_class": sample,
            }
        )
        return [
            SchemaPattern(
                subject_class=gap.subject_class or "",
                property_uri=gap.property_uri,
                object_class=cls,
                evidence_source="mined",
            )
            for cls in sorted(sample)
            if (gap.subject_class, gap.property_uri, cls) not in known
        ]

    def _blank_predicates(
        self,
        helper: SparqlHelper,
        gap: VoidGap,
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> list[str] | None:
        """Return the predicates of a sample of the blank-node objects of a gap."""
        edge = self._edge(graph_uris, gap.property_uri)
        member = self._member("?s", gap.subject_class or "")
        query = (
            f"SELECT DISTINCT ?p WHERE {{ {{ SELECT DISTINCT ?o WHERE {{ {member} {edge} FILTER(isBlank(?o)) }} "
            f"LIMIT 1000 }} ?o ?p ?v }} LIMIT 200"
        )
        rows = self._ask(
            helper, query, "void/blank-predicates", failures, graph_uris, [gap.subject_class or ""]
        )
        return None if rows is None else sorted(r["p"]["value"] for r in rows)

    def _languages(
        self,
        helper: SparqlHelper,
        patterns: list[SchemaPattern],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> None:
        """Record the language tags of a sample of each language-tagged literal pattern."""
        tags: dict[str, dict[str, int]] = {}
        for p in patterns:
            if p.datatype != RDF_LANG_STRING:
                continue
            query = (
                f"SELECT ?l (COUNT(*) AS ?n) WHERE {{ {{ SELECT DISTINCT ?s ?o WHERE {{ {self._match(p, graph_uris, sample=True)} }} "
                f"LIMIT {self.language_sample} }} BIND(LANG(?o) AS ?l) }} GROUP BY ?l"
            )
            rows = self._ask(
                helper, query, "void/languages", failures, graph_uris, [p.subject_class]
            )
            if rows is not None:
                tags[f"{p.subject_class} {p.property_uri}"] = {
                    r["l"]["value"]: int(r["n"]["value"]) for r in rows if "l" in r
                }
        self.record["languages"] = {"sample": self.language_sample, "tags": tags}

    def _class_samples(
        self,
        helper: SparqlHelper,
        patterns: list[SchemaPattern],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> None:
        """Keep a few subjects of every class: their IRIs give the namespaces of the class."""
        classes = sorted(
            {p.subject_class for p in patterns}
            | {
                p.object_class
                for p in patterns
                if p.object_class not in ("Literal", "Resource", "BlankNode")
            }
        )
        for cls in classes:
            from rdfsolve.mining.query_builders import _graph_clause

            opening, closing = _graph_clause(graph_uris)
            # Members of the class (from the type index), with a statement in the graphs of the
            # source when it has graphs; starting from all statements does not end (Bgee).
            in_graphs = (
                f"FILTER EXISTS {{ {opening} ?s ?_p ?_o . {closing} }}" if graph_uris else ""
            )
            query = (
                f"SELECT DISTINCT ?s WHERE {{ {{ {{ ?s a <{cls}> }} UNION {{ GRAPH ?_t {{ ?s a <{cls}> }} }} }} "
                f"{in_graphs} }} LIMIT {self.class_samples}"
            )
            rows = self._ask(helper, query, "void/class-samples", failures, graph_uris, [cls])
            if rows:
                self.class_examples[cls] = [_term(r["s"]) for r in rows]

    def _match(
        self, p: SchemaPattern, graph_uris: list[str] | None, *, sample: bool = False
    ) -> str:
        """Return the graph pattern of a pattern's triples.

        For a count, each triple once; for a sample, starting from the members of the subject
        class.
        """
        edge = self._edge(graph_uris, p.property_uri)
        head = (
            f"{self._member('?s', p.subject_class)} {edge}"
            if sample
            else f"{edge} {self._typed('?s', p.subject_class)}"
        )
        if p.object_class == "Literal":
            return head + (f" FILTER(DATATYPE(?o) = <{p.datatype}>)" if p.datatype else "")
        if p.object_class in ("Resource", "BlankNode"):
            return head
        return f"{head} {self._typed('?o', p.object_class)}"

    def _examples(
        self,
        helper: SparqlHelper,
        patterns: list[SchemaPattern],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> None:
        """Find one example of each of the largest patterns of the VoID."""
        done = {(e.subject_class, e.property_uri) for e in self.examples}
        largest = sorted(
            (p for p in patterns if p.evidence_source == "void"),
            key=lambda p: -(p.count or 0),
        )[: self.example_patterns]
        for p in largest:
            if (p.subject_class, p.property_uri) in done:
                continue
            query = f"SELECT ?s ?o WHERE {{ {self._match(p, graph_uris, sample=True)} }} LIMIT 1"
            rows = self._ask(
                helper, query, "void/examples", failures, graph_uris, [p.subject_class]
            )
            if rows:
                self.examples.append(
                    PatternExample(
                        subject_class=p.subject_class,
                        property_uri=p.property_uri,
                        subject=_term(rows[0]["s"]),
                        value=_term(rows[0]["o"]),
                    )
                )

    def _drift(
        self,
        helper: SparqlHelper,
        patterns: list[SchemaPattern],
        failures: list[QueryFailure],
        graph_uris: list[str] | None,
    ) -> dict[str, Any]:
        """Re-count the largest and some random VoID patterns on the endpoint."""
        counted = [p for p in patterns if p.evidence_source == "void" and p.count]
        largest = sorted(counted, key=lambda p: -(p.count or 0))[: self.drift_largest]
        rest = [p for p in counted if p not in largest]
        chosen = largest + self.random.sample(rest, min(self.drift_random, len(rest)))
        rows_out: list[dict[str, Any]] = []
        for p in chosen:
            query = f"SELECT (COUNT(*) AS ?n) WHERE {{ {self._match(p, graph_uris)} }}"
            rows = self._ask(helper, query, "void/drift", failures, graph_uris, [p.subject_class])
            now = int(rows[0]["n"]["value"]) if rows else None
            rows_out.append(
                {
                    "subject_class": p.subject_class,
                    "property": p.property_uri,
                    "object": p.object_class,
                    "datatype": p.datatype,
                    "void_count": p.count,
                    "count_now": now,
                    "ratio": round(now / p.count, 4) if now is not None and p.count else None,
                    "chosen": "largest" if p in largest else "random",
                }
            )
        measured = [r for r in rows_out if r["ratio"] is not None]
        off = [r for r in measured if abs(r["ratio"] - 1) > 0.1]
        return {
            "checked": len(rows_out),
            "measured": len(measured),
            "off_by_more_than_10_percent": len(off),
            "patterns": rows_out,
        }


@dataclass
class PublishedVoid:
    """The VoID description that an endpoint publishes in one of its graphs."""

    graph: str
    void: Graph
    issued: str | None
    # How the graph was read: "construct" (whole), "construct_void_terms" (only the triples of
    # VoID and service-description terms) or "select_pages" (paged SELECT, rebuilt locally).
    read_by: str = "construct"


def find_published_void(
    helper: SparqlHelper, *, seconds: float = 120.0, max_bytes: int = 1024**3
) -> PublishedVoid | None:
    """Return the full VoID description that the endpoint publishes, or None.

    Full: class partitions that have property partitions (void-generator). A graph with only a
    description of the dataset and a few linksets to other datasets (WikiPathways states 9 in its
    data graph) is not one. The graph is fetched whole: it is metadata (IDSM: 473,000 triples,
    80 MB), so a larger response is allowed than for data.
    """
    from rdfsolve.sparql_helper import EndpointError

    query = (
        "PREFIX void: <http://rdfs.org/ns/void#> SELECT ?g (COUNT(DISTINCT ?cp) AS ?n) WHERE { "
        "GRAPH ?g { ?cp void:class ?c ; void:propertyPartition ?pp } } GROUP BY ?g "
        "ORDER BY DESC(?n) LIMIT 1"
    )
    try:
        with helper.budget(seconds):
            rows = helper.select(query, purpose="void/find")["results"]["bindings"]
    except (EndpointError, KeyError) as error:
        logger.info("No published VoID found: %s", str(error)[:200])
        return None
    if not rows or int(rows[0]["n"]["value"]) == 0:
        return None
    graph = rows[0]["g"]["value"]
    saved = helper.max_response_bytes
    helper.max_response_bytes = max(saved, max_bytes)
    try:
        void, read_by = _read_void_graph(helper, graph)
    finally:
        helper.max_response_bytes = saved
    if void is None:
        return None
    from rdflib.namespace import DCTERMS

    issued = max((str(o) for o in void.objects(None, DCTERMS.issued)), default=None)
    logger.info(
        "Published VoID in %s: %d triples, issued %s (read by %s)",
        graph,
        len(void),
        issued,
        read_by,
    )
    return PublishedVoid(graph, void, issued, read_by)


def _read_void_graph(helper: SparqlHelper, graph: str) -> tuple[Graph | None, str]:
    """Read a VoID graph: whole, else only its VoID terms, else in pages of SELECT.

    Virtuoso refuses a CONSTRUCT of a large graph ("D1CTX: Hash dictionary is full, exceeded
    1000000/2000000 entries": SIBiLS, RIKEN BRC, rehearsal 2026-10-06). The triples of the
    VoID, void-ext and service-description terms (and rdf:type, dcterms:issued) are then asked
    for alone; when that is refused too, the graph is read in ordered pages of SELECT and
    rebuilt here, unless it has blank nodes (their names do not hold across pages).
    """
    from rdflib import Literal

    from rdfsolve.sparql_helper import EndpointError

    whole = f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}"
    terms = (
        f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o "
        'FILTER(STRSTARTS(STR(?p), "http://rdfs.org/ns/void#") '
        '|| STRSTARTS(STR(?p), "http://ldf.fi/void-ext#") '
        '|| STRSTARTS(STR(?p), "http://www.w3.org/ns/sparql-service-description#") '
        "|| ?p = <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
        "|| ?p = <http://purl.org/dc/terms/issued>) } }"
    )
    for query, read_by in ((whole, "construct"), (terms, "construct_void_terms")):
        try:
            return Graph().parse(data=helper.construct(query), format="turtle"), read_by
        except EndpointError as error:
            logger.warning(
                "The VoID in %s was not read by %s: %s", graph, read_by, str(error)[:200]
            )
    void = Graph()
    template = helper.prepare_paginated_query(
        f"SELECT ?s ?p ?o WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }} ORDER BY ?s ?p ?o"
    )

    def term(binding: dict[str, Any]) -> Any:
        """Return the RDF term of a SPARQL JSON binding; a blank node cannot be paged."""
        if binding["type"] == "bnode":
            raise ValueError("blank node")
        if binding["type"] == "uri":
            return URIRef(binding["value"])
        return Literal(
            binding["value"],
            lang=binding.get("xml:lang"),
            datatype=URIRef(binding["datatype"]) if binding.get("datatype") else None,
        )

    try:
        for page in helper.select_chunked(template, chunk_size=10_000, purpose="void/read"):
            for row in page:
                void.add((term(row["s"]), term(row["p"]), term(row["o"])))
    except (EndpointError, ValueError, KeyError) as error:
        logger.warning("The VoID in %s could not be fetched: %s", graph, str(error)[:200])
        return None, "not_read"
    return void, "select_pages"


def void_for_source(published: PublishedVoid, graph_uris: list[str] | None) -> Graph | None:
    """Return the part of a published VoID that describes the graphs of a source, or None.

    A source with graphs takes the datasets that the service description gives for them; a
    source without graphs takes the default dataset, or the only dataset described.
    """
    from rdfsolve.schema_models.readers.void import (
        SD,
        scope_void_graph,
        void_datasets_of_graphs,
    )

    void = published.void
    if graph_uris:
        datasets = void_datasets_of_graphs(void, graph_uris)
        if len(datasets) < len(set(graph_uris)):
            return None
    else:
        # The default graph when it is described, else every named graph of the default
        # dataset (SIB: the default graph is the union of the named graphs and is not described),
        # without the VoID graph itself and the endpoint's service graphs (/.well-known/).
        defaults = list(void.objects(None, SD.defaultDataset))
        datasets = sorted(
            g
            for d in defaults
            for g in void.objects(d, SD.defaultGraph)
            if isinstance(g, URIRef) and any(void.objects(g, VOID.classPartition))
        )
        if not datasets:
            named = [
                n
                for d in defaults
                for n in void.objects(d, SD.namedGraph)
                if str(void.value(n, SD.name) or n) != published.graph
                and "/.well-known/" not in str(void.value(n, SD.name) or n)
            ]
            datasets = sorted(
                g for n in named for g in void.objects(n, SD.graph) if isinstance(g, URIRef)
            )
        if not datasets:
            return None
    scoped = scope_void_graph(void, datasets)
    return scoped if any(scoped.objects(None, VOID.classPartition)) else None
