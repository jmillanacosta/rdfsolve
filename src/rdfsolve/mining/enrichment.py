"""Query source definitions and bounded examples in the mining graph scope."""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from typing import Any

from rdfsolve._outcomes import QueryFailure
from rdfsolve.mining.query_builders import _graph_clause
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.schema_models.core import MinedSchema, SchemaPattern
from rdfsolve.schema_models.enrichment import (
    DEFINITION_PREDICATES,
    LABEL_PREDICATES,
    PatternExample,
    RdfTerm,
    SchemaEnrichment,
    TermAnnotation,
)
from rdfsolve.sparql_helper import SparqlHelper


def _iri(value: str) -> str:
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value) or re.search(r'[<>"{}|^`\\\s]', value):
        raise ValueError(f"Invalid IRI in enrichment query: {value!r}")
    return f"<{value}>"


def definition_query(iris: list[str], graph_uris: list[str] | None) -> str:
    """Read definition predicates without a Cartesian product of text fields."""
    for iri in graph_uris or []:
        _iri(iri)
    opening, closing = _graph_clause(graph_uris)
    return f"""SELECT DISTINCT ?term ?predicate ?text WHERE {{
      VALUES ?term {{ {" ".join(map(_iri, iris))} }}
      VALUES ?predicate {{ {" ".join(map(_iri, DEFINITION_PREDICATES + LABEL_PREDICATES))} }}
      {opening} ?term ?predicate ?text . FILTER(isLiteral(?text)) {closing}
    }}"""


def example_query(pattern: SchemaPattern, graph_uris: list[str] | None, limit: int) -> str:
    """Select values of this pattern, not an unrelated value of the property."""
    if not 1 <= limit <= 20:
        raise ValueError("Example limit must be between 1 and 20")
    for iri in graph_uris or []:
        _iri(iri)
    opening, closing = _graph_clause(graph_uris)
    if pattern.object_class == "Literal":
        condition = "FILTER(isLiteral(?value))"
        if pattern.datatype:
            condition += f" FILTER(datatype(?value) = {_iri(pattern.datatype)})"
    elif pattern.object_class == "Resource":
        condition = "FILTER(isIRI(?value)) FILTER NOT EXISTS { ?value a ?anyType }"
    elif pattern.object_class == "BlankNode":
        condition = "FILTER(isBlank(?value))"
    else:
        condition = f"?value a {_iri(pattern.object_class)} ."
    return f"""SELECT DISTINCT ?subject ?value WHERE {{
      {opening}
      ?subject a {_iri(pattern.subject_class)} ; {_iri(pattern.property_uri)} ?value .
      {condition}
      {closing}
    }} LIMIT {limit}"""


def class_example_query(iri: str, graph_uris: list[str] | None, limit: int) -> str:
    """Sample instances, including classes that occur only as object types."""
    for graph in graph_uris or []:
        _iri(graph)
    opening, closing = _graph_clause(graph_uris)
    return f"SELECT DISTINCT ?subject WHERE {{ {opening} ?subject a {_iri(iri)} . {closing} }} LIMIT {limit}"


def _term(binding: dict[str, Any], scope: int) -> RdfTerm:
    kind = binding["type"]
    if kind == "typed-literal":
        kind = "literal"
    value = binding["value"]
    # Blank node labels are local to one query response.
    if kind == "bnode":
        value = f"example_{scope}_{value}"
    return RdfTerm(
        kind=kind, value=value, datatype=binding.get("datatype"), language=binding.get("xml:lang")
    )


def query_enrichment(
    schema: MinedSchema,
    helper: SparqlHelper,
    graph_uris: list[str] | None = None,
    *,
    examples_per_pattern: int = 2,
    delay: float = 0.0,
    report: ReportCollector | None = None,
    annotation_iris: list[str] | None = None,
) -> SchemaEnrichment:
    """Attempt definitions and examples for every class and pattern.

    Samples are endpoint-order examples, not random or representative samples.
    A complete state means all planned queries completed, not all terms have text.
    """
    if not 0 <= examples_per_pattern <= 20:
        raise ValueError("examples_per_pattern must be between 0 and 20")
    result = SchemaEnrichment(
        examples_per_pattern=examples_per_pattern,
        graph_uris=graph_uris,
        endpoint=helper.endpoint_url,
    )
    phase = report.start_phase("enrichment") if report else None
    successful = 0

    def select(query: str, purpose: str) -> list[dict[str, Any]]:
        nonlocal successful
        if result.query_count and delay:
            time.sleep(delay)
        started = time.monotonic()
        outcome = select_outcome(query, purpose, helper, graph_uris=graph_uris)
        result.query_count += 1
        result.failures.extend(outcome.failures)
        complete = outcome.state == "complete"
        successful += int(complete)
        if report:
            report.record_query(purpose, time.monotonic() - started, complete)
            report.record_outcome(outcome)
        return outcome.rows

    def invalid(error: Exception, purpose: str) -> None:
        failure = QueryFailure("invalid_response", str(error), purpose, [], graph_uris)
        result.failures.append(failure)
        if report:
            from rdfsolve._outcomes import QueryOutcome

            report.record_outcome(QueryOutcome([], "failed", [failure]))

    def batched(queries: list[str], purpose: str) -> Iterator[tuple[int, dict[str, Any]]]:
        for offset in range(0, len(queries), 10):
            branches = [
                f"{{ {{ {query} }} BIND({index} AS ?slot) }}"
                for index, query in enumerate(queries[offset : offset + 10], offset)
            ]
            for row in select("SELECT * WHERE { " + " UNION ".join(branches) + " }", purpose):
                try:
                    index = int(row["slot"]["value"])
                    if not offset <= index < min(offset + 10, len(queries)):
                        raise ValueError("Example response has an invalid query slot")
                    yield index, row
                except (KeyError, TypeError, ValueError) as error:
                    invalid(error, purpose)

    classes = sorted(schema.get_classes())
    iris = sorted(set(classes) | set(schema.get_properties()) | set(annotation_iris or []))
    for offset in range(0, len(iris), 50):
        for row in select(definition_query(iris[offset : offset + 50], graph_uris), "definitions"):
            try:
                definition = TermAnnotation(
                    term_iri=row["term"]["value"],
                    predicate=row["predicate"]["value"],
                    text=_term(row["text"], result.query_count),
                )
                destination = (
                    result.labels
                    if definition.predicate in LABEL_PREDICATES
                    else result.definitions
                )
                if definition not in destination:
                    destination.append(definition)
            except (KeyError, TypeError, ValueError) as error:
                invalid(error, "definitions")
    if examples_per_pattern:
        result.class_examples = {iri: [] for iri in classes}
        queries = [class_example_query(iri, graph_uris, examples_per_pattern) for iri in classes]
        for index, row in batched(queries, "class_examples"):
            try:
                result.class_examples[classes[index]].append(
                    _term(row["subject"], result.query_count)
                )
            except (KeyError, TypeError, ValueError) as error:
                invalid(error, "class_examples")
        unique = {}
        for pattern in schema.patterns:
            key = (
                pattern.subject_class,
                pattern.property_uri,
                pattern.object_class,
                pattern.datatype,
            )
            unique[key] = pattern
        patterns = list(unique.values())
        queries = [example_query(pattern, graph_uris, examples_per_pattern) for pattern in patterns]
        for index, row in batched(queries, "pattern_examples"):
            pattern = patterns[index]
            try:
                result.examples.append(
                    PatternExample(
                        subject_class=pattern.subject_class,
                        property_uri=pattern.property_uri,
                        subject=_term(row["subject"], result.query_count),
                        value=_term(row["value"], result.query_count),
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                invalid(error, "pattern_examples")
    result.state = ("partial" if successful else "failed") if result.failures else "complete"
    if report and phase:
        report.finish_phase(phase, error="Incomplete enrichment" if result.failures else None)
    return result
