"""Query source definitions and bounded examples in the mining graph scope."""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from typing import Any

from rdfsolve._outcomes import QueryFailure
from rdfsolve.mining.query_builders import (
    _graph_clause,
    _graph_scope,
    _subject_type_pattern,
    _type_pattern,
)
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.mining.report_tracking import ReportCollector
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import (
    DEFINITION_PREDICATES,
    NAME_PREDICATES,
    PatternExample,
    RdfTerm,
    SchemaEnrichment,
    TermAnnotation,
)
from rdfsolve.schema_models.paths import absolute_iri
from rdfsolve.schema_models.pattern import SchemaPattern
from rdfsolve.sparql_helper import SparqlHelper


def _iri(value: str) -> str:
    """Write an IRI of the data in a query; one that is not an RDF IRI is written when sent."""
    try:
        return f"<{absolute_iri(value)}>"
    except ValueError as error:
        raise ValueError(f"Invalid IRI in enrichment query: {value!r}") from error


def definition_query(iris: list[str], graph_uris: list[str] | None) -> str:
    """Read definition predicates without a Cartesian product of text fields."""
    for iri in graph_uris or []:
        _iri(iri)
    opening, closing = _graph_clause(graph_uris)
    return f"""SELECT DISTINCT ?term ?predicate ?text WHERE {{
      VALUES ?term {{ {" ".join(map(_iri, iris))} }}
      VALUES ?predicate {{ {" ".join(map(_iri, DEFINITION_PREDICATES + NAME_PREDICATES))} }}
      {opening} ?term ?predicate ?text . FILTER(isLiteral(?text)) {closing}
    }}"""


# Subject-value pairs of a pattern sampled before a value filter (see example_query).
EXAMPLE_SAMPLE = 1000


def _filtered(pattern: SchemaPattern) -> bool:
    """Return whether the value condition of *pattern*'s example query is a filter."""
    return pattern.object_binding != "term" and pattern.object_class in (
        "Literal",
        "Resource",
        "BlankNode",
    )


def example_query(
    pattern: SchemaPattern,
    graph_uris: list[str] | None,
    limit: int,
    type_context_graph_uris: list[str] | None = None,
    *,
    with_dataset: bool = True,
    sample: int | None = None,
) -> str:
    """Select values of this pattern, not an unrelated value of the property.

    With *sample*, a value condition that is a filter (a literal's datatype, an untyped
    resource, a blank node) is applied to the first *sample* subject-value pairs of the
    property, not to all of them: QLever evaluates the filter over the whole join before the
    limit (ChEMBL chembl#Activity chemblId, 24 M values: over 300 s, against 0.1 s on a sample
    of 1,000; article/experiments/example-queries-20261002). A sample with no such value gives
    no row; the caller asks again without one.
    """
    if not 1 <= limit <= 20:
        raise ValueError("Example limit must be between 1 and 20")
    for iri in graph_uris or []:
        _iri(iri)
    dataset, opening, closing = _graph_scope(graph_uris, type_context_graph_uris)
    if not with_dataset:
        dataset = ""
    subject = (
        f"VALUES ?subject {{ {_iri(pattern.subject_class)} }}"
        if pattern.subject_binding == "term"
        else _type_pattern("?subject", _iri(pattern.subject_class), type_context_graph_uris)
    )
    if pattern.object_binding == "term":
        condition = f"VALUES ?value {{ {_iri(pattern.object_class)} }}"
    elif pattern.object_class == "Literal":
        condition = "FILTER(isLiteral(?value))"
        if pattern.datatype:
            condition += f" FILTER(datatype(?value) = {_iri(pattern.datatype)})"
    elif pattern.object_class == "Resource":
        condition = f"FILTER(isIRI(?value)) FILTER NOT EXISTS {{ {_type_pattern('?value', '?anyType', type_context_graph_uris)} }}"
    elif pattern.object_class == "BlankNode":
        condition = "FILTER(isBlank(?value))"
    else:
        condition = _type_pattern("?value", _iri(pattern.object_class), type_context_graph_uris)
    if sample and _filtered(pattern):
        return f"""SELECT DISTINCT ?subject ?value {dataset} WHERE {{
      {{ SELECT ?subject ?value WHERE {{
        {subject}
        {opening} ?subject {_iri(pattern.property_uri)} ?value . {closing}
      }} LIMIT {sample} }}
      {condition}
    }} LIMIT {limit}"""
    return f"""SELECT DISTINCT ?subject ?value {dataset} WHERE {{
      {subject}
      {opening} ?subject {_iri(pattern.property_uri)} ?value . {closing}
      {condition}
    }} LIMIT {limit}"""


def class_example_query(
    iri: str,
    graph_uris: list[str] | None,
    limit: int,
    type_context_graph_uris: list[str] | None = None,
    *,
    with_dataset: bool = True,
) -> str:
    """Sample typed subjects in the selected data scope."""
    for graph in graph_uris or []:
        _iri(graph)
    pattern = _subject_type_pattern("?subject", _iri(iri), type_context_graph_uris)
    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)
    if not with_dataset:
        dataset = ""
    return f"SELECT DISTINCT ?subject {dataset} WHERE {{ {pattern} }} LIMIT {limit}"


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
    type_context_graph_uris: list[str] | None = None,
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

    def run(query: str, purpose: str, *, tried: bool = False) -> list[dict[str, Any]] | None:
        """Run a query and record its outcome.

        A *tried* query (a batch, which is asked again one query at a time) that does not
        complete is not a failure: it returns None.
        """
        nonlocal successful
        if result.query_count and delay:
            time.sleep(delay)
        started = time.monotonic()
        outcome = select_outcome(query, purpose, helper, graph_uris=graph_uris)
        result.query_count += 1
        complete = outcome.state == "complete"
        if report:
            report.record_query(purpose, time.monotonic() - started, complete)
        if tried and not complete:
            return None
        result.failures.extend(outcome.failures)
        successful += int(complete)
        if report:
            report.record_outcome(outcome)
        return outcome.rows

    def select(query: str, purpose: str) -> list[dict[str, Any]]:
        """Run a query and record its outcome; return its rows."""
        return run(query, purpose) or []

    def invalid(error: Exception, purpose: str) -> None:
        """Record malformed response data as a failure."""
        failure = QueryFailure("invalid_response", str(error), purpose, [], graph_uris)
        result.failures.append(failure)
        if report:
            from rdfsolve._outcomes import QueryOutcome

            report.record_outcome(QueryOutcome([], "failed", [failure]))

    dataset, _, _ = _graph_scope(graph_uris, type_context_graph_uris)

    def batched(queries: list[str], purpose: str) -> Iterator[tuple[int, dict[str, Any]]]:
        """Group example queries and retain each result slot.

        A batch that does not complete is asked again one query at a time, so that only the
        query that is too costly fails (ChEMBL: one string-valued pattern of chembl#Activity
        failed 13 batches of 10, 2026-09-30).
        """
        for offset in range(0, len(queries), 10):
            group = list(enumerate(queries[offset : offset + 10], offset))
            branches = [f"{{ {{ {query} }} BIND({index} AS ?slot) }}" for index, query in group]
            union = "SELECT * " + dataset + " WHERE { " + " UNION ".join(branches) + " }"
            rows = run(union, purpose, tried=len(group) > 1)
            if rows is None:
                rows = [
                    {**row, "slot": {"type": "literal", "value": str(index)}}
                    for index, query in group
                    for row in select(f"SELECT * {dataset} WHERE {{ {query} }}", purpose)
                ]
            for row in rows:
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
                    result.labels if definition.predicate in NAME_PREDICATES else result.definitions
                )
                if definition not in destination:
                    destination.append(definition)
            except (KeyError, TypeError, ValueError) as error:
                invalid(error, "definitions")
    if examples_per_pattern:
        result.class_examples = {iri: [] for iri in classes}
        queries = [
            class_example_query(
                iri, graph_uris, examples_per_pattern, type_context_graph_uris, with_dataset=False
            )
            for iri in classes
        ]
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

        def add_example(pattern: SchemaPattern, row: dict[str, Any]) -> bool:
            """Add an example of *pattern* from a response row; return whether it was valid."""
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
                return False
            return True

        def examples_of(selected: list[SchemaPattern], sample: int | None) -> set[int]:
            """Ask for examples of *selected*; return the indexes of those that got one."""
            queries = [
                example_query(
                    pattern,
                    graph_uris,
                    examples_per_pattern,
                    type_context_graph_uris,
                    with_dataset=False,
                    sample=sample,
                )
                for pattern in selected
            ]
            return {
                i
                for i, row in batched(queries, "pattern_examples")
                if add_example(selected[i], row)
            }

        found = examples_of(patterns, EXAMPLE_SAMPLE)
        # A sample with no value of the pattern (a rare datatype of the property): the full query.
        sampled = [p for i, p in enumerate(patterns) if i not in found and _filtered(p)]
        if sampled:
            examples_of(sampled, None)
    result.state = ("partial" if successful else "failed") if result.failures else "complete"
    if report and phase:
        report.finish_phase(phase, error="Incomplete enrichment" if result.failures else None)
    return result
