"""Mine Wikibase endpoints (Wikidata and other instances) from declarations and windows.

Wikibase publishes its properties in RDF: each wikibase:Property states its value type,
its direct predicate (wdt:) and its labels. Items carry no rdf:type; they are classified
by a membership property (P31, "instance of", on Wikidata). This strategy reads every
declared property, then observes one bounded window of each property's direct statements,
classifying subjects and objects by the membership property. A window smaller than its
limit observed the property completely; a full window is a sample and the run says so.
"""

from __future__ import annotations

import logging
from typing import Any

from rdflib import Literal

from rdfsolve._outcomes import QueryFailure, QueryOutcome
from rdfsolve.mining.query_fallbacks import select_outcome
from rdfsolve.mining.strategy import MiningContext, MiningStrategy
from rdfsolve.models import SchemaPattern
from rdfsolve.schema_models.enrichment import PatternExample, RdfTerm, TermAnnotation

logger = logging.getLogger(__name__)

WIKIBASE = "http://wikiba.se/ontology#"
LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class WikibaseStrategy(MiningStrategy):
    """Declared Wikibase properties plus one bounded statement window per property."""

    def __init__(self, membership: str, *, window: int = 1000, language: str = "en") -> None:
        """Classify items by *membership* (e.g. wdt:P31) and observe *window* statements each."""
        self.membership_property = membership
        self.window = window
        self.language = language
        self.examples: list[PatternExample] = []
        self.labels: list[TermAnnotation] = []

    @property
    def name(self) -> str:
        """Return the strategy name for reporting."""
        return "wikibase"

    def mine(self, context: MiningContext) -> list[SchemaPattern]:
        """Read declared properties, then observe each property's statements in a window."""
        declared = self._declared(context)
        summary: dict[str, Any] = {
            "membership_property": self.membership_property,
            "window": self.window,
            "declared_properties": len(declared),
            "unclassified_subject_statements": 0,
            "properties": {},
        }
        context.report.report.config["wikibase"] = summary
        patterns: list[SchemaPattern] = []
        sampled: list[str] = []
        not_attempted: list[str] = []
        for direct in sorted(declared):
            if direct == self.membership_property:
                continue
            if not_attempted:
                not_attempted.append(direct)
                summary["properties"][direct] = {"statements": None, "state": "not_attempted"}
                continue
            if (direct,) in context.resumed:
                rows = context.resumed[(direct,)]
                patterns.extend(SchemaPattern.model_validate(row) for row in rows)
                context.report.checkpoint("patterns", [direct], rows)
                summary["properties"][direct] = {"statements": None, "state": "resumed"}
                continue
            outcome = select_outcome(
                self._window_query(context, direct), "wikibase/window", context.helper, [direct]
            )
            if outcome.state != "complete":
                context.report.record_outcome(outcome)
                limited = any(f.category == "rate_limited" for f in outcome.failures)
                state = "rate_limited" if limited else "failed"
                summary["properties"][direct] = {"statements": None, "state": state}
                if limited:
                    # The endpoint asked for a longer pause than the client waits. Every next
                    # request is refused too, so the run stops; the checkpoint allows a resume.
                    not_attempted.append(direct)
                continue
            found, statements, unclassified = self._rows(direct, outcome.rows)
            summary["unclassified_subject_statements"] += unclassified
            state = "sampled" if statements >= self.window else "complete"
            summary["properties"][direct] = {"statements": statements, "state": state}
            if state == "sampled":
                sampled.append(direct)
            patterns.extend(found)
            context.report.checkpoint(
                "patterns", [direct], [p.model_dump(mode="json") for p in found]
            )
        if len(not_attempted) > 1:
            context.report.record_outcome(
                QueryOutcome(
                    state="partial",
                    failures=[
                        QueryFailure(
                            "rate_limited",
                            f"The endpoint asked for a pause longer than the wait budget; "
                            f"{len(not_attempted) - 1} properties were not attempted. "
                            "Resume from the checkpoint.",
                            "wikibase/window",
                            not_attempted[1:],
                        )
                    ],
                )
            )
        if sampled:
            context.report.record_outcome(
                QueryOutcome(
                    state="partial",
                    failures=[
                        QueryFailure(
                            "sampled",
                            f"{len(sampled)} properties filled their window of {self.window} "
                            "statements; other statements may add rows.",
                            "wikibase/window",
                            sampled,
                        )
                    ],
                )
            )
        return patterns

    def _declared(self, context: MiningContext) -> dict[str, str]:
        """Direct predicate of every declared property, with its label as field name."""
        language = Literal(self.language).n3()
        query = f"""SELECT ?direct ?label WHERE {{
  ?property a <{WIKIBASE}Property> ; <{WIKIBASE}directClaim> ?direct .
  OPTIONAL {{ ?property <{LABEL}> ?label FILTER(LANG(?label) = {language}) }}
}}"""
        outcome = select_outcome(query, "wikibase/properties", context.helper, [])
        if outcome.state != "complete":
            context.report.record_outcome(outcome)
            raise RuntimeError("Could not read the declared Wikibase properties")
        declared: dict[str, str] = {}
        for row in outcome.rows:
            direct = row["direct"]["value"]
            label = row.get("label", {}).get("value", "")
            declared[direct] = label
            if label:
                self.labels.append(
                    TermAnnotation(
                        term_iri=direct,
                        predicate=LABEL,
                        text=RdfTerm(kind="literal", value=label, language=self.language),
                    )
                )
        return declared

    def _window_query(self, context: MiningContext, direct: str) -> str:
        """Group one window of statements by subject class, object class and value kind."""
        member = f"<{self.membership_property}>"
        window = f"SELECT ?s ?o WHERE {{ ?s <{direct}> ?o }} LIMIT {self.window}"
        body = (
            f"OPTIONAL {{ ?s {member} ?sc }} OPTIONAL {{ ?o {member} ?oc }} "
            'BIND(IF(isLiteral(?o), "literal", IF(isBlank(?o), "bnode", "uri")) AS ?kind) '
            "BIND(DATATYPE(?o) AS ?dt)"
        )
        project = (
            "?sc ?oc ?kind ?dt (COUNT(*) AS ?n) (SAMPLE(?s) AS ?subject) (SAMPLE(?o) AS ?value)"
        )
        group = "GROUP BY ?sc ?oc ?kind ?dt"
        if context.helper.sparql_engine == "blazegraph":
            # Blazegraph evaluates a named subquery first; a plain subquery may be joined late.
            return f"SELECT {project} WITH {{ {window} }} AS %window WHERE {{ INCLUDE %window . {body} }} {group}"
        return f"SELECT {project} WHERE {{ {{ {window} }} {body} }} {group}"

    def _rows(
        self, direct: str, rows: list[dict[str, Any]]
    ) -> tuple[list[SchemaPattern], int, int]:
        """Turn grouped window rows into patterns and examples; count unclassified subjects."""
        patterns: dict[tuple[str, str, str | None], SchemaPattern] = {}
        statements = unclassified = 0
        for row in rows:
            count = int(row["n"]["value"])
            statements += count
            if "sc" not in row:
                unclassified += count
                continue
            subject_class = row["sc"]["value"]
            kind = row["kind"]["value"]
            datatype = row.get("dt", {}).get("value") if kind == "literal" else None
            object_class = (
                "Literal"
                if kind == "literal"
                else "BlankNode"
                if kind == "bnode"
                else row.get("oc", {}).get("value", "Resource")
            )
            key = (subject_class, object_class, datatype)
            if key not in patterns:
                patterns[key] = SchemaPattern(
                    subject_class=subject_class,
                    property_uri=direct,
                    object_class=object_class,
                    datatype=datatype,
                )
                self.examples.append(
                    PatternExample(
                        subject_class=subject_class,
                        property_uri=direct,
                        subject=RdfTerm.model_validate(_term(row["subject"])),
                        value=RdfTerm.model_validate(_term(row["value"])),
                    )
                )
        return list(patterns.values()), statements, unclassified


def _term(binding: dict[str, Any]) -> dict[str, Any]:
    """RDF term fields from a SPARQL JSON binding."""
    kind = "literal" if binding["type"] in ("literal", "typed-literal") else binding["type"]
    return {
        "kind": kind,
        "value": binding["value"],
        "datatype": binding.get("datatype"),
        "language": binding.get("xml:lang"),
    }
